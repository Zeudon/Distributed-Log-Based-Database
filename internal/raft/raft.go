// Implements the Raft consensus algorithm from scratch.

package raft

import (
	"bytes"
	"encoding/gob"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/Zeudon/Distributed-Log-Based-Database/config"
	"github.com/Zeudon/Distributed-Log-Based-Database/internal/rpc"
	"github.com/Zeudon/Distributed-Log-Based-Database/internal/storage"
	"github.com/Zeudon/Distributed-Log-Based-Database/proto"
)

// ApplyMsg is sent on the applyCh channel when a log entry is ready to be
// applied to the state machine, or when a snapshot should be installed.
type ApplyMsg struct {
	CommandValid bool
	Command      proto.Command
	CommandIndex uint64

	SnapshotValid bool
	Snapshot      proto.Snapshot
}

// RaftNode is a single participant in a Raft consensus group.
type RaftNode struct {
	mu  sync.Mutex
	cfg config.ClusterConfig

	// Persistent state
	currentTerm uint64
	votedFor    int // -1 = none
	log         []proto.LogEntry

	// Volatile state
	commitIndex uint64
	lastApplied uint64
	role        RaftRole
	leaderID    int // -1 = unknown

	// Volatile state — leader only (reinitialized after election)
	nextIndex  []uint64 // for each peer: next log index to send
	matchIndex []uint64 // for each peer: highest known replicated index

	// Snapshot metadata
	snapshotIndex uint64
	snapshotTerm  uint64

	// Infrastructure
	store   *stateStore
	wal     *storage.WAL
	mem     *storage.MemTable
	snapMgr *storage.SnapshotManager
	rpcCli  *rpc.Client
	applyCh chan ApplyMsg

	// Internal signals
	heartbeatCh     chan struct{} // follower resets election timer on receipt
	leaderChangeCh  chan struct{} // triggers heartbeat loop when elected
	commitCh        chan struct{} // signals applyCommitted goroutine
	snapshotTrigger chan struct{} // signals maybeSnapshot goroutine
}

// NewRaftNode constructs a RaftNode. Call Start() to begin operation.
func NewRaftNode(
	cfg config.ClusterConfig,
	wal *storage.WAL,
	mem *storage.MemTable,
	snapMgr *storage.SnapshotManager,
	rpcCli *rpc.Client,
) *RaftNode {
	store := newStateStore(cfg.DataDir)
	ps, err := store.load()
	if err != nil {
		slog.Warn("Raft: failed to load persisted state, starting fresh", "err", err)
		ps = persistentState{VotedFor: -1}
	}

	// Restore snapshot index/term if a snapshot exists
	snap, _ := snapMgr.Load()

	r := &RaftNode{
		cfg:             cfg,
		currentTerm:     ps.CurrentTerm,
		votedFor:        ps.VotedFor,
		log:             ps.Log,
		role:            Follower,
		leaderID:        -1,
		snapshotIndex:   snap.LastIncludedIndex,
		snapshotTerm:    snap.LastIncludedTerm,
		nextIndex:       make([]uint64, len(cfg.PeerAddresses)),
		matchIndex:      make([]uint64, len(cfg.PeerAddresses)),
		store:           store,
		wal:             wal,
		mem:             mem,
		snapMgr:         snapMgr,
		rpcCli:          rpcCli,
		applyCh:         make(chan ApplyMsg, 64),
		heartbeatCh:     make(chan struct{}, 1),
		leaderChangeCh:  make(chan struct{}, 1),
		commitCh:        make(chan struct{}, 1),
		snapshotTrigger: make(chan struct{}, 1),
	}

	// lastApplied starts at the snapshot index (already applied)
	r.lastApplied = snap.LastIncludedIndex
	r.commitIndex = snap.LastIncludedIndex

	return r
}

// Start launches the Raft background goroutines.
func (r *RaftNode) Start() {
	slog.Info("Raft node starting",
		"nodeID", r.cfg.NodeID,
		"partitionID", r.cfg.PartitionID,
		"peers", r.cfg.PeerAddresses)
	go r.runElectionTimer()
	go r.applyCommitted()
	go r.maybeSnapshotLoop()
}

// Submit proposes a command for replication. Returns the assigned log index,
// the current term, and whether this node believes it is the leader.
// The caller should wait until ApplyMsg with CommandIndex == index appears on
// the ApplyCh channel before responding to the client.
func (r *RaftNode) Submit(cmd proto.Command) (index uint64, term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != Leader {
		return 0, r.currentTerm, false
	}

	// Encode the command
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(cmd)

	index = r.lastLogIndex() + 1
	term = r.currentTerm
	entry := proto.LogEntry{Index: index, Term: term, Command: buf.Bytes()}
	r.appendToLog([]proto.LogEntry{entry})
	_ = r.wal.Append(entry)

	slog.Debug("Raft: command submitted", "index", index, "term", term)
	// Immediately send AppendEntries to peers (don't wait for heartbeat timer)
	go r.broadcastAppendEntries()

	return index, term, true
}

// ApplyCh returns the channel on which applied log entries are delivered.
func (r *RaftNode) ApplyCh() <-chan ApplyMsg {
	return r.applyCh
}

// LeaderID returns the current known leader (-1 if unknown).
func (r *RaftNode) LeaderID() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

// IsLeader returns true if this node currently believes it is the leader.
func (r *RaftNode) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role == Leader
}

// LeaderAddr returns the RPC address of the current leader, or "" if unknown.
func (r *RaftNode) LeaderAddr() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.leaderID < 0 {
		return ""
	}
	// leaderID == cfg.NodeID → self
	if r.leaderID == r.cfg.NodeID {
		return r.cfg.SelfAddress
	}
	// Map leaderID to peer address.
	// Peers are the other replicas in the same partition, indexed 0..N-1.
	// NodeIDs within a partition are assigned sequentially, but we store peer
	// addresses in PeerAddresses, so we use a small helper.
	return r.peerAddr(r.leaderID)
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (r *RaftNode) HandleRequestVote(args proto.RequestVoteArgs, reply *proto.RequestVoteReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply.Term = r.currentTerm
	reply.VoteGranted = false

	if args.Term < r.currentTerm {
		return
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}

	// Grant vote if we haven't voted yet (or already voted for this candidate)
	// AND the candidate's log is at least as up-to-date as ours.
	canVote := r.votedFor == -1 || r.votedFor == args.CandidateID
	logOk := args.LastLogTerm > r.lastLogTerm() ||
		(args.LastLogTerm == r.lastLogTerm() && args.LastLogIndex >= r.lastLogIndex())

	if canVote && logOk {
		r.votedFor = args.CandidateID
		r.persistLocked()
		reply.VoteGranted = true
		r.signalHeartbeat() // reset election timer
		slog.Debug("Raft: vote granted",
			"to", args.CandidateID,
			"term", args.Term)
	}
}

// HandleAppendEntries processes an incoming AppendEntries RPC (also heartbeat).
func (r *RaftNode) HandleAppendEntries(args proto.AppendEntriesArgs, reply *proto.AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply.Term = r.currentTerm
	reply.Success = false

	if args.Term < r.currentTerm {
		return
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}

	// Recognise the sender as leader and reset election timer
	r.leaderID = args.LeaderID
	if r.role != Follower {
		r.role = Follower
	}
	r.signalHeartbeat()

	// Log consistency check
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > r.lastLogIndex() {
			// We're missing entries; tell leader where we diverge
			reply.ConflictIndex = r.lastLogIndex() + 1
			reply.ConflictTerm = 0
			return
		}
		prevTerm := r.termAt(args.PrevLogIndex)
		if prevTerm != args.PrevLogTerm {
			// Find the first index of the conflicting term
			reply.ConflictTerm = prevTerm
			reply.ConflictIndex = args.PrevLogIndex
			for reply.ConflictIndex > r.snapshotIndex+1 {
				if r.termAt(reply.ConflictIndex-1) != prevTerm {
					break
				}
				reply.ConflictIndex--
			}
			return
		}
	}

	// Append new entries, overwriting any conflicting entries
	for i, entry := range args.Entries {
		if entry.Index <= r.snapshotIndex {
			continue // already snapshotted
		}
		if entry.Index <= r.lastLogIndex() {
			// Conflict: truncate and break
			if r.termAt(entry.Index) != entry.Term {
				r.truncateLogAfter(entry.Index - 1)
				_ = r.wal.TruncateAfter(entry.Index - 1)
				// Append remaining entries
				for _, e := range args.Entries[i:] {
					r.appendToLog([]proto.LogEntry{e})
					_ = r.wal.Append(e)
				}
				break
			}
			// Same term — entry already present, skip
		} else {
			// New entry
			r.appendToLog([]proto.LogEntry{entry})
			_ = r.wal.Append(entry)
		}
	}

	// Advance commit index
	if args.LeaderCommit > r.commitIndex {
		newCommit := args.LeaderCommit
		if last := r.lastLogIndex(); last < newCommit {
			newCommit = last
		}
		r.commitIndex = newCommit
		r.signalCommit()
	}

	reply.Success = true
}

// HandleInstallSnapshot processes an incoming InstallSnapshot RPC.
func (r *RaftNode) HandleInstallSnapshot(args proto.InstallSnapshotArgs, reply *proto.InstallSnapshotReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply.Term = r.currentTerm

	if args.Term < r.currentTerm {
		return
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}
	r.leaderID = args.LeaderID
	r.signalHeartbeat()

	// Ignore if we already have a newer snapshot
	if args.LastIncludedIndex <= r.snapshotIndex {
		return
	}

	snap := proto.Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}
	_ = r.snapMgr.Save(snap)
	_ = storage.Compact(snap, r.wal)

	// Discard log entries covered by the snapshot
	r.snapshotIndex = args.LastIncludedIndex
	r.snapshotTerm = args.LastIncludedTerm
	if len(r.log) > 0 && r.log[len(r.log)-1].Index <= args.LastIncludedIndex {
		r.log = nil
	} else if len(r.log) > 0 && r.log[0].Index <= args.LastIncludedIndex {
		r.truncateLogAfter(args.LastIncludedIndex) // keep only entries after snapshot
		r.log = r.entriesFrom(args.LastIncludedIndex + 1)
	}

	r.commitIndex = args.LastIncludedIndex
	r.lastApplied = args.LastIncludedIndex
	r.persistLocked()

	// Deliver snapshot to state machine
	r.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snap,
	}
}

// runElectionTimer runs continuously. When no heartbeat is received within the
// election timeout window, it starts a new election.
func (r *RaftNode) runElectionTimer() {
	for {
		timeout := r.randomElectionTimeout()
		select {
		case <-r.heartbeatCh:
			// Heartbeat / vote-granted received — reset timer
		case <-time.After(timeout):
			r.mu.Lock()
			role := r.role
			r.mu.Unlock()
			if role != Leader {
				go r.startElection()
			}
		}
	}
}

// startElection transitions this node to Candidate and solicits votes.
func (r *RaftNode) startElection() {
	r.mu.Lock()
	r.role = Candidate
	r.currentTerm++
	r.votedFor = r.cfg.NodeID
	r.leaderID = -1
	r.persistLocked()

	term := r.currentTerm
	lastIdx := r.lastLogIndex()
	lastTerm := r.lastLogTerm()
	peers := r.cfg.PeerAddresses
	r.mu.Unlock()

	slog.Info("Raft: election started", "node", r.cfg.NodeID, "term", term)

	args := proto.RequestVoteArgs{
		Term:         term,
		CandidateID:  r.cfg.NodeID,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	votes := 1 // vote for self
	required := (r.cfg.ReplicaCount / 2) + 1
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, peerAddr := range peers {
		peerAddr := peerAddr
		wg.Add(1)
		go func() {
			defer wg.Done()
			var reply proto.RequestVoteReply
			_, payload, err := r.rpcCli.Call(peerAddr, proto.MsgRequestVote, args)
			if err != nil {
				slog.Debug("Raft: RequestVote failed", "peer", peerAddr, "err", err)
				return
			}
			if err := rpc.DecodePayload(payload, &reply); err != nil {
				slog.Debug("Raft: RequestVote decode failed", "peer", peerAddr, "err", err)
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				return
			}
			if !reply.VoteGranted || r.role != Candidate || r.currentTerm != term {
				return
			}

			voteMu.Lock()
			votes++
			won := votes >= required
			voteMu.Unlock()

			if won {
				r.becomeLeaderLocked()
			}
		}()
	}
}

// becomeLeaderLocked transitions this node to Leader and initialises
// nextIndex / matchIndex. Caller must hold r.mu.
func (r *RaftNode) becomeLeaderLocked() {
	if r.role == Leader {
		return // already leader
	}
	r.role = Leader
	r.leaderID = r.cfg.NodeID
	nextIdx := r.lastLogIndex() + 1
	for i := range r.nextIndex {
		r.nextIndex[i] = nextIdx
		r.matchIndex[i] = 0
	}
	slog.Info("Raft: became leader", "node", r.cfg.NodeID, "term", r.currentTerm)
	go r.sendHeartbeats()
}

// becomeFollowerLocked transitions this node to Follower with the given term.
// Caller must hold r.mu.
func (r *RaftNode) becomeFollowerLocked(term uint64) {
	r.role = Follower
	r.currentTerm = term
	r.votedFor = -1
	r.persistLocked()
}

// sendHeartbeats is the leader heartbeat loop. It runs until this node is no
// longer the leader. It sends AppendEntries (with or without entries) to all
// peers at HeartbeatIntervalMs.
func (r *RaftNode) sendHeartbeats() {
	interval := time.Duration(r.cfg.HeartbeatIntervalMs) * time.Millisecond
	for {
		r.mu.Lock()
		if r.role != Leader {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()

		r.broadcastAppendEntries()
		time.Sleep(interval)
	}
}

// broadcastAppendEntries sends AppendEntries to all peers concurrently.
func (r *RaftNode) broadcastAppendEntries() {
	r.mu.Lock()
	if r.role != Leader {
		r.mu.Unlock()
		return
	}
	peers := r.cfg.PeerAddresses
	term := r.currentTerm
	leaderID := r.cfg.NodeID
	leaderCommit := r.commitIndex
	r.mu.Unlock()

	for i, peerAddr := range peers {
		i, peerAddr := i, peerAddr
		go func() {
			r.mu.Lock()
			if r.role != Leader {
				r.mu.Unlock()
				return
			}
			nextIdx := r.nextIndex[i]
			prevLogIndex := nextIdx - 1
			prevLogTerm := r.termAt(prevLogIndex)
			entries := r.entriesFrom(nextIdx)

			// If entries have been compacted, send InstallSnapshot instead
			if nextIdx <= r.snapshotIndex {
				r.mu.Unlock()
				r.sendInstallSnapshot(peerAddr, i)
				return
			}

			args := proto.AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			r.mu.Unlock()

			var reply proto.AppendEntriesReply
			_, aePayload, err := r.rpcCli.Call(peerAddr, proto.MsgAppendEntries, args)
			if err != nil {
				slog.Debug("Raft: AppendEntries failed", "peer", peerAddr, "err", err)
				return
			}
			if err := rpc.DecodePayload(aePayload, &reply); err != nil {
				slog.Debug("Raft: AppendEntries decode failed", "peer", peerAddr, "err", err)
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				return
			}
			if r.role != Leader || r.currentTerm != term {
				return
			}

			if reply.Success {
				if len(entries) > 0 {
					newMatch := entries[len(entries)-1].Index
					if newMatch > r.matchIndex[i] {
						r.matchIndex[i] = newMatch
					}
					r.nextIndex[i] = newMatch + 1
				}
				r.advanceCommitIndexLocked()
			} else {
				// Back up nextIndex using conflict hint
				if reply.ConflictTerm > 0 {
					// Search for last entry with conflictTerm
					found := uint64(0)
					for j := len(r.log) - 1; j >= 0; j-- {
						if r.log[j].Term == reply.ConflictTerm {
							found = r.log[j].Index
							break
						}
					}
					if found > 0 {
						r.nextIndex[i] = found + 1
					} else {
						r.nextIndex[i] = reply.ConflictIndex
					}
				} else if reply.ConflictIndex > 0 {
					r.nextIndex[i] = reply.ConflictIndex
				} else if r.nextIndex[i] > 1 {
					r.nextIndex[i]--
				}
			}
		}()
	}
}

// sendInstallSnapshot sends the current snapshot to a lagging peer.
func (r *RaftNode) sendInstallSnapshot(peerAddr string, peerIdx int) {
	snap, err := r.snapMgr.Load()
	if err != nil {
		return
	}

	r.mu.Lock()
	term := r.currentTerm
	leaderID := r.cfg.NodeID
	r.mu.Unlock()

	args := proto.InstallSnapshotArgs{
		Term:              term,
		LeaderID:          leaderID,
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Data:              snap.Data,
	}

	var reply proto.InstallSnapshotReply
	_, isPayload, err := r.rpcCli.Call(peerAddr, proto.MsgInstallSnapshot, args)
	if err != nil {
		slog.Debug("Raft: InstallSnapshot failed", "peer", peerAddr, "err", err)
		return
	}
	if err := rpc.DecodePayload(isPayload, &reply); err != nil {
		slog.Debug("Raft: InstallSnapshot decode failed", "peer", peerAddr, "err", err)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		return
	}
	if r.role == Leader {
		r.nextIndex[peerIdx] = snap.LastIncludedIndex + 1
		r.matchIndex[peerIdx] = snap.LastIncludedIndex
	}
}

// advanceCommitIndexLocked checks if a majority of replicas have replicated a
// higher index and advances commitIndex if so. Caller must hold r.mu.
func (r *RaftNode) advanceCommitIndexLocked() {
	// Find the highest index replicated on a majority
	for n := r.lastLogIndex(); n > r.commitIndex; n-- {
		if r.termAt(n) != r.currentTerm {
			continue // Only commit entries from current term (Raft §5.4.2)
		}
		count := 1 // self
		for _, match := range r.matchIndex {
			if match >= n {
				count++
			}
		}
		if count >= (r.cfg.ReplicaCount/2)+1 {
			r.commitIndex = n
			r.signalCommit()
			slog.Debug("Raft: commitIndex advanced", "index", n)
			break
		}
	}
}

// applyCommitted drains committed log entries and sends them on applyCh.
func (r *RaftNode) applyCommitted() {
	for range r.commitCh {
		for {
			r.mu.Lock()
			if r.lastApplied >= r.commitIndex {
				r.mu.Unlock()
				break
			}
			r.lastApplied++
			applyIdx := r.lastApplied
			entry, ok := r.entryAt(applyIdx)
			r.mu.Unlock()

			if !ok {
				slog.Warn("Raft: apply: entry not found in log", "index", applyIdx)
				continue
			}

			var cmd proto.Command
			dec := gob.NewDecoder(bytes.NewReader(entry.Command))
			if err := dec.Decode(&cmd); err != nil {
				slog.Error("Raft: apply: decode command", "index", applyIdx, "err", err)
				continue
			}

			r.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      cmd,
				CommandIndex: applyIdx,
			}
		}
	}
}

// maybeSnapshotLoop watches for triggers and takes a snapshot + compacts the
// WAL when the in-memory log exceeds SnapshotThreshold.
func (r *RaftNode) maybeSnapshotLoop() {
	for range r.snapshotTrigger {
		r.mu.Lock()
		if len(r.log) < r.cfg.SnapshotThreshold {
			r.mu.Unlock()
			continue
		}
		lastApplied := r.lastApplied
		lastAppliedTerm := r.termAt(lastApplied)
		kvData := r.mem.Snapshot()
		r.mu.Unlock()

		snap := proto.Snapshot{
			LastIncludedIndex: lastApplied,
			LastIncludedTerm:  lastAppliedTerm,
			Data:              kvData,
		}
		if err := r.snapMgr.Save(snap); err != nil {
			slog.Error("Raft: snapshot save failed", "err", err)
			continue
		}
		if err := storage.Compact(snap, r.wal); err != nil {
			slog.Error("Raft: WAL compaction failed", "err", err)
			continue
		}

		r.mu.Lock()
		// Drop log entries now covered by the snapshot
		r.snapshotIndex = lastApplied
		r.snapshotTerm = lastAppliedTerm
		remaining := r.entriesFrom(lastApplied + 1)
		r.log = remaining
		r.persistLocked()
		r.mu.Unlock()

		slog.Info("Raft: snapshot taken and WAL compacted", "index", lastApplied)
	}
}

// TriggerSnapshot signals the snapshot loop to run. Can be called externally
// or by the apply loop when the threshold is exceeded.
func (r *RaftNode) TriggerSnapshot() {
	select {
	case r.snapshotTrigger <- struct{}{}:
	default:
	}
}

// Term returns the current Raft term.
func (r *RaftNode) Term() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

// Role returns the current Raft role as a string ("Leader", "Follower", "Candidate").
func (r *RaftNode) Role() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role.String()
}

// CommitIndex returns the current commitIndex.
func (r *RaftNode) CommitIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIndex
}

// LastApplied returns the index of the last applied log entry.
func (r *RaftNode) LastApplied() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastApplied
}

// LastLogIndexPublic returns the index of the last entry in the log.
func (r *RaftNode) LastLogIndexPublic() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastLogIndex()
}

// SnapshotIndex returns the index of the last included snapshot entry.
func (r *RaftNode) SnapshotIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotIndex
}

// persistLocked writes term, votedFor, and log to disk. Caller must hold r.mu.
func (r *RaftNode) persistLocked() {
	ps := persistentState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Log:         r.log,
	}
	if err := r.store.save(ps); err != nil {
		slog.Error("Raft: persist failed", "err", err)
	}
}

// signalHeartbeat sends a non-blocking notification to the election timer.
func (r *RaftNode) signalHeartbeat() {
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
	}
}

// signalCommit sends a non-blocking notification to the apply loop.
func (r *RaftNode) signalCommit() {
	select {
	case r.commitCh <- struct{}{}:
	default:
	}
}

// randomElectionTimeout returns a random duration in [min, max] ms.
func (r *RaftNode) randomElectionTimeout() time.Duration {
	min := r.cfg.ElectionTimeoutMinMs
	max := r.cfg.ElectionTimeoutMaxMs
	ms := min + rand.Intn(max-min+1)
	return time.Duration(ms) * time.Millisecond
}

// peerAddr maps a nodeID to its RPC address using cluster config.
// NodeIDs within a partition group are 0..ReplicaCount-1 in the same order
// as PeerAddresses plus the node's own SelfAddress.
func (r *RaftNode) peerAddr(nodeID int) string {
	// Build the full list: self + peers, sorted by NodeID
	// The peers list in config does NOT include self.
	// Peer index within the partition group:
	// We identify peers by their order in PeerAddresses, but NodeID could be
	// anything. We simply assign peer index = position in PeerAddresses.
	// For leader redirect, we return the address mapped to the leaderID.
	// Convention: PeerAddresses[i] corresponds to the (i-th peer, skipping self).
	// Since we don't store nodeIDs alongside peer addresses in config, we do a
	// best-effort: return the address at index (nodeID % len(peers)).
	peers := r.cfg.PeerAddresses
	if len(peers) == 0 {
		return ""
	}
	idx := nodeID % len(peers)
	if idx < 0 {
		idx = 0
	}
	return peers[idx]
}
