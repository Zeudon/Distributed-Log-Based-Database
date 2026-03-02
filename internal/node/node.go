// Package node wires together all system components — storage, Raft, and RPC —
// into a single cohesive database node.
package node

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Zeudon/Distributed-Log-Based-Database/config"
	"github.com/Zeudon/Distributed-Log-Based-Database/internal/httpapi"
	"github.com/Zeudon/Distributed-Log-Based-Database/internal/raft"
	"github.com/Zeudon/Distributed-Log-Based-Database/internal/rpc"
	"github.com/Zeudon/Distributed-Log-Based-Database/internal/storage"
	"github.com/Zeudon/Distributed-Log-Based-Database/proto"
)

// pendingOp tracks an in-flight client write waiting for Raft to commit it.
type pendingOp struct {
	term uint64
	ch   chan error
}

// Node is the top-level object that binds together one Raft replica's storage,
// Raft state machine, and RPC server.
type Node struct {
	cfg config.ClusterConfig

	wal     *storage.WAL
	mem     *storage.MemTable
	snapMgr *storage.SnapshotManager

	raftNode *raft.RaftNode
	rpcSrv   *rpc.Server
	rpcCli   *rpc.Client

	// pendingOps maps a committed log index → channel that the waiting handler
	// goroutine blocks on. When the apply loop processes that index it sends nil
	// (or an error) to the channel.
	pendingMu  sync.Mutex
	pendingOps map[uint64]*pendingOp
	// doneEntries records indices that were applied before their pendingOp was
	// registered, allowing waitForCommit to return immediately in that case.
	doneEntries map[uint64]struct{}

	// lastCommitTime is updated every time a log entry is applied to the MemTable.
	lastCommitMu   sync.RWMutex
	lastCommitTime time.Time
}

// NewNode constructs a Node from configuration. Call Start() to begin.
func NewNode(cfg config.ClusterConfig) (*Node, error) {
	slog.Info("Initialising node",
		"nodeID", cfg.NodeID,
		"partitionID", cfg.PartitionID,
		"selfAddr", cfg.SelfAddress)

	// Open storage
	wal, err := storage.OpenWAL(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("node: open WAL: %w", err)
	}
	mem := storage.NewMemTable()
	snapMgr := storage.NewSnapshotManager(cfg.DataDir)

	// Restore from snapshot
	snap, err := snapMgr.Load()
	if err != nil {
		return nil, fmt.Errorf("node: load snapshot: %w", err)
	}
	if snap.LastIncludedIndex > 0 {
		mem.Restore(snap.Data)
		slog.Info("MemTable restored from snapshot",
			"index", snap.LastIncludedIndex,
			"keys", mem.Len())
	}

	// Replay WAL entries after the snapshot
	entries, err := wal.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("node: read WAL: %w", err)
	}
	replayed := 0
	for _, entry := range entries {
		if entry.Index <= snap.LastIncludedIndex {
			continue
		}
		if err := applyEntry(mem, entry); err != nil {
			slog.Warn("WAL replay: skipped bad entry",
				"index", entry.Index,
				"err", err)
		}
		replayed++
	}
	slog.Info("WAL replayed", "entries", replayed)

	// Build Raft node and RPC layers
	rpcCli := rpc.NewClient()
	rpcSrv := rpc.NewServer(cfg.SelfAddress)

	raftNode := raft.NewRaftNode(cfg, wal, mem, snapMgr, rpcCli)

	n := &Node{
		cfg:         cfg,
		wal:         wal,
		mem:         mem,
		snapMgr:     snapMgr,
		raftNode:    raftNode,
		rpcSrv:      rpcSrv,
		rpcCli:      rpcCli,
		pendingOps:  make(map[uint64]*pendingOp),
		doneEntries: make(map[uint64]struct{}),
	}

	n.registerHandlers()
	return n, nil
}

// Start begins serving RPC and runs the Raft consensus engine.
// It blocks until the RPC server is stopped.
func (n *Node) Start() error {
	n.raftNode.Start()
	go n.runApplyLoop()

	// Start HTTP REST API server alongside the RPC server.
	httpSrv := httpapi.NewServer(n, n.cfg.HTTPPort)
	go func() {
		slog.Info("HTTP API server starting", "port", n.cfg.HTTPPort)
		if err := httpSrv.Listen(); err != nil {
			slog.Error("HTTP server stopped", "err", err)
		}
	}()

	slog.Info("Node started", "addr", n.cfg.SelfAddress)
	return n.rpcSrv.Listen()
}

func (n *Node) registerHandlers() {
	// Client → node
	n.rpcSrv.RegisterHandler(proto.MsgGet, n.handleGet)
	n.rpcSrv.RegisterHandler(proto.MsgSet, n.handleSet)
	n.rpcSrv.RegisterHandler(proto.MsgDelete, n.handleDelete)

	// Node → node (Raft)
	n.rpcSrv.RegisterHandler(proto.MsgRequestVote, n.handleRequestVote)
	n.rpcSrv.RegisterHandler(proto.MsgAppendEntries, n.handleAppendEntries)
	n.rpcSrv.RegisterHandler(proto.MsgInstallSnapshot, n.handleInstallSnapshot)
}

func (n *Node) handleGet(_ uint8, payload []byte) (uint8, any, error) {
	var req proto.GetRequest
	if err := rpc.DecodePayload(payload, &req); err != nil {
		return proto.MsgError, errResp("decode error", proto.ErrInternal, ""), nil
	}
	value, found := n.mem.Get(req.Key)
	return proto.MsgGetResponse, proto.GetResponse{Value: value, Found: found}, nil
}

func (n *Node) handleSet(_ uint8, payload []byte) (uint8, any, error) {
	var req proto.SetRequest
	if err := rpc.DecodePayload(payload, &req); err != nil {
		return proto.MsgError, errResp("decode error", proto.ErrInternal, ""), nil
	}

	if !n.raftNode.IsLeader() {
		return proto.MsgError, errResp("not leader", proto.ErrNotLeader, n.raftNode.LeaderAddr()), nil
	}

	cmd := proto.Command{Type: proto.CmdSet, Key: req.Key, Value: req.Value}
	idx, term, isLeader := n.raftNode.Submit(cmd)
	if !isLeader {
		return proto.MsgError, errResp("not leader", proto.ErrNotLeader, n.raftNode.LeaderAddr()), nil
	}

	if err := n.waitForCommit(idx, term); err != nil {
		return proto.MsgError, errResp(err.Error(), proto.ErrInternal, ""), nil
	}
	return proto.MsgSetResponse, proto.SetResponse{Success: true}, nil
}

func (n *Node) handleDelete(_ uint8, payload []byte) (uint8, any, error) {
	var req proto.DeleteRequest
	if err := rpc.DecodePayload(payload, &req); err != nil {
		return proto.MsgError, errResp("decode error", proto.ErrInternal, ""), nil
	}

	if !n.raftNode.IsLeader() {
		return proto.MsgError, errResp("not leader", proto.ErrNotLeader, n.raftNode.LeaderAddr()), nil
	}

	cmd := proto.Command{Type: proto.CmdDelete, Key: req.Key}
	idx, term, isLeader := n.raftNode.Submit(cmd)
	if !isLeader {
		return proto.MsgError, errResp("not leader", proto.ErrNotLeader, n.raftNode.LeaderAddr()), nil
	}

	if err := n.waitForCommit(idx, term); err != nil {
		return proto.MsgError, errResp(err.Error(), proto.ErrInternal, ""), nil
	}
	return proto.MsgDeleteResponse, proto.DeleteResponse{Success: true}, nil
}

func (n *Node) handleRequestVote(_ uint8, payload []byte) (uint8, any, error) {
	var args proto.RequestVoteArgs
	if err := rpc.DecodePayload(payload, &args); err != nil {
		return proto.MsgError, errResp("decode", proto.ErrInternal, ""), nil
	}
	var reply proto.RequestVoteReply
	n.raftNode.HandleRequestVote(args, &reply)
	return proto.MsgRequestVoteReply, reply, nil
}

func (n *Node) handleAppendEntries(_ uint8, payload []byte) (uint8, any, error) {
	var args proto.AppendEntriesArgs
	if err := rpc.DecodePayload(payload, &args); err != nil {
		return proto.MsgError, errResp("decode", proto.ErrInternal, ""), nil
	}
	var reply proto.AppendEntriesReply
	n.raftNode.HandleAppendEntries(args, &reply)
	return proto.MsgAppendEntriesReply, reply, nil
}

func (n *Node) handleInstallSnapshot(_ uint8, payload []byte) (uint8, any, error) {
	var args proto.InstallSnapshotArgs
	if err := rpc.DecodePayload(payload, &args); err != nil {
		return proto.MsgError, errResp("decode", proto.ErrInternal, ""), nil
	}
	var reply proto.InstallSnapshotReply
	n.raftNode.HandleInstallSnapshot(args, &reply)
	return proto.MsgInstallSnapshotReply, reply, nil
}

// runApplyLoop drains ApplyMsgs from Raft and applies them to the MemTable.
func (n *Node) runApplyLoop() {
	for msg := range n.raftNode.ApplyCh() {
		if msg.SnapshotValid {
			n.mem.Restore(msg.Snapshot.Data)
			slog.Info("Node: snapshot installed", "index", msg.Snapshot.LastIncludedIndex)
			continue
		}
		if !msg.CommandValid {
			continue
		}

		cmd := msg.Command
		switch cmd.Type {
		case proto.CmdSet:
			n.mem.Set(cmd.Key, cmd.Value)
		case proto.CmdDelete:
			n.mem.Delete(cmd.Key)
		}

		// Record the time of the latest commit.
		n.lastCommitMu.Lock()
		n.lastCommitTime = time.Now()
		n.lastCommitMu.Unlock()

		// Check snapshot threshold
		if n.raftNode.IsLeader() {
			n.raftNode.TriggerSnapshot()
		}

		// Notify any waiting client handler
		n.pendingMu.Lock()
		if op, ok := n.pendingOps[msg.CommandIndex]; ok {
			delete(n.pendingOps, msg.CommandIndex)
			op.ch <- nil
		} else {
			// Applied before the handler registered its pending op — record it.
			n.doneEntries[msg.CommandIndex] = struct{}{}
		}
		n.pendingMu.Unlock()
	}
}

// waitForCommit blocks until the entry at idx is applied, or until a 5-second
// timeout. It handles the race where the entry is applied before the channel
// is registered by checking the doneEntries set.
func (n *Node) waitForCommit(idx uint64, term uint64) error {
	ch := make(chan error, 1)

	n.pendingMu.Lock()
	// Check if it was already applied before we got here.
	if _, done := n.doneEntries[idx]; done {
		delete(n.doneEntries, idx)
		n.pendingMu.Unlock()
		return nil
	}
	n.pendingOps[idx] = &pendingOp{term: term, ch: ch}
	n.pendingMu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		n.pendingMu.Lock()
		delete(n.pendingOps, idx)
		n.pendingMu.Unlock()
		return fmt.Errorf("commit timed out for log index %d", idx)
	}
}

// applyEntry applies a single WAL entry to the MemTable during startup replay.
func applyEntry(mem *storage.MemTable, entry proto.LogEntry) error {
	var cmd proto.Command
	dec := gob.NewDecoder(bytes.NewReader(entry.Command))
	if err := dec.Decode(&cmd); err != nil {
		return err
	}
	switch cmd.Type {
	case proto.CmdSet:
		mem.Set(cmd.Key, cmd.Value)
	case proto.CmdDelete:
		mem.Delete(cmd.Key)
	}
	return nil
}

// errResp is a helper to build an ErrorResponse.
func errResp(msg string, code proto.ErrorCode, leaderAddr string) proto.ErrorResponse {
	return proto.ErrorResponse{
		Message:    msg,
		ErrCode:    code,
		LeaderAddr: leaderAddr,
	}
}

// SubmitSet submits a CmdSet command to Raft and blocks until it is committed.
func (n *Node) SubmitSet(key, value string) error {
	if !n.raftNode.IsLeader() {
		return fmt.Errorf("not leader")
	}
	cmd := proto.Command{Type: proto.CmdSet, Key: key, Value: value}
	idx, term, isLeader := n.raftNode.Submit(cmd)
	if !isLeader {
		return fmt.Errorf("not leader")
	}
	return n.waitForCommit(idx, term)
}

// SubmitDelete submits a CmdDelete command to Raft and blocks until it is committed.
func (n *Node) SubmitDelete(key string) error {
	if !n.raftNode.IsLeader() {
		return fmt.Errorf("not leader")
	}
	cmd := proto.Command{Type: proto.CmdDelete, Key: key}
	idx, term, isLeader := n.raftNode.Submit(cmd)
	if !isLeader {
		return fmt.Errorf("not leader")
	}
	return n.waitForCommit(idx, term)
}

// MemGet reads a key directly from the in-memory state (stale-read semantics).
func (n *Node) MemGet(key string) (string, bool) { return n.mem.Get(key) }

// MemSnapshot returns a deep copy of all key-value pairs in the MemTable.
func (n *Node) MemSnapshot() map[string]string { return n.mem.Snapshot() }

// MemTableLen returns the number of keys in the MemTable.
func (n *Node) MemTableLen() int { return n.mem.Len() }

// NodeID returns this node's integer ID.
func (n *Node) NodeID() int { return n.cfg.NodeID }

// PartitionID returns the partition this node belongs to.
func (n *Node) PartitionID() int { return n.cfg.PartitionID }

// SelfAddr returns the RPC listen address of this node.
func (n *Node) SelfAddr() string { return n.cfg.SelfAddress }

// PeerAddresses returns the RPC addresses of other replicas in this partition.
func (n *Node) PeerAddresses() []string { return n.cfg.PeerAddresses }

// PeerNodeIDs derives the integer node IDs of peer replicas.
// Convention: nodeID = partitionID * replicaCount + replicaIndex.
func (n *Node) PeerNodeIDs() []int {
	base := n.cfg.PartitionID * n.cfg.ReplicaCount
	var ids []int
	for i := 0; i < n.cfg.ReplicaCount; i++ {
		id := base + i
		if id != n.cfg.NodeID {
			ids = append(ids, id)
		}
	}
	return ids
}

// RaftTerm returns the current Raft term.
func (n *Node) RaftTerm() uint64 { return n.raftNode.Term() }

// RaftRole returns the current Raft role string.
func (n *Node) RaftRole() string { return n.raftNode.Role() }

// RaftLeaderID returns the current known Raft leader ID.
func (n *Node) RaftLeaderID() int { return n.raftNode.LeaderID() }

// RaftLeaderAddr returns the RPC address of the current Raft leader.
func (n *Node) RaftLeaderAddr() string { return n.raftNode.LeaderAddr() }

// RaftCommitIndex returns the current Raft commit index.
func (n *Node) RaftCommitIndex() uint64 { return n.raftNode.CommitIndex() }

// RaftLastApplied returns the last applied Raft log index.
func (n *Node) RaftLastApplied() uint64 { return n.raftNode.LastApplied() }

// RaftLastLogIndex returns the index of the last Raft log entry.
func (n *Node) RaftLastLogIndex() uint64 { return n.raftNode.LastLogIndexPublic() }

// RaftSnapshotIndex returns the last included index in the latest snapshot.
func (n *Node) RaftSnapshotIndex() uint64 { return n.raftNode.SnapshotIndex() }

// RaftIsLeader returns true if this node is the current Raft leader.
func (n *Node) RaftIsLeader() bool { return n.raftNode.IsLeader() }

// LastCommitTime returns the wall-clock time of the most recent log entry applied.
func (n *Node) LastCommitTime() time.Time {
	n.lastCommitMu.RLock()
	defer n.lastCommitMu.RUnlock()
	return n.lastCommitTime
}

// LastWALAppendTime returns the wall-clock time of the most recent WAL append.
func (n *Node) LastWALAppendTime() time.Time { return n.wal.LastAppendTime() }

// LastSnapshotTime returns the wall-clock time of the most recent snapshot save.
func (n *Node) LastSnapshotTime() time.Time { return n.snapMgr.LastSnapshotTime() }

// SnapshotExists returns true if a snapshot file is present on disk.
func (n *Node) SnapshotExists() bool { return n.snapMgr.Exists() }
