// Package proto defines all wire types shared across the RPC and Raft layers.
// Serialisation is done with encoding/gob — no external code generation required.
package proto

// ─────────────────────────────────────────────────────────────────────────────
// RPC message type constants
// ─────────────────────────────────────────────────────────────────────────────

const (
	// Client → node messages
	MsgGet    uint8 = 0x01
	MsgSet    uint8 = 0x02
	MsgDelete uint8 = 0x03

	// Node → node Raft messages
	MsgRequestVote      uint8 = 0x10
	MsgRequestVoteReply uint8 = 0x11
	MsgAppendEntries    uint8 = 0x12
	MsgAppendEntriesReply uint8 = 0x13
	MsgInstallSnapshot  uint8 = 0x14
	MsgInstallSnapshotReply uint8 = 0x15

	// Response / error messages
	MsgGetResponse    uint8 = 0x20
	MsgSetResponse    uint8 = 0x21
	MsgDeleteResponse uint8 = 0x22
	MsgError          uint8 = 0x2F
)

// ─────────────────────────────────────────────────────────────────────────────
// Log entry (shared by storage and Raft)
// ─────────────────────────────────────────────────────────────────────────────

// LogEntry is a single entry in the Raft log.
// Command holds a gob-encoded SetRequest or DeleteRequest.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command []byte // serialised SetRequest or DeleteRequest
}

// CommandType identifies the operation stored inside LogEntry.Command.
type CommandType uint8

const (
	CmdSet    CommandType = 1
	CmdDelete CommandType = 2
)

// Command is the envelope stored in LogEntry.Command.
type Command struct {
	Type  CommandType
	Key   string
	Value string // empty for CmdDelete
}

// ─────────────────────────────────────────────────────────────────────────────
// Client RPC request / response messages
// ─────────────────────────────────────────────────────────────────────────────

// GetRequest is sent by the client to retrieve a value by key.
type GetRequest struct {
	Key string
}

// GetResponse is sent back to the client after a Get.
type GetResponse struct {
	Value string
	Found bool
}

// SetRequest is sent by the client to store a key-value pair.
type SetRequest struct {
	Key   string
	Value string
}

// SetResponse is sent back to the client after a successful Set.
type SetResponse struct {
	Success bool
}

// DeleteRequest is sent by the client to remove a key.
type DeleteRequest struct {
	Key string
}

// DeleteResponse is sent back to the client after a Delete.
type DeleteResponse struct {
	Success bool
}

// ErrorResponse is returned when an operation fails.
// If the node is not the partition leader, LeaderAddr is populated so the
// client can retry against the actual leader.
type ErrorResponse struct {
	Message    string
	LeaderAddr string // non-empty when ErrCode == ErrNotLeader
	ErrCode    ErrorCode
}

// ErrorCode classifies the type of error.
type ErrorCode uint8

const (
	ErrUnknown    ErrorCode = 0
	ErrNotLeader  ErrorCode = 1
	ErrKeyNotFound ErrorCode = 2
	ErrInternal   ErrorCode = 3
	ErrTimeout    ErrorCode = 4
)

// ─────────────────────────────────────────────────────────────────────────────
// Raft RPC messages
// ─────────────────────────────────────────────────────────────────────────────

// RequestVoteArgs is sent by a Candidate to solicit votes from peers.
type RequestVoteArgs struct {
	Term         uint64 // candidate's current term
	CandidateID  int    // candidate's node ID
	LastLogIndex uint64 // index of candidate's last log entry
	LastLogTerm  uint64 // term of candidate's last log entry
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64 // responder's current term (so candidate updates itself)
	VoteGranted bool
}

// AppendEntriesArgs is sent by the Leader to replicate log entries and as a
// periodic heartbeat (Entries is empty for heartbeats).
type AppendEntriesArgs struct {
	Term         uint64     // leader's current term
	LeaderID     int        // so followers can redirect clients
	PrevLogIndex uint64     // index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // term of PrevLogIndex entry
	Entries      []LogEntry // empty for heartbeat
	LeaderCommit uint64     // leader's commitIndex
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term          uint64 // responder's current term (so leader updates itself)
	Success       bool
	// ConflictIndex and ConflictTerm allow the leader to quickly back up.
	ConflictIndex uint64
	ConflictTerm  uint64
}

// InstallSnapshotArgs is sent by the Leader to a lagging follower so it can
// catch up by installing a full snapshot rather than replaying a long log.
type InstallSnapshotArgs struct {
	Term              uint64 // leader's current term
	LeaderID          int
	LastIncludedIndex uint64            // snapshot replaces all entries up through this index
	LastIncludedTerm  uint64            // term of LastIncludedIndex
	Data              map[string]string // full KV state at snapshot point
}

// InstallSnapshotReply is the response to an InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term uint64 // responder's current term
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot (used internally by storage and Raft)
// ─────────────────────────────────────────────────────────────────────────────

// Snapshot captures the full state machine state at a given log position.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              map[string]string // full KV map
}
