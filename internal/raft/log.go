package raft

import "github.com/Zeudon/Distributed-Log-Based-Database/proto"

// lastLogIndex returns the index of the last entry in the log.
// Returns r.snapshotIndex if the log is empty (i.e. everyt entry is
// captured in the installed snapshot).
func (r *RaftNode) lastLogIndex() uint64 {
	if len(r.log) == 0 {
		return r.snapshotIndex
	}
	return r.log[len(r.log)-1].Index
}

// lastLogTerm returns the term of the last entry in the log.
// Returns r.snapshotTerm if the log is empty.
func (r *RaftNode) lastLogTerm() uint64 {
	if len(r.log) == 0 {
		return r.snapshotTerm
	}
	return r.log[len(r.log)-1].Term
}

// entryAt returns the log entry at a given absolute index.
// Returns a zero entry and false if the index is not in the in-memory log
// (i.e. it was compacted away).
func (r *RaftNode) entryAt(index uint64) (proto.LogEntry, bool) {
	if len(r.log) == 0 {
		return proto.LogEntry{}, false
	}
	firstIdx := r.log[0].Index
	if index < firstIdx || index > r.log[len(r.log)-1].Index {
		return proto.LogEntry{}, false
	}
	return r.log[index-firstIdx], true
}

// termAt returns the term for a given absolute log index.
// Handles the special case where index == r.snapshotIndex.
func (r *RaftNode) termAt(index uint64) uint64 {
	if index == r.snapshotIndex {
		return r.snapshotTerm
	}
	entry, ok := r.entryAt(index)
	if !ok {
		return 0
	}
	return entry.Term
}

// appendToLog appends entries to r.log and persists.
func (r *RaftNode) appendToLog(entries []proto.LogEntry) {
	r.log = append(r.log, entries...)
	r.persistLocked()
}

// truncateLogAfter removes all entries with Index > afterIndex.
func (r *RaftNode) truncateLogAfter(afterIndex uint64) {
	if len(r.log) == 0 {
		return
	}
	firstIdx := r.log[0].Index
	if afterIndex < firstIdx {
		r.log = nil
	} else {
		r.log = r.log[:afterIndex-firstIdx+1]
	}
	r.persistLocked()
}

// entriesFrom returns all log entries starting from (and including) fromIndex.
func (r *RaftNode) entriesFrom(fromIndex uint64) []proto.LogEntry {
	if len(r.log) == 0 {
		return nil
	}
	firstIdx := r.log[0].Index
	if fromIndex > r.log[len(r.log)-1].Index {
		return nil
	}
	if fromIndex <= firstIdx {
		return r.log
	}
	return r.log[fromIndex-firstIdx:]
}
