// Package storage implements the log-structured storage engine:
//   - WAL (Write-Ahead Log): append-only durable log of Raft entries
//   - MemTable: in-memory key-value state machine
//   - Snapshot: periodic serialised checkpoints of the MemTable
//   - Compaction: WAL truncation after a snapshot is persisted
package storage
