package storage

import (
	"fmt"
	"log/slog"

	"github.com/Zeudon/Distributed-Log-Based-Database/proto"
)

// Compact performs log compaction after a snapshot has been successfully saved.
// It removes all WAL entries whose Index is strictly less than
// snap.LastIncludedIndex, since those entries are now captured in the snapshot.
//
// Call order on the leader / replica:
//  1. MemTable.Snapshot()          → collect current KV state
//  2. SnapshotManager.Save(snap)   → persist snapshot atomically
//  3. Compact(snap, wal)           → truncate covered WAL entries
func Compact(snap proto.Snapshot, wal *WAL) error {
	if snap.LastIncludedIndex == 0 {
		return nil // nothing to compact
	}

	beforeCount := countWALEntries(wal)

	if err := wal.TruncateBefore(snap.LastIncludedIndex); err != nil {
		return fmt.Errorf("compact: truncate WAL: %w", err)
	}

	afterCount := countWALEntries(wal)
	slog.Info("WAL compacted",
		"snapshotIndex", snap.LastIncludedIndex,
		"entriesRemoved", beforeCount-afterCount,
		"entriesRemaining", afterCount)
	return nil
}

// countWALEntries reads the WAL to count how many entries it contains.
// Used only for logging; errors are silently ignored.
func countWALEntries(wal *WAL) int {
	entries, err := wal.ReadAll()
	if err != nil {
		return -1
	}
	return len(entries)
}
