package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Zeudon/Distributed-Log-Based-Database/proto"
)

// SnapshotManager handles persisting and loading Snapshots.
// Snapshots are written atomically via a tmp file + rename.
type SnapshotManager struct {
	dataDir          string
	mu               sync.Mutex
	lastSnapshotTime time.Time
}

// NewSnapshotManager creates a SnapshotManager that stores snapshots in dataDir.
func NewSnapshotManager(dataDir string) *SnapshotManager {
	return &SnapshotManager{dataDir: dataDir}
}

// Save serialises snap and writes it atomically to <dataDir>/snapshot.bin.
func (sm *SnapshotManager) Save(snap proto.Snapshot) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if err := os.MkdirAll(sm.dataDir, 0o755); err != nil {
		return fmt.Errorf("snapshot save: mkdir: %w", err)
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(snap); err != nil {
		return fmt.Errorf("snapshot save: encode: %w", err)
	}

	tmpPath := filepath.Join(sm.dataDir, "snapshot.bin.tmp")
	finalPath := filepath.Join(sm.dataDir, "snapshot.bin")

	if err := os.WriteFile(tmpPath, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("snapshot save: write tmp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("snapshot save: rename: %w", err)
	}

	slog.Info("Snapshot saved",
		"lastIncludedIndex", snap.LastIncludedIndex,
		"lastIncludedTerm", snap.LastIncludedTerm,
		"keys", len(snap.Data))
	sm.lastSnapshotTime = time.Now()
	return nil
}

// LastSnapshotTime returns the wall-clock time of the most recent successful Save.
// Returns the zero time if no snapshot has been saved yet.
func (sm *SnapshotManager) LastSnapshotTime() time.Time {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.lastSnapshotTime
}

// Load reads and decodes the snapshot from <dataDir>/snapshot.bin.
// Returns (zero-value Snapshot, nil) if no snapshot file exists yet.
func (sm *SnapshotManager) Load() (proto.Snapshot, error) {
	path := filepath.Join(sm.dataDir, "snapshot.bin")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return proto.Snapshot{}, nil
	}
	if err != nil {
		return proto.Snapshot{}, fmt.Errorf("snapshot load: read: %w", err)
	}

	var snap proto.Snapshot
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&snap); err != nil {
		return proto.Snapshot{}, fmt.Errorf("snapshot load: decode: %w", err)
	}

	slog.Info("Snapshot loaded",
		"lastIncludedIndex", snap.LastIncludedIndex,
		"lastIncludedTerm", snap.LastIncludedTerm,
		"keys", len(snap.Data))
	return snap, nil
}

// Exists returns true if a snapshot file is present.
func (sm *SnapshotManager) Exists() bool {
	_, err := os.Stat(filepath.Join(sm.dataDir, "snapshot.bin"))
	return err == nil
}
