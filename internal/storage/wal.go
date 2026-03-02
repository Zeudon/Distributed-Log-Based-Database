package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Zeudon/Distributed-Log-Based-Database/proto"
)

// WAL is a write-ahead log that durably persists Raft log entries as an
// append-only file.
//
// On-disk format (repeated per entry):
//
//	┌───────────────────────┬──────────────────────────────┐
//	│ 4 bytes (uint32)      │  N bytes                     │
//	│  gob payload length   │  gob-encoded proto.LogEntry  │
//	└───────────────────────┴──────────────────────────────┘
type WAL struct {
	mu             sync.Mutex
	path           string
	file           *os.File
	lastAppendTime time.Time
}

// OpenWAL opens (or creates) the WAL file at <dataDir>/wal.log.
func OpenWAL(dataDir string) (*WAL, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create data dir: %w", err)
	}
	path := filepath.Join(dataDir, "wal.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open file: %w", err)
	}
	slog.Info("WAL opened", "path", path)
	return &WAL{path: path, file: f}, nil
}

// Append encodes entry and appends it to the WAL.
func (w *WAL) Append(entry proto.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(entry); err != nil {
		return fmt.Errorf("wal append: encode: %w", err)
	}

	data := buf.Bytes()
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := w.file.Write(lenBuf); err != nil {
		return fmt.Errorf("wal append: write length: %w", err)
	}
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("wal append: write data: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.lastAppendTime = time.Now()
	return nil
}

// LastAppendTime returns the wall-clock time of the most recent successful Append.
// Returns the zero time if no entry has been appended yet.
func (w *WAL) LastAppendTime() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastAppendTime
}

// ReadAll reads all entries from the WAL from the beginning of the file.
func (w *WAL) ReadAll() ([]proto.LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := os.Open(w.path)
	if err != nil {
		return nil, fmt.Errorf("wal readall: open: %w", err)
	}
	defer f.Close()

	var entries []proto.LogEntry
	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(f, lenBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("wal readall: read length: %w", err)
		}
		length := binary.BigEndian.Uint32(lenBuf)
		data := make([]byte, length)
		if _, err := io.ReadFull(f, data); err != nil {
			return nil, fmt.Errorf("wal readall: read data: %w", err)
		}

		var entry proto.LogEntry
		dec := gob.NewDecoder(bytes.NewReader(data))
		if err := dec.Decode(&entry); err != nil {
			return nil, fmt.Errorf("wal readall: decode: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// TruncateAfter removes all entries with Index > afterIndex by rewriting the
// WAL with only entries whose Index <= afterIndex. Used by Raft when a
// follower needs to overwrite conflicting log entries.
func (w *WAL) TruncateAfter(afterIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := w.readAllLocked()
	if err != nil {
		return err
	}

	var keep []proto.LogEntry
	for _, e := range entries {
		if e.Index <= afterIndex {
			keep = append(keep, e)
		}
	}
	return w.rewrite(keep)
}

// TruncateBefore removes all entries with Index < beforeIndex by rewriting
// the WAL keeping only entries whose Index >= beforeIndex. Used after a
// snapshot is persisted (log compaction).
func (w *WAL) TruncateBefore(beforeIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := w.readAllLocked()
	if err != nil {
		return err
	}

	var keep []proto.LogEntry
	for _, e := range entries {
		if e.Index >= beforeIndex {
			keep = append(keep, e)
		}
	}
	return w.rewrite(keep)
}

// Close syncs and closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.file.Sync()
	return w.file.Close()
}

// ─── internal helpers ──────────────────────────────────────────────────────

// readAllLocked reads all entries without acquiring the mutex (caller holds it).
func (w *WAL) readAllLocked() ([]proto.LogEntry, error) {
	f, err := os.Open(w.path)
	if err != nil {
		return nil, fmt.Errorf("wal: open for read: %w", err)
	}
	defer f.Close()

	var entries []proto.LogEntry
	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(f, lenBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		length := binary.BigEndian.Uint32(lenBuf)
		data := make([]byte, length)
		if _, err := io.ReadFull(f, data); err != nil {
			return nil, err
		}
		var entry proto.LogEntry
		dec := gob.NewDecoder(bytes.NewReader(data))
		if err := dec.Decode(&entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// rewrite atomically replaces the WAL file with only the given entries using
// a write-to-tmp + rename pattern to avoid corruption on crash.
func (w *WAL) rewrite(entries []proto.LogEntry) error {
	tmpPath := w.path + ".tmp"
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("wal rewrite: create tmp: %w", err)
	}

	for _, entry := range entries {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(entry); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("wal rewrite: encode: %w", err)
		}
		data := buf.Bytes()
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err := tmp.Write(lenBuf); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("wal rewrite: write len: %w", err)
		}
		if _, err := tmp.Write(data); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("wal rewrite: write data: %w", err)
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	tmp.Close()

	// Atomic rename.
	if err := os.Rename(tmpPath, w.path); err != nil {
		return fmt.Errorf("wal rewrite: rename: %w", err)
	}

	// Reopen the main file handle in append mode.
	w.file.Close()
	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal rewrite: reopen: %w", err)
	}
	w.file = f
	return nil
}
