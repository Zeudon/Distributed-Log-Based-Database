package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/Zeudon/Distributed-Log-Based-Database/proto"
)

// RaftRole represents the three possible states of a Raft node.
type RaftRole int

const (
	Follower  RaftRole = iota
	Candidate RaftRole = iota
	Leader    RaftRole = iota
)

func (r RaftRole) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

// persistentState holds all Raft state that must survive crashes.
// It is written to disk on every mutation of currentTerm, votedFor, or log.
type persistentState struct {
	CurrentTerm uint64
	VotedFor    int // -1 means "none"
	Log         []proto.LogEntry
}

// stateStore manages durable persistence of persistentState.
type stateStore struct {
	mu   sync.Mutex
	path string
}

func newStateStore(dataDir string) *stateStore {
	return &stateStore{path: filepath.Join(dataDir, "raft_state.gob")}
}

// load reads persisted state from disk.
// Returns a zeroed persistentState with VotedFor = -1 if no file exists.
func (s *stateStore) load() (persistentState, error) {
	ps := persistentState{VotedFor: -1}

	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return ps, nil
	}
	if err != nil {
		return ps, fmt.Errorf("raft state load: %w", err)
	}

	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&ps); err != nil {
		return ps, fmt.Errorf("raft state decode: %w", err)
	}
	slog.Info("Raft state loaded",
		"term", ps.CurrentTerm,
		"votedFor", ps.VotedFor,
		"logLen", len(ps.Log))
	return ps, nil
}

// save durably persists ps to disk (write tmp + rename).
func (s *stateStore) save(ps persistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(ps); err != nil {
		return fmt.Errorf("raft state encode: %w", err)
	}

	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("raft state write tmp: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("raft state rename: %w", err)
	}
	return nil
}
