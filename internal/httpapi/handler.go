// Package httpapi provides a REST HTTP API layer for each database node.
// It sits alongside the existing binary RPC server and exposes the same
// operations (plus Raft metadata) over JSON/HTTP.
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// NodeAccessor interface
// ─────────────────────────────────────────────────────────────────────────────

// NodeAccessor is the narrow interface that the HTTP layer requires from node.Node.
// Using an interface keeps httpapi decoupled from the concrete node type and
// makes the handlers trivially testable with a mock.
type NodeAccessor interface {
	// KV operations
	MemGet(key string) (string, bool)
	MemSnapshot() map[string]string
	SubmitSet(key, value string) error // blocks until committed or timed out
	SubmitDelete(key string) error     // blocks until committed or timed out

	// Identity
	NodeID() int
	PartitionID() int
	SelfAddr() string
	PeerAddresses() []string
	PeerNodeIDs() []int

	// Raft state
	RaftTerm() uint64
	RaftRole() string
	RaftLeaderID() int
	RaftLeaderAddr() string
	RaftCommitIndex() uint64
	RaftLastApplied() uint64
	RaftLastLogIndex() uint64
	RaftSnapshotIndex() uint64
	RaftIsLeader() bool

	// Timestamps
	LastCommitTime() time.Time
	LastWALAppendTime() time.Time
	LastSnapshotTime() time.Time
	SnapshotExists() bool
	MemTableLen() int
}

// ─────────────────────────────────────────────────────────────────────────────
// Server
// ─────────────────────────────────────────────────────────────────────────────

// Server is the HTTP server that wraps a NodeAccessor.
type Server struct {
	accessor NodeAccessor
	port     int
	srv      *http.Server
}

// NewServer creates a Server that delegates to accessor and listens on port.
func NewServer(a NodeAccessor, port int) *Server {
	s := &Server{accessor: a, port: port}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/keys", s.handleCreate)
	mux.HandleFunc("PUT /v1/keys/{key}", s.handleUpdate)
	mux.HandleFunc("GET /v1/keys/{key}", s.handleGetOne)
	mux.HandleFunc("GET /v1/keys", s.handleGetAll)
	mux.HandleFunc("GET /v1/health", s.handleHealth)
	s.srv = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	return s
}

// Listen starts the HTTP server and blocks until an error or Close is called.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("httpapi: listen %s: %w", s.srv.Addr, err)
	}
	return s.srv.Serve(ln)
}

// Close shuts the HTTP server down gracefully.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared JSON response types
// ─────────────────────────────────────────────────────────────────────────────

type raftInfo struct {
	Term        uint64 `json:"term"`
	Role        string `json:"role"`
	LeaderID    int    `json:"leader_id"`
	LeaderAddr  string `json:"leader_addr"`
	CommitIndex uint64 `json:"commit_index"`
}

type errorBody struct {
	Error       string   `json:"error"`
	Key         string   `json:"key,omitempty"`
	PartitionID int      `json:"partition_id"`
	DurationMs  float64  `json:"duration_ms"`
	Raft        raftInfo `json:"raft"`
}

// leaderHTTPAddr derives the HTTP address from the leader's RPC address by
// replacing the RPC port with the node's own HTTP port. All nodes share the
// same HTTP_PORT value, so this substitution is safe.
func leaderHTTPAddr(rpcAddr string, httpPort int) string {
	host, _, err := net.SplitHostPort(rpcAddr)
	if err != nil || host == "" {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(httpPort))
}

func (s *Server) raftInfoNow() raftInfo {
	return raftInfo{
		Term:        s.accessor.RaftTerm(),
		Role:        s.accessor.RaftRole(),
		LeaderID:    s.accessor.RaftLeaderID(),
		LeaderAddr:  s.accessor.RaftLeaderAddr(),
		CommitIndex: s.accessor.RaftCommitIndex(),
	}
}

// writeJSON encodes v as JSON with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// notLeaderRedirect returns a 307 response with location and leader header when
// this node is not the Raft leader for a write operation.
func (s *Server) notLeaderRedirect(w http.ResponseWriter, key string, start time.Time) {
	leaderRPC := s.accessor.RaftLeaderAddr()
	httpAddr := leaderHTTPAddr(leaderRPC, s.port)
	if httpAddr != "" {
		w.Header().Set("X-Raft-Leader-Http-Addr", httpAddr)
	}
	writeJSON(w, http.StatusTemporaryRedirect, errorBody{
		Error:       "not leader",
		Key:         key,
		PartitionID: s.accessor.PartitionID(),
		DurationMs:  ms(start),
		Raft:        s.raftInfoNow(),
	})
}

func ms(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000.0
}

func optionalTime(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /v1/keys — insert a new key (409 if already exists)
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	partID := s.accessor.PartitionID()

	var body struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body; 'key' and 'value' required"})
		return
	}

	if !s.accessor.RaftIsLeader() {
		s.notLeaderRedirect(w, body.Key, start)
		return
	}

	// Duplicate check on local MemTable (optimistic).
	if _, found := s.accessor.MemGet(body.Key); found {
		writeJSON(w, http.StatusConflict, errorBody{
			Error:       "key already exists",
			Key:         body.Key,
			PartitionID: partID,
			DurationMs:  ms(start),
			Raft:        s.raftInfoNow(),
		})
		return
	}

	if err := s.accessor.SubmitSet(body.Key, body.Value); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{
			Error:       err.Error(),
			Key:         body.Key,
			PartitionID: partID,
			DurationMs:  ms(start),
			Raft:        s.raftInfoNow(),
		})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"key":          body.Key,
		"value":        body.Value,
		"partition_id": partID,
		"duration_ms":  ms(start),
		"raft":         s.raftInfoNow(),
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// PUT /v1/keys/{key} — update an existing key (404 if not found)
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	key := r.PathValue("key")
	partID := s.accessor.PartitionID()

	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body; 'value' required"})
		return
	}

	if !s.accessor.RaftIsLeader() {
		s.notLeaderRedirect(w, key, start)
		return
	}

	if _, found := s.accessor.MemGet(key); !found {
		writeJSON(w, http.StatusNotFound, errorBody{
			Error:       "key not found",
			Key:         key,
			PartitionID: partID,
			DurationMs:  ms(start),
			Raft:        s.raftInfoNow(),
		})
		return
	}

	if err := s.accessor.SubmitSet(key, body.Value); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{
			Error:       err.Error(),
			Key:         key,
			PartitionID: partID,
			DurationMs:  ms(start),
			Raft:        s.raftInfoNow(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"key":          key,
		"value":        body.Value,
		"partition_id": partID,
		"duration_ms":  ms(start),
		"raft":         s.raftInfoNow(),
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /v1/keys/{key} — retrieve a single key
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) handleGetOne(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	key := r.PathValue("key")
	partID := s.accessor.PartitionID()

	value, found := s.accessor.MemGet(key)
	if !found {
		writeJSON(w, http.StatusNotFound, errorBody{
			Error:       "key not found",
			Key:         key,
			PartitionID: partID,
			DurationMs:  ms(start),
			Raft:        s.raftInfoNow(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"key":          key,
		"value":        value,
		"partition_id": partID,
		"duration_ms":  ms(start),
		"raft":         s.raftInfoNow(),
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /v1/keys — retrieve all key-value pairs on this partition
// ─────────────────────────────────────────────────────────────────────────────

type kvPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) handleGetAll(w http.ResponseWriter, r *http.Request) {
	snapshot := s.accessor.MemSnapshot()
	pairs := make([]kvPair, 0, len(snapshot))
	for k, v := range snapshot {
		pairs = append(pairs, kvPair{Key: k, Value: v})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"partition_id": s.accessor.PartitionID(),
		"count":        len(pairs),
		"keys":         pairs,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /v1/health — node and Raft health
// ─────────────────────────────────────────────────────────────────────────────

type peerInfo struct {
	Addr   string `json:"addr"`
	NodeID int    `json:"node_id"`
	Status string `json:"status"` // "up" | "down"
}

type healthResponse struct {
	NodeID      int    `json:"node_id"`
	PartitionID int    `json:"partition_id"`
	SelfAddr    string `json:"self_addr"`
	Role        string `json:"role"`
	Raft        struct {
		Term          uint64 `json:"term"`
		LeaderID      int    `json:"leader_id"`
		LeaderAddr    string `json:"leader_addr"`
		CommitIndex   uint64 `json:"commit_index"`
		LastApplied   uint64 `json:"last_applied"`
		LastLogIndex  uint64 `json:"last_log_index"`
		SnapshotIndex uint64 `json:"snapshot_index"`
	} `json:"raft"`
	Storage struct {
		MemTableKeyCount  int     `json:"memtable_key_count"`
		SnapshotExists    bool    `json:"snapshot_exists"`
		LastCommitTime    *string `json:"last_commit_time"`
		LastWALAppendTime *string `json:"last_wal_append_time"`
		LastSnapshotTime  *string `json:"last_snapshot_time"`
	} `json:"storage"`
	Peers     []peerInfo `json:"peers"`
	NodesDown int        `json:"nodes_down"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	h := healthResponse{
		NodeID:      s.accessor.NodeID(),
		PartitionID: s.accessor.PartitionID(),
		SelfAddr:    s.accessor.SelfAddr(),
		Role:        s.accessor.RaftRole(),
	}
	h.Raft.Term = s.accessor.RaftTerm()
	h.Raft.LeaderID = s.accessor.RaftLeaderID()
	h.Raft.LeaderAddr = s.accessor.RaftLeaderAddr()
	h.Raft.CommitIndex = s.accessor.RaftCommitIndex()
	h.Raft.LastApplied = s.accessor.RaftLastApplied()
	h.Raft.LastLogIndex = s.accessor.RaftLastLogIndex()
	h.Raft.SnapshotIndex = s.accessor.RaftSnapshotIndex()

	h.Storage.MemTableKeyCount = s.accessor.MemTableLen()
	h.Storage.SnapshotExists = s.accessor.SnapshotExists()
	h.Storage.LastCommitTime = optionalTime(s.accessor.LastCommitTime())
	h.Storage.LastWALAppendTime = optionalTime(s.accessor.LastWALAppendTime())
	h.Storage.LastSnapshotTime = optionalTime(s.accessor.LastSnapshotTime())

	// Probe peer liveness with a short TCP dial timeout.
	peerAddrs := s.accessor.PeerAddresses()
	peerIDs := s.accessor.PeerNodeIDs()
	nodesDown := 0
	for i, addr := range peerAddrs {
		status := "up"
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			status = "down"
			nodesDown++
		} else {
			conn.Close()
		}
		nodeID := -1
		if i < len(peerIDs) {
			nodeID = peerIDs[i]
		}
		h.Peers = append(h.Peers, peerInfo{Addr: addr, NodeID: nodeID, Status: status})
	}
	h.NodesDown = nodesDown

	writeJSON(w, http.StatusOK, h)
}
