// Package gateway implements a stateless REST API gateway that routes
// single-key operations to the correct partition and fans out aggregate
// queries across all partitions concurrently.
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

// PartitionConfig describes one partition's set of HTTP replica addresses.
type PartitionConfig struct {
	ID        int
	HTTPAddrs []string // one per replica, e.g. ["node-p0-r0:8080", ...]
}

// ─────────────────────────────────────────────────────────────────────────────
// Gateway
// ─────────────────────────────────────────────────────────────────────────────

// Gateway is a stateless HTTP proxy that routes to the correct partition and
// aggregates responses from multiple partitions for fan-out endpoints.
type Gateway struct {
	partitions []PartitionConfig
	leaderMu   sync.RWMutex
	leaderAddr map[int]string // partitionID → current known leader HTTP addr

	httpCli *http.Client
	port    int
	srv     *http.Server
}

// New creates a new Gateway.
func New(partitions []PartitionConfig, port int) *Gateway {
	g := &Gateway{
		partitions: partitions,
		leaderAddr: make(map[int]string),
		httpCli: &http.Client{
			Timeout: 10 * time.Second,
			// Do not follow redirects automatically — we handle 307 ourselves.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		port: port,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/keys", g.handleCreate)
	mux.HandleFunc("PUT /v1/keys/{key}", g.handleUpdate)
	mux.HandleFunc("GET /v1/keys/{key}", g.handleGetOne)
	mux.HandleFunc("GET /v1/keys", g.handleGetAll)
	mux.HandleFunc("GET /v1/health", g.handleHealth)

	g.srv = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	return g
}

// Listen starts the HTTP server and blocks until an error or Close is called.
func (g *Gateway) Listen() error {
	ln, err := net.Listen("tcp", g.srv.Addr)
	if err != nil {
		return fmt.Errorf("gateway: listen %s: %w", g.srv.Addr, err)
	}
	return g.srv.Serve(ln)
}

// Close shuts the gateway down gracefully.
func (g *Gateway) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return g.srv.Shutdown(ctx)
}

// ─────────────────────────────────────────────────────────────────────────────
// Partition routing helpers
// ─────────────────────────────────────────────────────────────────────────────

// partitionIDForKey computes the partition for a key using FNV-1a hash.
func (g *Gateway) partitionIDForKey(key string) int {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	if h < 0 { // uint32 is never negative, but keep the guard
		h = -h
	}
	return int(h) % len(g.partitions)
}

// knownLeader returns the cached leader HTTP address for a partition, or
// falls back to replica[0] if the leader is unknown.
func (g *Gateway) knownLeader(partID int) string {
	g.leaderMu.RLock()
	addr := g.leaderAddr[partID]
	g.leaderMu.RUnlock()
	if addr != "" {
		return addr
	}
	if partID < len(g.partitions) && len(g.partitions[partID].HTTPAddrs) > 0 {
		return g.partitions[partID].HTTPAddrs[0]
	}
	return ""
}

// updateLeader caches a new leader address for a partition.
func (g *Gateway) updateLeader(partID int, addr string) {
	g.leaderMu.Lock()
	g.leaderAddr[partID] = addr
	g.leaderMu.Unlock()
}

// ─────────────────────────────────────────────────────────────────────────────
// Proxying helpers
// ─────────────────────────────────────────────────────────────────────────────

// proxyWrite forwards a write request (POST/PUT) to the correct partition
// leader, following up to 3 leader redirects transparently.
func (g *Gateway) proxyWrite(w http.ResponseWriter, r *http.Request, partID int) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("failed to read request body"))
		return
	}

	const maxRetries = 4
	for attempt := 0; attempt < maxRetries; attempt++ {
		addr := g.knownLeader(partID)
		if addr == "" {
			writeJSON(w, http.StatusBadGateway, errBody("no known address for partition "+strconv.Itoa(partID)))
			return
		}

		url := "http://" + addr + r.URL.Path
		req, err := http.NewRequestWithContext(r.Context(), r.Method, url, bytes.NewReader(body))
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := g.httpCli.Do(req)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, errBody("upstream error: "+err.Error()))
			return
		}
		defer resp.Body.Close() //nolint:errcheck

		// If the node redirects us to the leader, update cache and retry.
		if resp.StatusCode == http.StatusTemporaryRedirect {
			if leaderHTTP := resp.Header.Get("X-Raft-Leader-Http-Addr"); leaderHTTP != "" {
				g.updateLeader(partID, leaderHTTP)
			}
			continue
		}

		// Proxy the response as-is.
		copyResponse(w, resp)
		return
	}
	writeJSON(w, http.StatusBadGateway, errBody("too many leader redirects for partition "+strconv.Itoa(partID)))
}

// proxyRead forwards a read request (GET /v1/keys/{key}) to any replica
// of the correct partition (prefers known leader, falls back to replica[0]).
func (g *Gateway) proxyRead(w http.ResponseWriter, r *http.Request, partID int) {
	addr := g.knownLeader(partID)
	if addr == "" {
		writeJSON(w, http.StatusBadGateway, errBody("no known address for partition "+strconv.Itoa(partID)))
		return
	}
	url := "http://" + addr + r.URL.Path
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	resp, err := g.httpCli.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, errBody("upstream error: "+err.Error()))
		return
	}
	defer resp.Body.Close() //nolint:errcheck
	copyResponse(w, resp)
}

// ─────────────────────────────────────────────────────────────────────────────
// Route handlers
// ─────────────────────────────────────────────────────────────────────────────

// handleCreate proxies POST /v1/keys to the correct partition leader.
func (g *Gateway) handleCreate(w http.ResponseWriter, r *http.Request) {
	// Peek at the key in the body to route it.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("failed to read body"))
		return
	}
	var payload struct {
		Key string `json:"key"`
	}
	_ = json.Unmarshal(body, &payload)
	partID := g.partitionIDForKey(payload.Key)

	// Restore body for proxyWrite.
	r.Body = io.NopCloser(bytes.NewReader(body))
	g.proxyWrite(w, r, partID)
}

// handleUpdate proxies PUT /v1/keys/{key} to the correct partition leader.
func (g *Gateway) handleUpdate(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	partID := g.partitionIDForKey(key)
	g.proxyWrite(w, r, partID)
}

// handleGetOne proxies GET /v1/keys/{key} to the correct partition.
func (g *Gateway) handleGetOne(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	partID := g.partitionIDForKey(key)
	g.proxyRead(w, r, partID)
}

// ─────────────────────────────────────────────────────────────────────────────
// Fan-out: GET /v1/keys — all partitions
// ─────────────────────────────────────────────────────────────────────────────

type partitionKeysResult struct {
	PartitionID int     `json:"partition_id"`
	Count       int     `json:"count"`
	Keys        []kvRow `json:"keys"`
}

type kvRow struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type partitionKeysError struct {
	PartitionID int    `json:"partition_id"`
	Error       string `json:"error"`
}

func (g *Gateway) handleGetAll(w http.ResponseWriter, r *http.Request) {
	type result struct {
		data partitionKeysResult
		err  *partitionKeysError
	}

	results := make([]result, len(g.partitions))
	var wg sync.WaitGroup
	for i, p := range g.partitions {
		wg.Add(1)
		go func(idx int, part PartitionConfig) {
			defer wg.Done()
			addr := g.knownLeader(part.ID)
			if addr == "" {
				results[idx].err = &partitionKeysError{PartitionID: part.ID, Error: "no known address"}
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/v1/keys", nil)
			if err != nil {
				results[idx].err = &partitionKeysError{PartitionID: part.ID, Error: err.Error()}
				return
			}
			resp, err := g.httpCli.Do(req)
			if err != nil {
				results[idx].err = &partitionKeysError{PartitionID: part.ID, Error: err.Error()}
				return
			}
			defer resp.Body.Close() //nolint:errcheck
			var pr partitionKeysResult
			if decErr := json.NewDecoder(resp.Body).Decode(&pr); decErr != nil {
				results[idx].err = &partitionKeysError{PartitionID: part.ID, Error: "decode error: " + decErr.Error()}
				return
			}
			results[idx].data = pr
		}(i, p)
	}
	wg.Wait()

	successPartitions := make([]partitionKeysResult, 0, len(g.partitions))
	errs := make([]partitionKeysError, 0)
	totalCount := 0
	for _, r := range results {
		if r.err != nil {
			errs = append(errs, *r.err)
		} else {
			successPartitions = append(successPartitions, r.data)
			totalCount += r.data.Count
		}
	}

	statusCode := http.StatusOK
	if len(errs) > 0 && len(successPartitions) > 0 {
		statusCode = http.StatusPartialContent
	} else if len(errs) > 0 && len(successPartitions) == 0 {
		statusCode = http.StatusBadGateway
	}

	resp := map[string]any{
		"total_count": totalCount,
		"partitions":  successPartitions,
	}
	if len(errs) > 0 {
		resp["errors"] = errs
	}
	writeJSON(w, statusCode, resp)
}

// ─────────────────────────────────────────────────────────────────────────────
// Fan-out: GET /v1/health — all 9 nodes
// ─────────────────────────────────────────────────────────────────────────────

type replicaHealthRow struct {
	NodeID      int    `json:"node_id"`
	Addr        string `json:"addr"`
	Role        string `json:"role,omitempty"`
	Status      string `json:"status"` // "up" | "down"
	CommitIndex uint64 `json:"commit_index,omitempty"`
	Term        uint64 `json:"term,omitempty"`
}

type partitionHealthResult struct {
	PartitionID int                `json:"partition_id"`
	LeaderID    int                `json:"leader_id,omitempty"`
	LeaderAddr  string             `json:"leader_addr,omitempty"`
	Term        uint64             `json:"term,omitempty"`
	Replicas    []replicaHealthRow `json:"replicas"`
}

// nodeHealthSummary is a subset of the per-node /v1/health response we
// care about for aggregation.
type nodeHealthSummary struct {
	NodeID int    `json:"node_id"`
	Role   string `json:"role"`
	Raft   struct {
		Term        uint64 `json:"term"`
		LeaderID    int    `json:"leader_id"`
		LeaderAddr  string `json:"leader_addr"`
		CommitIndex uint64 `json:"commit_index"`
	} `json:"raft"`
}

func (g *Gateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	type probeResult struct {
		partID int
		addr   string
		data   *nodeHealthSummary
		err    string
	}

	// Fan out to all replicas across all partitions.
	var allAddrs []probeResult
	for _, p := range g.partitions {
		for _, addr := range p.HTTPAddrs {
			allAddrs = append(allAddrs, probeResult{partID: p.ID, addr: addr})
		}
	}

	results := make([]probeResult, len(allAddrs))
	var wg sync.WaitGroup
	for i, pr := range allAddrs {
		wg.Add(1)
		go func(idx int, item probeResult) {
			defer wg.Done()
			results[idx] = item
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+item.addr+"/v1/health", nil)
			if err != nil {
				results[idx].err = err.Error()
				return
			}
			resp, err := g.httpCli.Do(req)
			if err != nil {
				results[idx].err = err.Error()
				return
			}
			defer resp.Body.Close() //nolint:errcheck
			var hs nodeHealthSummary
			if decErr := json.NewDecoder(resp.Body).Decode(&hs); decErr != nil {
				results[idx].err = "decode: " + decErr.Error()
				return
			}
			results[idx].data = &hs
		}(i, pr)
	}
	wg.Wait()

	// Group by partition.
	partMap := make(map[int]*partitionHealthResult)
	for _, p := range g.partitions {
		partMap[p.ID] = &partitionHealthResult{PartitionID: p.ID, Replicas: []replicaHealthRow{}}
	}

	nodesUp, nodesDown := 0, 0
	for _, res := range results {
		ph := partMap[res.partID]
		row := replicaHealthRow{Addr: res.addr}
		if res.err != "" {
			row.Status = "down"
			nodesDown++
		} else {
			row.Status = "up"
			nodesUp++
			if res.data != nil {
				row.Role = res.data.Role
				row.NodeID = res.data.NodeID
				row.CommitIndex = res.data.Raft.CommitIndex
				row.Term = res.data.Raft.Term
				// Populate leader info on the partition result.
				if res.data.Role == "Leader" {
					ph.LeaderAddr = res.addr
					ph.Term = res.data.Raft.Term
					ph.LeaderID = res.data.Raft.LeaderID
				}
			}
		}
		ph.Replicas = append(ph.Replicas, row)
	}

	// Build ordered list.
	partitions := make([]partitionHealthResult, 0, len(g.partitions))
	for _, p := range g.partitions {
		partitions = append(partitions, *partMap[p.ID])
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"total_nodes": len(allAddrs),
		"nodes_up":    nodesUp,
		"nodes_down":  nodesDown,
		"partitions":  partitions,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]string {
	return map[string]string{"error": msg}
}

// copyResponse streams a upstream response back to the client preserving
// status code and Content-Type.
func copyResponse(w http.ResponseWriter, resp *http.Response) {
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	// Copy any leader-hint headers the caller may want to inspect.
	if lh := resp.Header.Get("X-Raft-Leader-Http-Addr"); lh != "" {
		w.Header().Set("X-Raft-Leader-Http-Addr", lh)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
