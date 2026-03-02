package rpc

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

const (
	maxRetries    = 3
	retryBaseMs   = 50
	dialTimeoutMs = 2000
)

// connEntry wraps a TCP connection with a mutex so callers can serialise
// request/reply pairs over the same persistent connection.
type connEntry struct {
	mu   sync.Mutex
	conn net.Conn
}

// Client manages persistent TCP connections to remote RPC servers with
// automatic reconnection and linear backoff retries.
type Client struct {
	mu    sync.Mutex
	conns map[string]*connEntry
}

// NewClient creates a new RPC client.
func NewClient() *Client {
	return &Client{conns: make(map[string]*connEntry)}
}

// Call sends a request of msgType to addr and returns the raw reply type and
// gob-encoded payload bytes. The caller is responsible for decoding the
// payload into the appropriate type based on replyType.
// Use DecodePayload to decode the returned bytes.
func (c *Client) Call(addr string, msgType uint8, req any) (replyType uint8, payload []byte, err error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(retryBaseMs*(1<<attempt)) * time.Millisecond)
			slog.Debug("RPC retry", "addr", addr, "attempt", attempt)
		}

		entry, connErr := c.getConn(addr)
		if connErr != nil {
			lastErr = connErr
			c.removeConn(addr)
			continue
		}

		entry.mu.Lock()
		encErr := Encode(entry.conn, msgType, req)
		if encErr != nil {
			entry.mu.Unlock()
			lastErr = encErr
			c.removeConn(addr)
			continue
		}

		rt, p, decErr := Decode(entry.conn)
		entry.mu.Unlock()
		if decErr != nil {
			lastErr = decErr
			c.removeConn(addr)
			continue
		}

		return rt, p, nil
	}
	return 0, nil, fmt.Errorf("rpc call to %s failed after %d attempts: %w", addr, maxRetries, lastErr)
}

// Close tears down all persistent connections held by this client.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, entry := range c.conns {
		entry.conn.Close()
		delete(c.conns, addr)
	}
}

// getConn returns an existing connection or dials a new one.
func (c *Client) getConn(addr string) (*connEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.conns[addr]; ok {
		return entry, nil
	}

	conn, err := net.DialTimeout("tcp", addr, time.Duration(dialTimeoutMs)*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("rpc dial %s: %w", addr, err)
	}
	entry := &connEntry{conn: conn}
	c.conns[addr] = entry
	return entry, nil
}

// removeConn removes a stale connection from the pool so the next call dials
// a fresh one.
func (c *Client) removeConn(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.conns[addr]; ok {
		entry.conn.Close()
		delete(c.conns, addr)
	}
}
