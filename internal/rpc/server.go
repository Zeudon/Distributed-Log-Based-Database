package rpc

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
)

// HandlerFunc processes a decoded RPC payload and returns a reply type and
// value to send back to the caller, or an error.
type HandlerFunc func(msgType uint8, payload []byte) (replyType uint8, reply any, err error)

// Server is a TCP RPC server that dispatches incoming messages to registered
// handlers based on message type.
type Server struct {
	addr     string
	handlers map[uint8]HandlerFunc
	mu       sync.RWMutex
	listener net.Listener
}

// NewServer creates a new RPC server that will listen on addr.
func NewServer(addr string) *Server {
	return &Server{
		addr:     addr,
		handlers: make(map[uint8]HandlerFunc),
	}
}

// RegisterHandler registers a handler for a specific message type.
// Must be called before Listen.
func (s *Server) RegisterHandler(msgType uint8, fn HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[msgType] = fn
}

// Listen starts accepting TCP connections. It blocks until the listener is
// closed. Call it in a goroutine.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("rpc server: listen %s: %w", s.addr, err)
	}
	s.listener = ln
	slog.Info("RPC server listening", "addr", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			// Listener was closed — exit cleanly.
			return nil
		}
		go s.handleConn(conn)
	}
}

// Close shuts down the listener, causing Listen to return.
func (s *Server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
}

// handleConn processes all messages arriving on a single TCP connection.
// The connection is kept open (persistent) and messages are handled in a loop.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()

	for {
		msgType, payload, err := Decode(conn)
		if err != nil {
			// EOF or connection reset — caller disconnected.
			slog.Debug("RPC connection closed", "remote", remote, "err", err)
			return
		}

		s.mu.RLock()
		handler, ok := s.handlers[msgType]
		s.mu.RUnlock()

		if !ok {
			slog.Warn("RPC: no handler for message type", "type", msgType)
			continue
		}

		replyType, reply, err := handler(msgType, payload)
		if err != nil {
			slog.Error("RPC handler error", "type", msgType, "err", err)
			continue
		}

		if err := Encode(conn, replyType, reply); err != nil {
			slog.Error("RPC: failed to send reply", "type", replyType, "err", err)
			return
		}
	}
}
