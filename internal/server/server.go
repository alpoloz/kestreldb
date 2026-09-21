package server

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"kestreldb/internal/engine"
)

type Server struct {
	addr string
	db   *engine.DB
	h    *handler

	mu            sync.Mutex
	listener      net.Listener
	connections   map[net.Conn]struct{}
	closed        bool
	replicaOf     string
	replicaID     string
	upstream      net.Conn
	replicaStop   context.CancelFunc
	replicas      map[string]replicaStatus
	replicaOffset uint64
	cluster       *clusterState
}

func New(addr string, db *engine.DB) *Server {
	s := &Server{
		addr:        addr,
		db:          db,
		connections: make(map[net.Conn]struct{}),
		replicas:    make(map[string]replicaStatus),
	}
	s.h = &handler{db: db, server: s}
	return s
}

// SetReplicaOf configures a read-only replica before ListenAndServe starts.
func (s *Server) SetReplicaOf(primaryAddress, replicaID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicaOf = primaryAddress
	s.replicaID = replicaID
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = ln.Close()
		return net.ErrClosed
	}
	s.listener = ln
	s.mu.Unlock()
	s.db.StartExpiration(100*time.Millisecond, 100)
	defer s.db.StopExpiration()
	if s.replicaOf != "" {
		ctx, cancel := context.WithCancel(context.Background())
		s.mu.Lock()
		s.replicaStop = cancel
		s.mu.Unlock()
		go s.runReplica(ctx)
		defer cancel()
	}
	log.Printf("kestreldb listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return nil
		}
		s.connections[conn] = struct{}{}
		s.mu.Unlock()
		go func() {
			s.h.serve(conn)
			s.mu.Lock()
			delete(s.connections, conn)
			s.mu.Unlock()
		}()
	}
}

// Close stops accepting connections, closes active connections, and cancels
// blocked database waiters. Calling Close more than once is safe.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	stopReplica := s.replicaStop
	upstream := s.upstream
	listener := s.listener
	connections := make([]net.Conn, 0, len(s.connections))
	for conn := range s.connections {
		connections = append(connections, conn)
	}
	s.mu.Unlock()
	if stopReplica != nil {
		stopReplica()
	}
	if upstream != nil {
		_ = upstream.Close()
	}

	s.db.CloseWaiters()
	s.db.StopExpiration()
	var closeErr error
	if listener != nil {
		closeErr = listener.Close()
	}
	for _, conn := range connections {
		_ = conn.Close()
	}
	if errors.Is(closeErr, net.ErrClosed) {
		return nil
	}
	return closeErr
}
