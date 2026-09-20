package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"strconv"
	"time"

	"kestreldb/internal/engine"
	"kestreldb/internal/proto"
)

// Replication uses a private framed stream after REPLSYNC. An S frame replaces
// the replica state; a C frame confirms catch-up from the requested offset;
// each M frame is one ordered mutation. Replicas acknowledge applied offsets.
type replicaStatus struct {
	Offset    uint64
	Connected bool
	LastAck   time.Time
}

func (s *Server) ReplicaStatus() map[string]replicaStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]replicaStatus, len(s.replicas))
	for id, status := range s.replicas {
		result[id] = status
	}
	return result
}

func (s *Server) isReplica() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replicaOf != ""
}

func (s *Server) replicationInfo() string {
	s.mu.Lock()
	primary := s.replicaOf
	replicaOffset := s.replicaOffset
	connected := 0
	for _, status := range s.replicas {
		if status.Connected {
			connected++
		}
	}
	s.mu.Unlock()
	if primary != "" {
		return fmt.Sprintf("role:slave\r\nmaster_host:%s\r\nslave_repl_offset:%d\r\n", primary, replicaOffset)
	}
	return fmt.Sprintf("role:master\r\nconnected_slaves:%d\r\nmaster_repl_offset:%d\r\n", connected, s.db.JournalOffset())
}

func (s *Server) serveReplica(conn net.Conn, reader *bufio.Reader, tokens []string) {
	if len(tokens) != 3 || len(tokens[1]) == 0 || len(tokens[1]) > 256 {
		_, _ = conn.Write([]byte("-ERR invalid REPLSYNC arguments\r\n"))
		return
	}
	requested, err := strconv.ParseUint(tokens[2], 10, 64)
	if err != nil {
		_, _ = conn.Write([]byte("-ERR invalid replication offset\r\n"))
		return
	}
	id := tokens[1]
	s.db.EnableJournal()
	var stream <-chan engine.MutationRecord
	var cancel func()
	var initial []byte
	var base uint64
	mode := byte('C')
	if requested > 0 {
		stream, cancel, err = s.db.SubscribeFrom(requested)
	}
	if requested == 0 || err != nil {
		mode = 'S'
		for {
			initial, base, err = s.db.SnapshotAtOffset()
			if err != nil {
				return
			}
			stream, cancel, err = s.db.SubscribeFrom(base)
			if err == nil {
				break
			}
			if !errors.Is(err, engine.ErrJournalGap) {
				return
			}
		}
	} else {
		base = requested
	}
	defer cancel()
	if err := writeReplicationFrame(conn, mode, base, initial); err != nil {
		return
	}
	s.mu.Lock()
	s.replicas[id] = replicaStatus{Offset: base, Connected: true}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		status := s.replicas[id]
		status.Connected = false
		s.replicas[id] = status
		s.mu.Unlock()
	}()
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		for {
			var ack [9]byte
			if _, err := io.ReadFull(reader, ack[:]); err != nil || ack[0] != 'A' {
				return
			}
			offset := binary.BigEndian.Uint64(ack[1:])
			latest := s.db.JournalOffset()
			s.mu.Lock()
			status := s.replicas[id]
			if offset >= status.Offset && offset <= latest {
				status.Offset = offset
				status.LastAck = time.Now()
				s.replicas[id] = status
			}
			s.mu.Unlock()
		}
	}()
	for {
		select {
		case <-ackDone:
			return
		case record, ok := <-stream:
			if !ok {
				return
			}
			if err := writeReplicationFrame(conn, 'M', record.Offset, record.Snapshot); err != nil {
				return
			}
		}
	}
}

func writeReplicationFrame(conn net.Conn, kind byte, offset uint64, data []byte) error {
	if len(data) > 1<<30 {
		return errors.New("replication frame too large")
	}
	var header [17]byte
	header[0] = kind
	binary.BigEndian.PutUint64(header[1:9], offset)
	binary.BigEndian.PutUint32(header[9:13], uint32(len(data)))
	binary.BigEndian.PutUint32(header[13:17], crc32.ChecksumIEEE(data))
	if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	if err := writeFull(conn, header[:]); err != nil {
		return err
	}
	return writeFull(conn, data)
}

func readReplicationFrame(reader io.Reader) (byte, uint64, []byte, error) {
	var header [17]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, 0, nil, err
	}
	size := binary.BigEndian.Uint32(header[9:13])
	if size > 1<<30 {
		return 0, 0, nil, errors.New("replication frame too large")
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return 0, 0, nil, err
	}
	if crc32.ChecksumIEEE(data) != binary.BigEndian.Uint32(header[13:17]) {
		return 0, 0, nil, errors.New("replication checksum mismatch")
	}
	return header[0], binary.BigEndian.Uint64(header[1:9]), data, nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func (s *Server) runReplica(ctx context.Context) {
	for ctx.Err() == nil {
		_ = s.syncFromPrimary(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (s *Server) syncFromPrimary(ctx context.Context) error {
	s.mu.Lock()
	address, id, offset := s.replicaOf, s.replicaID, s.replicaOffset
	s.mu.Unlock()
	if id == "" {
		id = s.addr
	}
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.upstream = conn
	s.mu.Unlock()
	defer func() {
		conn.Close()
		s.mu.Lock()
		if s.upstream == conn {
			s.upstream = nil
		}
		s.mu.Unlock()
	}()
	if err := writeFull(conn, proto.FormatRESPCommand([]string{"REPLSYNC", id, strconv.FormatUint(offset, 10)})); err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
	first := true
	for {
		kind, next, data, err := readReplicationFrame(reader)
		if err != nil {
			return err
		}
		if first {
			if kind == 'S' {
				if err := s.db.LoadSnapshot(data); err != nil {
					return err
				}
			} else if kind != 'C' || next != offset {
				return errors.New("invalid replication handshake")
			}
			first = false
		} else {
			if kind != 'M' || next != offset+1 {
				return fmt.Errorf("replication offset gap after %d", offset)
			}
			if err := s.db.LoadSnapshot(data); err != nil {
				return err
			}
		}
		offset = next
		s.mu.Lock()
		s.replicaOffset = offset
		s.mu.Unlock()
		var ack [9]byte
		ack[0] = 'A'
		binary.BigEndian.PutUint64(ack[1:], offset)
		if err := writeFull(conn, ack[:]); err != nil {
			return err
		}
	}
}

// ReplicaOffset reports the last state applied from the primary.
func (s *Server) ReplicaOffset() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replicaOffset
}
