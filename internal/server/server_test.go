package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"kestreldb/internal/engine"
	"kestreldb/internal/proto"
)

func TestRESPConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	h := &handler{db: engine.NewDB()}
	go h.serve(serverConn)
	r := bufio.NewReader(clientConn)
	write := func(parts ...string) proto.Response {
		t.Helper()
		var command bytes.Buffer
		command.WriteString("*")
		command.WriteString(strconv.Itoa(len(parts)))
		command.WriteString("\r\n")
		for _, part := range parts {
			fmt.Fprintf(&command, "$%d\r\n%s\r\n", len(part), part)
		}
		if _, err := clientConn.Write(command.Bytes()); err != nil {
			t.Fatal(err)
		}
		response, err := proto.ReadResponse(r)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	if got := write("SET", "a\x00b", "line\r\nvalue"); got.Type != proto.RSimpleString || got.Str != "OK" {
		t.Fatalf("SET = %#v", got)
	}
	if got := write("GET", "a\x00b"); got.Type != proto.RBlobString || got.Str != "line\r\nvalue" {
		t.Fatalf("GET = %#v", got)
	}
	if got := write("HELLO", "3"); got.Type != proto.RMap {
		t.Fatalf("HELLO 3 = %#v", got)
	}
}

func TestServerCloseStopsBlockedConnections(t *testing.T) {
	db := engine.NewDB()
	server := New("", db)
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	server.mu.Lock()
	server.connections[serverConn] = struct{}{}
	server.mu.Unlock()
	done := make(chan struct{})
	go func() {
		server.h.serve(serverConn)
		close(done)
	}()

	if _, err := clientConn.Write([]byte(proto.FormatCommand([]string{"BLPOP", "list", "0"}))); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked connection did not stop during server shutdown")
	}

	if _, _, _, err := db.WaitForListPop(context.Background(), []string{"other"}, engine.ListLeft); err == nil {
		t.Fatal("blocking operation started after server shutdown")
	}
}

func TestPrimaryReplicaSynchronizationAndReconnect(t *testing.T) {
	reserve, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	primaryAddr := reserve.Addr().String()
	reserve.Close()
	primaryDB := engine.NewDB()
	primary := New(primaryAddr, primaryDB)
	primaryDone := make(chan error, 1)
	go func() { primaryDone <- primary.ListenAndServe() }()
	defer func() { primary.Close(); <-primaryDone }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", primaryAddr, 50*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("primary did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	primaryDB.Set("first", []byte("one"))
	replicaDB := engine.NewDB()
	replica := New("", replicaDB)
	replica.SetReplicaOf(primaryAddr, "test-replica")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go replica.runReplica(ctx)
	defer replica.Close()
	waitForValue := func(key, want string) {
		t.Helper()
		for time.Now().Before(deadline) {
			value, found, err := replicaDB.Get(key)
			if err == nil && found && string(value) == want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("replica never received %q=%q", key, want)
	}
	waitForValue("first", "one")
	primaryDB.Set("second", []byte("two"))
	waitForValue("second", "two")
	for replica.ReplicaOffset() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if replica.ReplicaOffset() < 1 {
		t.Fatalf("replica offset = %d", replica.ReplicaOffset())
	}
	for primary.ReplicaStatus()["test-replica"].Offset < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if status := primary.ReplicaStatus()["test-replica"]; !status.Connected || status.Offset < 1 {
		t.Fatalf("primary replica status = %#v", status)
	}
	// A disconnected replica catches up from its acknowledged offset.
	replica.mu.Lock()
	if replica.upstream != nil {
		replica.upstream.Close()
	}
	replica.mu.Unlock()
	primaryDB.Set("third", []byte("three"))
	deadline = time.Now().Add(4 * time.Second)
	waitForValue("third", "three")
}
