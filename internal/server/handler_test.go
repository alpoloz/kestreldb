package server

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"kestreldb/internal/engine"
	"kestreldb/internal/proto"
)

func TestGenericCommandsAndWrongTypeResponse(t *testing.T) {
	h := &handler{db: engine.NewDB()}

	assertIntResponse(t, runCommand(t, h, "HSET", "key", "field", "value"), 1)
	assertStringResponse(t, runCommand(t, h, "TYPE", "key"), "hash")
	assertIntResponse(t, runCommand(t, h, "EXISTS", "key", "missing", "key"), 2)

	wrongType := runCommand(t, h, "ZADD", "key", "1", "member")
	if wrongType.Type != proto.RError || wrongType.Str != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("ZADD wrong-type response = %#v", wrongType)
	}

	assertIntResponse(t, runCommand(t, h, "DEL", "key"), 1)
	assertStringResponse(t, runCommand(t, h, "TYPE", "key"), "none")
	assertIntResponse(t, runCommand(t, h, "ZADD", "key", "1", "member"), 1)
}

func TestExtendedHashCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}

	assertIntResponse(t, runCommand(t, h, "HSET", "hash", "alpha", "one", "beta", "two", "alpha", "updated"), 2)
	assertBlobResponse(t, runCommand(t, h, "HGET", "hash", "alpha"), "updated")
	hmget := runCommand(t, h, "HMGET", "hash", "alpha", "missing", "beta")
	if hmget.Type != proto.RArray || len(hmget.Elements) != 3 ||
		hmget.Elements[0].Type != proto.RBlobString || hmget.Elements[0].Str != "updated" ||
		hmget.Elements[1].Type != proto.RNil ||
		hmget.Elements[2].Type != proto.RBlobString || hmget.Elements[2].Str != "two" {
		t.Fatalf("HMGET response = %#v", hmget)
	}
	assertIntResponse(t, runCommand(t, h, "HEXISTS", "hash", "alpha"), 1)
	assertIntResponse(t, runCommand(t, h, "HSTRLEN", "hash", "alpha"), 7)
	assertBlobValues(t, runCommand(t, h, "HKEYS", "hash"), "alpha", "beta")
	assertBlobValues(t, runCommand(t, h, "HVALS", "hash"), "updated", "two")
	assertIntResponse(t, runCommand(t, h, "HSETNX", "hash", "alpha", "ignored"), 0)
	assertIntResponse(t, runCommand(t, h, "HSETNX", "hash", "gamma", "three"), 1)
	assertIntResponse(t, runCommand(t, h, "HINCRBY", "numbers", "integer", "5"), 5)
	assertBlobResponse(t, runCommand(t, h, "HINCRBYFLOAT", "numbers", "float", "1.5"), "1.5")
	assertIntResponse(t, runCommand(t, h, "HDEL", "hash", "alpha", "missing", "beta"), 2)

	assertHashScanResponse(t, runCommand(t, h, "HSCAN", "hash", "0", "COUNT", "1"), "0", "gamma", "three")

	malformed := runCommand(t, h, "HSET", "hash", "field")
	if malformed.Type != proto.RError {
		t.Fatalf("malformed HSET response = %#v", malformed)
	}
}

func TestExpirationCommands(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	db := engine.NewDBWithClock(func() time.Time { return now })
	h := &handler{db: db}

	assertStringResponse(t, runCommand(t, h, "SET", "key", "value", "PX", "1500"), "OK")
	assertIntResponse(t, runCommand(t, h, "PTTL", "key"), 1500)
	assertStringResponse(t, runCommand(t, h, "SET", "key", "updated", "KEEPTTL"), "OK")
	assertIntResponse(t, runCommand(t, h, "TTL", "key"), 1)
	assertIntResponse(t, runCommand(t, h, "PEXPIRE", "key", "2000", "GT"), 1)
	assertIntResponse(t, runCommand(t, h, "PEXPIRE", "key", "1000", "GT"), 0)
	assertIntResponse(t, runCommand(t, h, "PERSIST", "key"), 1)
	assertIntResponse(t, runCommand(t, h, "PTTL", "key"), -1)
	assertStringResponse(t, runCommand(t, h, "SET", "absolute", "value"), "OK")
	assertIntResponse(t, runCommand(t, h, "EXPIREAT", "absolute", "1700000005"), 1)
	assertIntResponse(t, runCommand(t, h, "EXPIRETIME", "absolute"), 1700000005)
	assertIntResponse(t, runCommand(t, h, "PEXPIREAT", "absolute", "1700000006000", "XX"), 1)
	assertInt64Response(t, runCommand(t, h, "PEXPIRETIME", "absolute"), 1700000006000)

	assertIntResponse(t, runCommand(t, h, "HSET", "hash", "first", "one", "second", "two"), 2)
	assertIntArray(t, runCommand(t, h, "HPEXPIRE", "hash", "1000", "FIELDS", "2", "first", "missing"), 1, -2)
	assertIntArray(t, runCommand(t, h, "HPTTL", "hash", "FIELDS", "2", "first", "second"), 1000, -1)
	assertIntArray(t, runCommand(t, h, "HPERSIST", "hash", "FIELDS", "2", "first", "missing"), 1, -2)
	assertIntArray(t, runCommand(t, h, "HEXPIREAT", "hash", "1700000005", "FIELDS", "1", "second"), 1)
	assertIntArray(t, runCommand(t, h, "HEXPIRETIME", "hash", "FIELDS", "1", "second"), 1700000005)
	assertIntArray(t, runCommand(t, h, "HPEXPIREAT", "hash", "1700000006000", "XX", "FIELDS", "1", "second"), 1)
	assertIntArray(t, runCommand(t, h, "HPEXPIRETIME", "hash", "FIELDS", "1", "second"), 1700000006000)
	assertIntResponse(t, runCommand(t, h, "SADDEX", "set", "1", "temporary", "other"), 2)

	now = now.Add(2 * time.Second)
	assertBlobResponse(t, runCommand(t, h, "GET", "key"), "updated")
	assertBlobResponse(t, runCommand(t, h, "HGET", "hash", "first"), "one")
	assertIntResponse(t, runCommand(t, h, "SCARD", "set"), 0)

	invalid := runCommand(t, h, "SET", "bad", "value", "EX", "0")
	if invalid.Type != proto.RError || invalid.Str != "ERR invalid expire time in SET" {
		t.Fatalf("invalid SET expiration response = %#v", invalid)
	}
}

func TestStringCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}

	assertIntResponse(t, runCommand(t, h, "HSET", "key", "field", "value"), 1)
	assertStringResponse(t, runCommand(t, h, "SET", "key", "hello\nworld"), "OK")
	assertStringResponse(t, runCommand(t, h, "TYPE", "key"), "string")
	assertBlobResponse(t, runCommand(t, h, "GET", "key"), "hello\nworld")

	wrongType := runCommand(t, h, "HGET", "key", "field")
	if wrongType.Type != proto.RError || wrongType.Str != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("HGET wrong-type response = %#v", wrongType)
	}

	assertStringResponse(t, runCommand(t, h, "MSET", "first", "one", "second", "two"), "OK")
	response := runCommand(t, h, "MGET", "first", "missing", "key", "second")
	if response.Type != proto.RArray || len(response.Elements) != 4 {
		t.Fatalf("MGET response = %#v", response)
	}
	assertBlobResponse(t, response.Elements[0], "one")
	if response.Elements[1].Type != proto.RNil {
		t.Fatalf("missing MGET element = %#v, want nil", response.Elements[1])
	}
	assertBlobResponse(t, response.Elements[2], "hello\nworld")
	assertBlobResponse(t, response.Elements[3], "two")
}

func TestStringMutationCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}

	assertIntResponse(t, runCommand(t, h, "APPEND", "text", "hello"), 5)
	assertIntResponse(t, runCommand(t, h, "APPEND", "text", " world"), 11)
	assertIntResponse(t, runCommand(t, h, "STRLEN", "text"), 11)
	assertBlobResponse(t, runCommand(t, h, "GET", "text"), "hello world")

	assertIntResponse(t, runCommand(t, h, "INCR", "counter"), 1)
	assertIntResponse(t, runCommand(t, h, "INCRBY", "counter", "24"), 25)
	assertIntResponse(t, runCommand(t, h, "DECR", "counter"), 24)
	assertIntResponse(t, runCommand(t, h, "DECRBY", "counter", "4"), 20)

	assertStringResponse(t, runCommand(t, h, "SET", "counter", "invalid"), "OK")
	response := runCommand(t, h, "INCR", "counter")
	if response.Type != proto.RError || response.Str != "ERR value is not an integer or out of range" {
		t.Fatalf("INCR invalid response = %#v", response)
	}
	assertBlobResponse(t, runCommand(t, h, "GET", "counter"), "invalid")
}

func TestExtendedStringCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}
	assertIntResponse(t, runCommand(t, h, "SETNX", "key", "first"), 1)
	assertIntResponse(t, runCommand(t, h, "SETNX", "key", "second"), 0)
	assertBlobResponse(t, runCommand(t, h, "SET", "key", "updated", "XX", "GET"), "first")
	assertBlobResponse(t, runCommand(t, h, "GETSET", "key", "replacement"), "updated")
	assertBlobResponse(t, runCommand(t, h, "GETDEL", "key"), "replacement")
	assertIntResponse(t, runCommand(t, h, "MSETNX", "a", "one", "b", "two"), 1)
	assertIntResponse(t, runCommand(t, h, "MSETNX", "b", "changed", "c", "three"), 0)

	assertIntResponse(t, runCommand(t, h, "SETRANGE", "range", "2", "ab"), 4)
	assertBlobResponse(t, runCommand(t, h, "GETRANGE", "range", "-2", "-1"), "ab")
	assertBlobResponse(t, runCommand(t, h, "INCRBYFLOAT", "float", "1.25"), "1.25")
}

func TestListCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}

	assertIntResponse(t, runCommand(t, h, "LPUSH", "list", "a", "b"), 2)
	assertIntResponse(t, runCommand(t, h, "RPUSH", "list", "c", "d"), 4)
	assertStringResponse(t, runCommand(t, h, "TYPE", "list"), "list")
	assertIntResponse(t, runCommand(t, h, "LLEN", "list"), 4)
	assertBlobValues(t, runCommand(t, h, "LRANGE", "list", "0", "-1"), "b", "a", "c", "d")
	assertBlobResponse(t, runCommand(t, h, "LINDEX", "list", "-2"), "c")

	assertStringResponse(t, runCommand(t, h, "LSET", "list", "-1", "last"), "OK")
	assertIntResponse(t, runCommand(t, h, "LINSERT", "list", "BEFORE", "c", "middle"), 5)
	assertBlobValues(t, runCommand(t, h, "LRANGE", "list", "0", "-1"), "b", "a", "middle", "c", "last")
	assertIntResponse(t, runCommand(t, h, "LREM", "list", "1", "a"), 1)
	assertStringResponse(t, runCommand(t, h, "LTRIM", "list", "1", "-2"), "OK")
	assertBlobValues(t, runCommand(t, h, "LRANGE", "list", "0", "-1"), "middle", "c")

	if response := runCommand(t, h, "LINDEX", "list", "10"); response.Type != proto.RNil {
		t.Fatalf("LINDEX out-of-range response = %#v, want nil", response)
	}
	indexError := runCommand(t, h, "LSET", "list", "10", "value")
	if indexError.Type != proto.RError || indexError.Str != "ERR index out of range" {
		t.Fatalf("LSET out-of-range response = %#v", indexError)
	}
}

func TestListPopCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}
	assertIntResponse(t, runCommand(t, h, "RPUSH", "list", "a", "b", "c", "d"), 4)

	assertBlobResponse(t, runCommand(t, h, "LPOP", "list"), "a")
	assertBlobValues(t, runCommand(t, h, "RPOP", "list", "2"), "d", "c")
	assertBlobValues(t, runCommand(t, h, "LPOP", "list", "0"))
	assertBlobResponse(t, runCommand(t, h, "RPOP", "list"), "b")
	assertStringResponse(t, runCommand(t, h, "TYPE", "list"), "none")

	if response := runCommand(t, h, "LPOP", "missing"); response.Type != proto.RNil {
		t.Fatalf("LPOP missing response = %#v, want nil", response)
	}
	assertBlobValues(t, runCommand(t, h, "LPOP", "missing", "2"))
	invalid := runCommand(t, h, "RPOP", "missing", "-1")
	if invalid.Type != proto.RError || invalid.Str != "ERR value is not an integer or out of range" {
		t.Fatalf("RPOP invalid count response = %#v", invalid)
	}
}

func TestListMoveCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}
	assertIntResponse(t, runCommand(t, h, "RPUSH", "source", "a", "b", "c"), 3)
	assertIntResponse(t, runCommand(t, h, "RPUSH", "destination", "x", "y"), 2)

	assertBlobResponse(t, runCommand(t, h, "LMOVE", "source", "destination", "RIGHT", "LEFT"), "c")
	assertBlobValues(t, runCommand(t, h, "LRANGE", "source", "0", "-1"), "a", "b")
	assertBlobValues(t, runCommand(t, h, "LRANGE", "destination", "0", "-1"), "c", "x", "y")
	assertBlobResponse(t, runCommand(t, h, "RPOPLPUSH", "source", "destination"), "b")
	assertBlobValues(t, runCommand(t, h, "LRANGE", "destination", "0", "-1"), "b", "c", "x", "y")

	if response := runCommand(t, h, "LMOVE", "missing", "destination", "LEFT", "RIGHT"); response.Type != proto.RNil {
		t.Fatalf("LMOVE missing response = %#v, want nil", response)
	}
	invalid := runCommand(t, h, "LMOVE", "source", "destination", "MIDDLE", "LEFT")
	if invalid.Type != proto.RError || invalid.Str != "ERR syntax error" {
		t.Fatalf("LMOVE invalid direction response = %#v", invalid)
	}
}

func TestBlockingListCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}
	assertIntResponse(t, runCommand(t, h, "RPUSH", "first", "a", "b"), 2)
	assertIntResponse(t, runCommand(t, h, "RPUSH", "second", "x", "y"), 2)

	assertBlobValues(t, runCommand(t, h, "BLPOP", "missing", "first", "second", "1"), "first", "a")
	assertBlobValues(t, runCommand(t, h, "BRPOP", "missing", "second", "1"), "second", "y")
	assertBlobResponse(t, runCommand(t, h, "BLMOVE", "first", "destination", "RIGHT", "LEFT", "1"), "b")
	assertBlobValues(t, runCommand(t, h, "LRANGE", "destination", "0", "-1"), "b")
	assertBlobResponse(t, runCommand(t, h, "BRPOPLPUSH", "second", "destination", "1"), "x")
	assertBlobValues(t, runCommand(t, h, "LRANGE", "destination", "0", "-1"), "x", "b")

	if response := runCommand(t, h, "BLPOP", "empty", "0.001"); response.Type != proto.RNil {
		t.Fatalf("BLPOP timeout response = %#v, want nil", response)
	}
	invalid := runCommand(t, h, "BRPOP", "empty", "invalid")
	if invalid.Type != proto.RError || invalid.Str != "ERR timeout is not a valid number" {
		t.Fatalf("BRPOP invalid timeout response = %#v", invalid)
	}
}

func TestBlockingCommandContextCancellation(t *testing.T) {
	db := engine.NewDB()
	h := &handler{db: db}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		var output bytes.Buffer
		w := proto.NewWriter(bufio.NewWriter(&output))
		_, err := h.dispatchContext(ctx, w, []string{"BLPOP", "list", "0"})
		done <- err
	}()
	cancel()

	if err := <-done; err != context.Canceled {
		t.Fatalf("dispatch error = %v, want context.Canceled", err)
	}
	if _, err := db.RPush("list", "retained"); err != nil {
		t.Fatal(err)
	}
	values, err := db.LRange("list", 0, -1)
	if err != nil || len(values) != 1 || values[0] != "retained" {
		t.Fatalf("list after cancellation = (%v, %v)", values, err)
	}
}

func TestBlockedConnectionDisconnectsCleanly(t *testing.T) {
	db := engine.NewDB()
	h := &handler{db: db}
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		h.serve(serverConn)
		close(done)
	}()

	if _, err := clientConn.Write([]byte(proto.FormatCommand([]string{"BLPOP", "list", "0"}))); err != nil {
		t.Fatal(err)
	}
	if err := clientConn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not stop after blocked connection disconnected")
	}

	if _, err := db.RPush("list", "retained"); err != nil {
		t.Fatal(err)
	}
	values, err := db.LRange("list", 0, -1)
	if err != nil || len(values) != 1 || values[0] != "retained" {
		t.Fatalf("list after disconnect = (%v, %v)", values, err)
	}
}

func TestSetCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}

	assertIntResponse(t, runCommand(t, h, "SADD", "colors", "red", "green", "red"), 2)
	assertStringResponse(t, runCommand(t, h, "TYPE", "colors"), "set")
	assertIntResponse(t, runCommand(t, h, "SCARD", "colors"), 2)
	assertIntResponse(t, runCommand(t, h, "SISMEMBER", "colors", "red"), 1)
	assertIntResponse(t, runCommand(t, h, "SISMEMBER", "colors", "blue"), 0)

	assertBlobMembers(t, runCommand(t, h, "SMEMBERS", "colors"), "red", "green")

	assertIntResponse(t, runCommand(t, h, "SREM", "colors", "red", "missing"), 1)
	assertIntResponse(t, runCommand(t, h, "SREM", "colors", "green"), 1)
	assertStringResponse(t, runCommand(t, h, "TYPE", "colors"), "none")

	assertStringResponse(t, runCommand(t, h, "SET", "string", "value"), "OK")
	wrongType := runCommand(t, h, "SADD", "string", "member")
	if wrongType.Type != proto.RError || wrongType.Str != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("SADD wrong-type response = %#v", wrongType)
	}
}

func TestSetAlgebraCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}

	assertIntResponse(t, runCommand(t, h, "SADD", "first", "a", "b", "c"), 3)
	assertIntResponse(t, runCommand(t, h, "SADD", "second", "b", "c", "d"), 3)
	assertIntResponse(t, runCommand(t, h, "SADD", "third", "c"), 1)

	assertBlobMembers(t, runCommand(t, h, "SUNION", "first", "second", "missing"), "a", "b", "c", "d")
	assertBlobMembers(t, runCommand(t, h, "SINTER", "first", "second", "third"), "c")
	assertBlobMembers(t, runCommand(t, h, "SDIFF", "first", "second"), "a")
	assertBlobMembers(t, runCommand(t, h, "SINTER", "first", "missing"))

	assertStringResponse(t, runCommand(t, h, "SET", "string", "value"), "OK")
	wrongType := runCommand(t, h, "SUNION", "missing", "string")
	if wrongType.Type != proto.RError || wrongType.Str != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("SUNION wrong-type response = %#v", wrongType)
	}
}

func TestStoredSetAlgebraCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}

	assertIntResponse(t, runCommand(t, h, "SADD", "first", "a", "b", "c"), 3)
	assertIntResponse(t, runCommand(t, h, "SADD", "second", "b", "c", "d"), 3)
	assertIntResponse(t, runCommand(t, h, "SADD", "third", "c"), 1)

	assertStringResponse(t, runCommand(t, h, "SET", "union", "replace me"), "OK")
	assertIntResponse(t, runCommand(t, h, "SUNIONSTORE", "union", "first", "second"), 4)
	assertBlobMembers(t, runCommand(t, h, "SMEMBERS", "union"), "a", "b", "c", "d")

	assertIntResponse(t, runCommand(t, h, "SINTERSTORE", "intersection", "first", "second", "third"), 1)
	assertBlobMembers(t, runCommand(t, h, "SMEMBERS", "intersection"), "c")

	assertIntResponse(t, runCommand(t, h, "SDIFFSTORE", "difference", "first", "second"), 1)
	assertBlobMembers(t, runCommand(t, h, "SMEMBERS", "difference"), "a")

	assertIntResponse(t, runCommand(t, h, "SINTERSTORE", "union", "first", "missing"), 0)
	assertStringResponse(t, runCommand(t, h, "TYPE", "union"), "none")
}

func TestMultiMembershipAndMoveCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}

	assertIntResponse(t, runCommand(t, h, "SADD", "source", "a", "b"), 2)
	assertIntResponse(t, runCommand(t, h, "SADD", "destination", "b"), 1)
	assertIntArray(t, runCommand(t, h, "SMISMEMBER", "source", "a", "missing", "b"), 1, 0, 1)

	assertIntResponse(t, runCommand(t, h, "SMOVE", "source", "destination", "b"), 1)
	assertBlobMembers(t, runCommand(t, h, "SMEMBERS", "source"), "a")
	assertBlobMembers(t, runCommand(t, h, "SMEMBERS", "destination"), "b")
	assertIntResponse(t, runCommand(t, h, "SMOVE", "source", "new", "missing"), 0)

	assertStringResponse(t, runCommand(t, h, "SET", "string", "value"), "OK")
	wrongType := runCommand(t, h, "SMOVE", "missing", "string", "member")
	if wrongType.Type != proto.RError || wrongType.Str != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("SMOVE wrong-type response = %#v", wrongType)
	}
}

func TestRandomSetCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}

	assertIntResponse(t, runCommand(t, h, "SADD", "set", "a", "b", "c"), 3)
	random := runCommand(t, h, "SRANDMEMBER", "set")
	if random.Type != proto.RBlobString || !isOneOf(random.Str, "a", "b", "c") {
		t.Fatalf("SRANDMEMBER response = %#v", random)
	}
	assertBlobMembers(t, runCommand(t, h, "SRANDMEMBER", "set", "10"), "a", "b", "c")

	repeated := runCommand(t, h, "SRANDMEMBER", "set", "-5")
	if repeated.Type != proto.RArray || len(repeated.Elements) != 5 {
		t.Fatalf("SRANDMEMBER -5 response = %#v", repeated)
	}
	for _, element := range repeated.Elements {
		if element.Type != proto.RBlobString || !isOneOf(element.Str, "a", "b", "c") {
			t.Fatalf("SRANDMEMBER -5 element = %#v", element)
		}
	}
	assertIntResponse(t, runCommand(t, h, "SCARD", "set"), 3)

	popped := runCommand(t, h, "SPOP", "set")
	if popped.Type != proto.RBlobString || !isOneOf(popped.Str, "a", "b", "c") {
		t.Fatalf("SPOP response = %#v", popped)
	}
	remaining := runCommand(t, h, "SPOP", "set", "10")
	if remaining.Type != proto.RArray || len(remaining.Elements) != 2 {
		t.Fatalf("SPOP count response = %#v", remaining)
	}
	assertStringResponse(t, runCommand(t, h, "TYPE", "set"), "none")

	if response := runCommand(t, h, "SPOP", "missing"); response.Type != proto.RNil {
		t.Fatalf("SPOP missing response = %#v, want nil", response)
	}
	assertBlobMembers(t, runCommand(t, h, "SPOP", "missing", "2"))
	invalid := runCommand(t, h, "SPOP", "missing", "-1")
	if invalid.Type != proto.RError || invalid.Str != "ERR value is not an integer or out of range" {
		t.Fatalf("SPOP invalid count response = %#v", invalid)
	}
}

func TestSetScanCommand(t *testing.T) {
	h := &handler{db: engine.NewDB()}
	assertIntResponse(t, runCommand(t, h, "SADD", "set", "alpha", "alpine", "beta", "bravo", "charlie"), 5)

	assertScanResponse(t, runCommand(t, h, "SSCAN", "set", "0", "COUNT", "2"), "2", "alpha", "alpine")
	assertScanResponse(t, runCommand(t, h, "SSCAN", "set", "2", "MATCH", "a*", "COUNT", "2"), "4")
	assertScanResponse(t, runCommand(t, h, "SSCAN", "set", "4", "COUNT", "2"), "0", "charlie")
	assertScanResponse(t, runCommand(t, h, "SSCAN", "missing", "42"), "0")

	invalid := runCommand(t, h, "SSCAN", "set", "not-a-cursor")
	if invalid.Type != proto.RError || invalid.Str != "ERR value is not an integer or out of range" {
		t.Fatalf("SSCAN invalid cursor response = %#v", invalid)
	}
	syntax := runCommand(t, h, "SSCAN", "set", "0", "UNKNOWN", "value")
	if syntax.Type != proto.RError || syntax.Str != "ERR syntax error" {
		t.Fatalf("SSCAN syntax response = %#v", syntax)
	}
}

func TestExtendedSortedSetCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}
	assertIntResponse(t, runCommand(t, h, "ZADD", "zset", "1", "alpha", "2", "bravo", "3", "charlie"), 3)
	assertIntResponse(t, runCommand(t, h, "ZADD", "zset", "XX", "GT", "CH", "4", "alpha"), 1)
	assertFloatResponse(t, runCommand(t, h, "ZADD", "zset", "INCR", "2", "bravo"), 4)
	assertFloatResponse(t, runCommand(t, h, "ZSCORE", "zset", "alpha"), 4)
	assertIntResponse(t, runCommand(t, h, "ZREVRANK", "zset", "charlie"), 2)
	assertIntResponse(t, runCommand(t, h, "ZCOUNT", "zset", "(3", "+inf"), 2)
	assertZSetResponse(t, runCommand(t, h, "ZRANGE", "zset", "+inf", "3", "BYSCORE", "REV", "WITHSCORES"),
		"bravo", 4, "alpha", 4, "charlie", 3)
	assertBlobValues(t, runCommand(t, h, "ZRANGE", "zset", "0", "-1"), "charlie", "alpha", "bravo")
	assertIntResponse(t, runCommand(t, h, "ZRANGESTORE", "copy", "zset", "0", "1"), 2)
	assertIntResponse(t, runCommand(t, h, "ZREMRANGEBYRANK", "copy", "0", "0"), 1)
	assertZSetResponse(t, runCommand(t, h, "ZPOPMAX", "copy"), "alpha", 4)
	assertStringResponse(t, runCommand(t, h, "TYPE", "copy"), "none")

	assertIntResponse(t, runCommand(t, h, "ZADD", "other", "10", "alpha", "5", "delta"), 2)
	assertZSetResponse(t, runCommand(t, h, "ZINTER", "2", "zset", "other", "WEIGHTS", "2", "3", "WITHSCORES"), "alpha", 38)
	assertIntResponse(t, runCommand(t, h, "ZINTERCARD", "2", "zset", "other", "LIMIT", "1"), 1)
	assertIntResponse(t, runCommand(t, h, "ZUNIONSTORE", "union", "2", "zset", "other"), 4)
	assertZScanResponse(t, runCommand(t, h, "ZSCAN", "other", "0", "MATCH", "a*", "COUNT", "10"), "0", "alpha", 10)
}

func runCommand(t *testing.T, h *handler, tokens ...string) proto.Response {
	t.Helper()

	var output bytes.Buffer
	w := proto.NewWriter(bufio.NewWriter(&output))
	_, err := h.dispatch(w, tokens)
	if err != nil {
		if writeErr := writeDispatchError(w, err); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	response, err := proto.ReadResponse(bufio.NewReader(&output))
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func assertIntResponse(t *testing.T, response proto.Response, want int) {
	t.Helper()
	if response.Type != proto.RInteger || response.Int != int64(want) {
		t.Fatalf("response = %#v, want integer %d", response, want)
	}
}

func assertInt64Response(t *testing.T, response proto.Response, want int64) {
	t.Helper()
	if response.Type != proto.RInteger || response.Int != want {
		t.Fatalf("response = %#v, want integer %d", response, want)
	}
}

func assertStringResponse(t *testing.T, response proto.Response, want string) {
	t.Helper()
	if response.Type != proto.RSimpleString || response.Str != want {
		t.Fatalf("response = %#v, want string %q", response, want)
	}
}

func assertBlobResponse(t *testing.T, response proto.Response, want string) {
	t.Helper()
	if response.Type != proto.RBlobString || response.Str != want {
		t.Fatalf("response = %#v, want blob string %q", response, want)
	}
}

func assertFloatResponse(t *testing.T, response proto.Response, want float64) {
	t.Helper()
	if response.Type != proto.RFloat || response.Float != want {
		t.Fatalf("response = %#v, want float %g", response, want)
	}
}

func assertZSetResponse(t *testing.T, response proto.Response, values ...any) {
	t.Helper()
	if response.Type != proto.RArray || len(response.Elements) != len(values) {
		t.Fatalf("response = %#v, want %d sorted-set elements", response, len(values))
	}
	for index, value := range values {
		switch want := value.(type) {
		case string:
			if response.Elements[index].Type != proto.RBlobString || response.Elements[index].Str != want {
				t.Fatalf("element %d = %#v, want blob %q", index, response.Elements[index], want)
			}
		case float64:
			if response.Elements[index].Type != proto.RFloat || response.Elements[index].Float != want {
				t.Fatalf("element %d = %#v, want float %g", index, response.Elements[index], want)
			}
		case int:
			if response.Elements[index].Type != proto.RFloat || response.Elements[index].Float != float64(want) {
				t.Fatalf("element %d = %#v, want float %d", index, response.Elements[index], want)
			}
		default:
			t.Fatalf("unsupported expected sorted-set value %T", value)
		}
	}
}

func assertBlobMembers(t *testing.T, response proto.Response, want ...string) {
	t.Helper()
	if response.Type != proto.RArray || len(response.Elements) != len(want) {
		t.Fatalf("response = %#v, want %d-member array", response, len(want))
	}

	members := make(map[string]struct{}, len(response.Elements))
	for _, element := range response.Elements {
		if element.Type != proto.RBlobString {
			t.Fatalf("array element = %#v, want blob string", element)
		}
		members[element.Str] = struct{}{}
	}
	if len(members) != len(want) {
		t.Fatalf("response = %#v, contains duplicate members", response)
	}
	for _, member := range want {
		if _, ok := members[member]; !ok {
			t.Fatalf("response = %#v, missing %q", response, member)
		}
	}
}

func assertBlobValues(t *testing.T, response proto.Response, want ...string) {
	t.Helper()
	if response.Type != proto.RArray || len(response.Elements) != len(want) {
		t.Fatalf("response = %#v, want %d-element blob array", response, len(want))
	}
	for i, element := range response.Elements {
		if element.Type != proto.RBlobString || element.Str != want[i] {
			t.Fatalf("response element %d = %#v, want blob %q", i, element, want[i])
		}
	}
}

func assertIntArray(t *testing.T, response proto.Response, want ...int64) {
	t.Helper()
	if response.Type != proto.RArray || len(response.Elements) != len(want) {
		t.Fatalf("response = %#v, want %d-element integer array", response, len(want))
	}
	for i, element := range response.Elements {
		if element.Type != proto.RInteger || element.Int != want[i] {
			t.Fatalf("response element %d = %#v, want integer %d", i, element, want[i])
		}
	}
}

func assertScanResponse(t *testing.T, response proto.Response, cursor string, members ...string) {
	t.Helper()
	if response.Type != proto.RArray || len(response.Elements) != 2 {
		t.Fatalf("scan response = %#v, want two-element array", response)
	}
	if response.Elements[0].Type != proto.RBlobString || response.Elements[0].Str != cursor {
		t.Fatalf("scan cursor = %#v, want %q", response.Elements[0], cursor)
	}
	assertBlobMembers(t, response.Elements[1], members...)
}

func assertHashScanResponse(t *testing.T, response proto.Response, cursor string, pairs ...string) {
	t.Helper()
	if response.Type != proto.RArray || len(response.Elements) != 2 {
		t.Fatalf("hash scan response = %#v, want two-element array", response)
	}
	if response.Elements[0].Type != proto.RBlobString || response.Elements[0].Str != cursor {
		t.Fatalf("hash scan cursor = %#v, want %q", response.Elements[0], cursor)
	}
	assertBlobValues(t, response.Elements[1], pairs...)
}

func assertZScanResponse(t *testing.T, response proto.Response, cursor, member string, score float64) {
	t.Helper()
	if response.Type != proto.RArray || len(response.Elements) != 2 {
		t.Fatalf("sorted-set scan response = %#v", response)
	}
	if response.Elements[0].Type != proto.RBlobString || response.Elements[0].Str != cursor {
		t.Fatalf("sorted-set scan cursor = %#v, want %q", response.Elements[0], cursor)
	}
	assertZSetResponse(t, response.Elements[1], member, score)
}

func isOneOf(value string, choices ...string) bool {
	for _, choice := range choices {
		if value == choice {
			return true
		}
	}
	return false
}
