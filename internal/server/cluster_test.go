package server

import (
	"fmt"
	"strings"
	"testing"

	"kestreldb/internal/engine"
	"kestreldb/internal/proto"
)

func TestClusterSlotHashTagsAndKnownVector(t *testing.T) {
	if got := ClusterSlot("123456789"); got != 12739 {
		t.Fatalf("ClusterSlot known vector = %d, want 12739", got)
	}
	if ClusterSlot("{user1000}.following") != ClusterSlot("{user1000}.followers") {
		t.Fatal("keys with the same hash tag mapped to different slots")
	}
	if ClusterSlot("foo{}{bar}") != ClusterSlot("foo{}{bar}") {
		t.Fatal("slot hashing is not deterministic")
	}
}

func TestClusterTopologyValidationAndSnapshot(t *testing.T) {
	s := New("", engine.NewDB())
	valid := testClusterTopology()
	if err := s.SetClusterTopology(valid); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := s.ClusterTopology()
	if !ok || snapshot.LocalID != "local" || len(snapshot.Nodes) != 2 {
		t.Fatalf("topology snapshot = %#v, %v", snapshot, ok)
	}
	snapshot.Nodes[0].Slots[0].End = 0
	again, _ := s.ClusterTopology()
	if again.Nodes[0].Slots[0].End == 0 {
		t.Fatal("topology snapshot aliases installed state")
	}

	overlap := testClusterTopology()
	overlap.Nodes[1].Slots[0].Start = 8000
	if err := s.SetClusterTopology(overlap); err == nil {
		t.Fatal("overlapping slots were accepted")
	}
	missingLocal := testClusterTopology()
	missingLocal.LocalID = "missing"
	if err := s.SetClusterTopology(missingLocal); err == nil {
		t.Fatal("missing local node was accepted")
	}
}

func TestClusterRequestRoutingAndTopologyChange(t *testing.T) {
	s := New("", engine.NewDB())
	if err := s.SetClusterTopology(testClusterTopology()); err != nil {
		t.Fatal(err)
	}
	localKey := keyInSlotRange(t, 0, 8191)
	remoteKey := keyInSlotRange(t, 8192, ClusterSlotCount-1)

	assertStringResponse(t, runCommand(t, s.h, "SET", localKey, "value"), "OK")
	moved := runCommand(t, s.h, "GET", remoteKey)
	wantMoved := fmt.Sprintf("MOVED %d 127.0.0.1:7001", ClusterSlot(remoteKey))
	if moved.Type != proto.RError || moved.Str != wantMoved {
		t.Fatalf("remote response = %#v, want %q", moved, wantMoved)
	}
	crossSlot := runCommand(t, s.h, "MGET", localKey, remoteKey)
	if crossSlot.Type != proto.RError || crossSlot.Str != "CROSSSLOT Keys in request don't hash to the same slot" {
		t.Fatalf("cross-slot response = %#v", crossSlot)
	}

	tag := tagInSlotRange(t, 0, 8191)
	assertStringResponse(t, runCommand(t, s.h, "MSET", "{"+tag+"}:one", "1", "{"+tag+"}:two", "2"), "OK")

	updated := testClusterTopology()
	updated.Nodes[0].Slots = []ClusterSlotRange{{Start: 0, End: ClusterSlotCount - 1}}
	updated.Nodes[1].Slots = nil
	if err := s.SetClusterTopology(updated); err != nil {
		t.Fatal(err)
	}
	assertStringResponse(t, runCommand(t, s.h, "SET", remoteKey, "now-local"), "OK")
}

func TestClusterUnassignedSlotAndIntrospection(t *testing.T) {
	s := New("", engine.NewDB())
	topology := ClusterTopology{
		LocalID: "local",
		Nodes: []ClusterNode{{
			ID: "local", Address: "127.0.0.1:7000",
			Slots: []ClusterSlotRange{{Start: 0, End: 0}},
		}},
	}
	if err := s.SetClusterTopology(topology); err != nil {
		t.Fatal(err)
	}
	key := keyInSlotRange(t, 1, ClusterSlotCount-1)
	down := runCommand(t, s.h, "GET", key)
	if down.Type != proto.RError || down.Str != "CLUSTERDOWN Hash slot not served" {
		t.Fatalf("unassigned response = %#v", down)
	}
	assertBlobResponse(t, runCommand(t, s.h, "CLUSTER", "MYID"), "local")
	assertIntResponse(t, runCommand(t, s.h, "CLUSTER", "KEYSLOT", key), int(ClusterSlot(key)))
	info := runCommand(t, s.h, "CLUSTER", "INFO")
	if info.Type != proto.RBlobString || !strings.Contains(info.Str, "cluster_state:fail") {
		t.Fatalf("CLUSTER INFO = %#v", info)
	}
	slots := runCommand(t, s.h, "CLUSTER", "SLOTS")
	if slots.Type != proto.RArray || len(slots.Elements) != 1 {
		t.Fatalf("CLUSTER SLOTS = %#v", slots)
	}

	s.ClearClusterTopology()
	assertStringResponse(t, runCommand(t, s.h, "SET", key, "standalone"), "OK")
}

func TestCommandMetadataArityFlagsAndKeys(t *testing.T) {
	implemented := strings.Fields(`
		INFO HELLO ECHO SELECT CLIENT CLUSTER PING QUIT
		TYPE DEL EXISTS RENAME RENAMENX COPY TOUCH DBSIZE FLUSHDB KEYS SCAN
		EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL EXPIRETIME PEXPIRETIME PERSIST
		SET SETNX GETSET GETDEL GET MSET MSETNX MGET APPEND STRLEN GETRANGE SETRANGE
		INCR DECR INCRBY DECRBY INCRBYFLOAT
		LPUSH RPUSH LPOP RPOP LLEN LRANGE LINDEX LSET LTRIM LINSERT LREM LMOVE
		RPOPLPUSH BLPOP BRPOP BLMOVE BRPOPLPUSH
		HSET HSETNX HGET HMGET HDEL HLEN HEXISTS HKEYS HVALS HSTRLEN HEXPIRE
		HPEXPIRE HEXPIREAT HPEXPIREAT HTTL HPTTL HEXPIRETIME HPEXPIRETIME HPERSIST
		HINCRBY HINCRBYFLOAT HGETALL HSCAN
		SADD SADDEX SREM SISMEMBER SMISMEMBER SMOVE SCARD SMEMBERS SPOP
		SRANDMEMBER SSCAN SUNION SINTER SDIFF SUNIONSTORE SINTERSTORE SDIFFSTORE
		ZADD ZREM ZSCORE ZMSCORE ZRANK ZREVRANK ZCARD ZCOUNT ZLEXCOUNT ZRANGE
		ZRANGESTORE ZINCRBY ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX
		ZPOPMIN ZPOPMAX ZMPOP ZRANDMEMBER ZSCAN ZUNION ZINTER ZDIFF ZUNIONSTORE
		ZINTERSTORE ZDIFFSTORE ZINTERCARD ZRANGEWITHSCORES
	`)
	for _, name := range implemented {
		if _, ok := commands[name]; !ok {
			t.Fatalf("missing metadata for %s", name)
		}
	}
	if err := validateCommandMetadata("GET", nil); err == nil {
		t.Fatal("GET without a key passed metadata validation")
	}
	if err := validateCommandMetadata("MSET", []string{"key", "value", "dangling"}); err == nil {
		t.Fatal("odd MSET passed metadata validation")
	}
	if err := validateCommandMetadata("HSET", []string{"key", "field"}); err == nil {
		t.Fatal("incomplete HSET passed metadata validation")
	}
	if !commands["GET"].has(commandReadOnly) || commands["SET"].has(commandReadOnly) {
		t.Fatal("read-only command flags are incorrect")
	}
	if !commands["BLPOP"].has(commandBlocking) || !commands["CLUSTER"].has(commandAdmin) {
		t.Fatal("blocking or admin command flags are incorrect")
	}
	keys := commandKeys("MSET", []string{"first", "1", "second", "2"})
	if len(keys) != 2 || keys[0] != "first" || keys[1] != "second" {
		t.Fatalf("MSET keys = %v", keys)
	}
}

func testClusterTopology() ClusterTopology {
	return ClusterTopology{
		LocalID: "local",
		Nodes: []ClusterNode{
			{ID: "local", Address: "127.0.0.1:7000", Slots: []ClusterSlotRange{{Start: 0, End: 8191}}},
			{ID: "remote", Address: "127.0.0.1:7001", Slots: []ClusterSlotRange{{Start: 8192, End: ClusterSlotCount - 1}}},
		},
	}
}

func keyInSlotRange(t *testing.T, start, end int) string {
	t.Helper()
	for index := 0; index < 100000; index++ {
		key := fmt.Sprintf("key-%d", index)
		slot := int(ClusterSlot(key))
		if slot >= start && slot <= end {
			return key
		}
	}
	t.Fatalf("could not find key in slot range %d-%d", start, end)
	return ""
}

func tagInSlotRange(t *testing.T, start, end int) string {
	t.Helper()
	for index := 0; index < 100000; index++ {
		tag := fmt.Sprintf("tag-%d", index)
		slot := int(ClusterSlot("{" + tag + "}"))
		if slot >= start && slot <= end {
			return tag
		}
	}
	t.Fatalf("could not find hash tag in slot range %d-%d", start, end)
	return ""
}
