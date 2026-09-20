package engine

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestKeyExpirationAndConditions(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	db := NewDBWithClock(func() time.Time { return now })
	db.Set("key", []byte("value"))

	changed, err := db.ExpireAt("key", now.Add(1500*time.Millisecond), ExpireOptions{NX: true})
	if err != nil || !changed {
		t.Fatalf("ExpireAt NX = (%t, %v)", changed, err)
	}
	if ttl := db.TTL("key", true); ttl != 1500 {
		t.Fatalf("PTTL = %d, want 1500", ttl)
	}
	if changed, err = db.ExpireAt("key", now.Add(time.Second), ExpireOptions{GT: true}); err != nil || changed {
		t.Fatalf("ExpireAt GT shorter = (%t, %v)", changed, err)
	}
	if changed, err = db.ExpireAt("key", now.Add(time.Second), ExpireOptions{LT: true}); err != nil || !changed {
		t.Fatalf("ExpireAt LT shorter = (%t, %v)", changed, err)
	}
	if absolute := db.ExpireTime("key", true); absolute != now.Add(time.Second).UnixMilli() {
		t.Fatalf("PEXPIRETIME = %d", absolute)
	}

	now = now.Add(time.Second)
	if kind := db.Type("key"); kind != KindNone {
		t.Fatalf("expired key type = %s", kind)
	}
	if ttl := db.TTL("key", true); ttl != -2 {
		t.Fatalf("expired key TTL = %d", ttl)
	}
	if _, err := db.HSet("key", "field", "value"); err != nil {
		t.Fatalf("reusing expired key as hash: %v", err)
	}
	if _, err := db.ExpireAt("key", now.Add(time.Second), ExpireOptions{NX: true, XX: true}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("conflicting expiration options error = %v", err)
	}
}

func TestSetExpirationOptions(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	db := NewDBWithClock(func() time.Time { return now })
	result, err := db.SetWithOptions("key", []byte("first"), SetOptions{HasExpiration: true, ExpireAt: now.Add(time.Second).UnixMilli()})
	if err != nil || !result.Stored || db.TTL("key", true) != 1000 {
		t.Fatalf("expiring SET = (%#v, %v), ttl %d", result, err, db.TTL("key", true))
	}
	result, err = db.SetWithOptions("key", []byte("second"), SetOptions{KeepTTL: true})
	if err != nil || !result.Stored || db.TTL("key", true) != 1000 {
		t.Fatalf("SET KEEPTTL = (%#v, %v), ttl %d", result, err, db.TTL("key", true))
	}
	db.Set("key", []byte("persistent"))
	if ttl := db.TTL("key", true); ttl != -1 {
		t.Fatalf("plain SET retained TTL: %d", ttl)
	}
	if db.Persist("key") {
		t.Fatal("PERSIST reported a change for persistent key")
	}
}

func TestExpiredCollectionsBehaveAsMissing(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	db := NewDBWithClock(func() time.Time { return now })
	_, _ = db.HSet("hash", "field", "value")
	_, _ = db.RPush("list", "value")
	_, _ = db.SAdd("set", "value")
	_, _ = db.ZAdd("zset", 1, "value")
	for _, key := range []string{"hash", "list", "set", "zset"} {
		if changed, err := db.ExpireAt(key, now.Add(time.Second), ExpireOptions{}); err != nil || !changed {
			t.Fatalf("ExpireAt(%s) = (%t, %v)", key, changed, err)
		}
	}
	now = now.Add(time.Second)
	if _, found, err := db.HGet("hash", "field"); err != nil || found {
		t.Fatalf("expired HGet = (%t, %v)", found, err)
	}
	if length, err := db.LLen("list"); err != nil || length != 0 {
		t.Fatalf("expired LLen = (%d, %v)", length, err)
	}
	if cardinality, err := db.SCard("set"); err != nil || cardinality != 0 {
		t.Fatalf("expired SCard = (%d, %v)", cardinality, err)
	}
	if cardinality, err := db.ZCard("zset"); err != nil || cardinality != 0 {
		t.Fatalf("expired ZCard = (%d, %v)", cardinality, err)
	}
}

func TestActiveExpirationWorker(t *testing.T) {
	var clockMu sync.Mutex
	now := time.UnixMilli(1_700_000_000_000)
	db := NewDBWithClock(func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	})
	db.Set("key", []byte("value"))
	if changed, err := db.ExpireAt("key", now.Add(time.Second), ExpireOptions{}); err != nil || !changed {
		t.Fatalf("ExpireAt = (%t, %v)", changed, err)
	}
	_, _ = db.HSet("hash", "field", "value")
	if results, err := db.HExpireAt("hash", []string{"field"}, now.Add(time.Second), ExpireOptions{}); err != nil || results[0] != 1 {
		t.Fatalf("HExpireAt = (%v, %v)", results, err)
	}
	if added, err := db.SAddEx("set", time.Second, "member"); err != nil || added != 1 {
		t.Fatalf("SAddEx = (%d, %v)", added, err)
	}
	db.StartExpiration(time.Millisecond, 1)
	clockMu.Lock()
	now = now.Add(time.Second)
	clockMu.Unlock()

	deadline := time.Now().Add(time.Second)
	for {
		keyShard := db.shardFor("key")
		keyShard.mu.RLock()
		_, keyExists := keyShard.entries["key"]
		keyShard.mu.RUnlock()
		hashShard := db.shardFor("hash")
		hashShard.mu.RLock()
		_, hashExists := hashShard.entries["hash"]
		hashShard.mu.RUnlock()
		setShard := db.shardFor("set")
		setShard.mu.RLock()
		_, setExists := setShard.entries["set"]
		setShard.mu.RUnlock()
		if !keyExists && !hashExists && !setExists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("active expiration worker did not delete key")
		}
		time.Sleep(time.Millisecond)
	}
	db.StopExpiration()
	db.StopExpiration()
}

func TestHashFieldExpiration(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	db := NewDBWithClock(func() time.Time { return now })
	_, _ = db.HSetMany("hash", HashPair{Field: "counter", Value: "1"}, HashPair{Field: "other", Value: "value"})

	results, err := db.HExpireAt("hash", []string{"counter", "missing"}, now.Add(time.Second), ExpireOptions{})
	if err != nil || len(results) != 2 || results[0] != 1 || results[1] != -2 {
		t.Fatalf("HExpireAt = (%v, %v)", results, err)
	}
	if ttl, err := db.HFieldTTL("hash", []string{"counter", "other", "missing"}, true, false); err != nil ||
		len(ttl) != 3 || ttl[0] != 1000 || ttl[1] != -1 || ttl[2] != -2 {
		t.Fatalf("HFieldTTL = (%v, %v)", ttl, err)
	}
	if value, err := db.HIncrBy("hash", "counter", 1); err != nil || value != 2 {
		t.Fatalf("HIncrBy = (%d, %v)", value, err)
	}
	if ttl, _ := db.HFieldTTL("hash", []string{"counter"}, true, false); ttl[0] != 1000 {
		t.Fatalf("numeric mutation cleared field TTL: %v", ttl)
	}
	if _, err := db.HSet("hash", "counter", "replacement"); err != nil {
		t.Fatal(err)
	}
	if ttl, _ := db.HFieldTTL("hash", []string{"counter"}, true, false); ttl[0] != -1 {
		t.Fatalf("HSET did not clear field TTL: %v", ttl)
	}

	results, err = db.HExpireAt("hash", []string{"counter"}, now, ExpireOptions{})
	if err != nil || results[0] != 2 {
		t.Fatalf("immediate HExpireAt = (%v, %v)", results, err)
	}
	if found, _ := db.HExists("hash", "counter"); found {
		t.Fatal("immediately expired field still exists")
	}
	results, _ = db.HExpireAt("hash", []string{"other"}, now.Add(time.Second), ExpireOptions{})
	now = now.Add(time.Second)
	if length, err := db.HLen("hash"); err != nil || length != 0 || db.Type("hash") != KindNone {
		t.Fatalf("hash after final field expiration = (%d, %v, %s)", length, err, db.Type("hash"))
	}
}

func TestSetMemberExpiration(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	db := NewDBWithClock(func() time.Time { return now })
	if added, err := db.SAddEx("set", time.Second, "temporary", "shared"); err != nil || added != 2 {
		t.Fatalf("SAddEx = (%d, %v)", added, err)
	}
	if added, err := db.SAdd("set", "shared", "persistent"); err != nil || added != 1 {
		t.Fatalf("SAdd = (%d, %v)", added, err)
	}
	now = now.Add(time.Second)
	if members, err := db.SMembers("set"); err != nil || len(members) != 1 || members[0] != "persistent" {
		t.Fatalf("members after item expiration = (%v, %v)", members, err)
	}
	if removed, err := db.SRem("set", "persistent"); err != nil || removed != 1 || db.Type("set") != KindNone {
		t.Fatalf("SRem final member = (%d, %v), type %s", removed, err, db.Type("set"))
	}
}
