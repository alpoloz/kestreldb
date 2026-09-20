package engine

import (
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestRenameAcrossShardsPreservesExpirationMetadata(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	db := NewDBWithClock(func() time.Time { return now })
	source, destination := keysOnDifferentShards(db)
	if _, err := db.HSet(source, "field", "value"); err != nil {
		t.Fatal(err)
	}
	if changed, err := db.ExpireAt(source, now.Add(5*time.Second), ExpireOptions{}); err != nil || !changed {
		t.Fatalf("ExpireAt = %v, %v", changed, err)
	}
	if result, err := db.HExpireAt(source, []string{"field"}, now.Add(3*time.Second), ExpireOptions{}); err != nil || !reflect.DeepEqual(result, []int{1}) {
		t.Fatalf("HExpireAt = %v, %v", result, err)
	}
	db.Set(destination, []byte("replace me"))

	if renamed, err := db.Rename(source, destination, false); err != nil || !renamed {
		t.Fatalf("Rename = %v, %v", renamed, err)
	}
	if got := db.Type(source); got != KindNone {
		t.Fatalf("source type = %v", got)
	}
	if got := db.Type(destination); got != KindHash {
		t.Fatalf("destination type = %v", got)
	}
	if got := db.TTL(destination, true); got != 5000 {
		t.Fatalf("destination TTL = %d", got)
	}
	if got, err := db.HFieldTTL(destination, []string{"field"}, true, false); err != nil || !reflect.DeepEqual(got, []int64{3000}) {
		t.Fatalf("destination field TTL = %v, %v", got, err)
	}
	if _, err := db.Rename("missing", destination, false); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("missing source error = %v", err)
	}
}

func TestRenameNXDoesNotReplaceDestination(t *testing.T) {
	db := NewDB()
	db.Set("source", []byte("one"))
	db.Set("destination", []byte("two"))
	if renamed, err := db.Rename("source", "destination", true); err != nil || renamed {
		t.Fatalf("Rename NX = %v, %v", renamed, err)
	}
	assertEngineString(t, db, "source", "one")
	assertEngineString(t, db, "destination", "two")
	if renamed, err := db.Rename("source", "source", true); err != nil || renamed {
		t.Fatalf("Rename NX same key = %v, %v", renamed, err)
	}
}

func TestCopyDeepCopiesEveryValueType(t *testing.T) {
	db := NewDB()

	db.Set("string", []byte("one"))
	mustCopy(t, db, "string", "string-copy")
	db.Set("string", []byte("changed"))
	assertEngineString(t, db, "string-copy", "one")

	if _, err := db.HSet("hash", "field", "one"); err != nil {
		t.Fatal(err)
	}
	mustCopy(t, db, "hash", "hash-copy")
	if _, err := db.HSet("hash", "field", "changed"); err != nil {
		t.Fatal(err)
	}
	if value, found, err := db.HGet("hash-copy", "field"); err != nil || !found || value != "one" {
		t.Fatalf("copied hash = %q, %v, %v", value, found, err)
	}

	if _, err := db.LPush("list", "one"); err != nil {
		t.Fatal(err)
	}
	mustCopy(t, db, "list", "list-copy")
	if err := db.LSet("list", 0, "changed"); err != nil {
		t.Fatal(err)
	}
	if value, found, err := db.LIndex("list-copy", 0); err != nil || !found || value != "one" {
		t.Fatalf("copied list = %q, %v, %v", value, found, err)
	}

	if _, err := db.SAdd("set", "one"); err != nil {
		t.Fatal(err)
	}
	mustCopy(t, db, "set", "set-copy")
	if _, err := db.SAdd("set", "changed"); err != nil {
		t.Fatal(err)
	}
	if found, err := db.SIsMember("set-copy", "changed"); err != nil || found {
		t.Fatalf("copied set changed member = %v, %v", found, err)
	}

	if _, err := db.ZAdd("zset", 1, "member"); err != nil {
		t.Fatal(err)
	}
	mustCopy(t, db, "zset", "zset-copy")
	if _, err := db.ZAdd("zset", 2, "member"); err != nil {
		t.Fatal(err)
	}
	if score, found, err := db.ZScore("zset-copy", "member"); err != nil || !found || score != 1 {
		t.Fatalf("copied zset = %v, %v, %v", score, found, err)
	}

	if copied, err := db.Copy("string", "string-copy", false); err != nil || copied {
		t.Fatalf("COPY without REPLACE = %v, %v", copied, err)
	}
	if copied, err := db.Copy("string", "string-copy", true); err != nil || !copied {
		t.Fatalf("COPY with REPLACE = %v, %v", copied, err)
	}
	if _, err := db.Copy("string", "string", true); !errors.Is(err, ErrSameKey) {
		t.Fatalf("COPY same key error = %v", err)
	}
}

func TestCopyPreservesKeyAndMemberExpirations(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	db := NewDBWithClock(func() time.Time { return now })
	if _, err := db.SAddEx("source", 3*time.Second, "member"); err != nil {
		t.Fatal(err)
	}
	if changed, err := db.ExpireAt("source", now.Add(5*time.Second), ExpireOptions{}); err != nil || !changed {
		t.Fatalf("ExpireAt = %v, %v", changed, err)
	}
	mustCopy(t, db, "source", "destination")
	if got := db.TTL("destination", true); got != 5000 {
		t.Fatalf("copied key TTL = %d", got)
	}
	now = now.Add(4 * time.Second)
	if count, err := db.SCard("source"); err != nil || count != 0 {
		t.Fatalf("source set after expiration = %d, %v", count, err)
	}
	if count, err := db.SCard("destination"); err != nil || count != 0 {
		t.Fatalf("copied set after expiration = %d, %v", count, err)
	}
}

func TestKeyspaceEnumerationAndFlush(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	db := NewDBWithClock(func() time.Time { return now })
	db.Set("alpha", []byte("one"))
	if _, err := db.HSet("beta", "field", "two"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.LPush("gamma", "three"); err != nil {
		t.Fatal(err)
	}
	db.Set("expired", []byte("gone"))
	if changed, err := db.ExpireAt("expired", now.Add(time.Second), ExpireOptions{}); err != nil || !changed {
		t.Fatalf("ExpireAt = %v, %v", changed, err)
	}
	now = now.Add(2 * time.Second)

	if got := db.Touch("alpha", "missing", "alpha"); got != 2 {
		t.Fatalf("Touch = %d", got)
	}
	if got := db.DBSize(); got != 3 {
		t.Fatalf("DBSize = %d", got)
	}
	if got := db.Keys("?e*"); !reflect.DeepEqual(got, []string{"beta"}) {
		t.Fatalf("Keys = %v", got)
	}

	next, page := db.Scan(0, ScanOptions{Count: 1, Match: "z*", UseMatch: true})
	if next == 0 || len(page) != 0 {
		t.Fatalf("filtered first page = cursor %d, keys %v", next, page)
	}
	var hashKeys []string
	cursor := uint64(0)
	for {
		cursor, page = db.Scan(cursor, ScanOptions{Count: 1, Type: "hash", UseType: true})
		hashKeys = append(hashKeys, page...)
		if cursor == 0 {
			break
		}
	}
	if !reflect.DeepEqual(hashKeys, []string{"beta"}) {
		t.Fatalf("SCAN TYPE hash = %v", hashKeys)
	}

	db.EnableJournal()
	before := db.JournalOffset()
	db.FlushDB()
	if got := db.DBSize(); got != 0 {
		t.Fatalf("DBSize after FlushDB = %d", got)
	}
	if got := db.JournalOffset(); got != before+1 {
		t.Fatalf("journal offset after FlushDB = %d, want %d", got, before+1)
	}
}

func TestKeyspaceMutationsProduceSingleJournalRecords(t *testing.T) {
	db := NewDB()
	db.EnableJournal()
	db.Set("source", []byte("value"))
	if renamed, err := db.Rename("source", "renamed", false); err != nil || !renamed {
		t.Fatalf("Rename = %v, %v", renamed, err)
	}
	if copied, err := db.Copy("renamed", "copied", false); err != nil || !copied {
		t.Fatalf("Copy = %v, %v", copied, err)
	}
	db.FlushDB()
	if got := db.JournalOffset(); got != 4 {
		t.Fatalf("journal offset = %d, want 4", got)
	}

	records, cancel, err := db.SubscribeFrom(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	for expected := uint64(1); expected <= 4; expected++ {
		record := <-records
		if record.Offset != expected {
			t.Fatalf("record offset = %d, want %d", record.Offset, expected)
		}
		restored := NewDB()
		if err := restored.LoadSnapshot(record.Snapshot); err != nil {
			t.Fatalf("LoadSnapshot at offset %d: %v", expected, err)
		}
		if expected == 2 && restored.Type("renamed") != KindString {
			t.Fatal("rename record does not contain destination")
		}
		if expected == 3 && restored.Type("copied") != KindString {
			t.Fatal("copy record does not contain destination")
		}
		if expected == 4 && restored.DBSize() != 0 {
			t.Fatal("flush record is not empty")
		}
	}
}

func mustCopy(t *testing.T, db *DB, source, destination string) {
	t.Helper()
	copied, err := db.Copy(source, destination, false)
	if err != nil || !copied {
		t.Fatalf("Copy(%q, %q) = %v, %v", source, destination, copied, err)
	}
}

func assertEngineString(t *testing.T, db *DB, key, want string) {
	t.Helper()
	value, found, err := db.Get(key)
	if err != nil || !found || string(value) != want {
		t.Fatalf("Get(%q) = %q, %v, %v; want %q", key, value, found, err, want)
	}
}

func TestKeysAreSortedForStableIteration(t *testing.T) {
	db := NewDB()
	for _, key := range []string{"charlie", "alpha", "bravo"} {
		db.Set(key, []byte(key))
	}
	got := db.Keys("*")
	want := append([]string(nil), got...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Keys = %v, want sorted %v", got, want)
	}
}
