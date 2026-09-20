package engine

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func TestUnifiedKeyspaceRejectsWrongType(t *testing.T) {
	db := NewDB()

	if added, err := db.HSet("shared", "field", "value"); err != nil || added != 1 {
		t.Fatalf("HSet() = (%d, %v), want (1, nil)", added, err)
	}
	if got := db.Type("shared"); got != KindHash {
		t.Fatalf("Type(shared) = %s, want hash", got)
	}
	if _, err := db.ZAdd("shared", 1, "member"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ZAdd() error = %v, want ErrWrongType", err)
	}

	if removed := db.Del("shared"); removed != 1 {
		t.Fatalf("Del(shared) = %d, want 1", removed)
	}
	if added, err := db.ZAdd("shared", 1, "member"); err != nil || added != 1 {
		t.Fatalf("ZAdd() = (%d, %v), want (1, nil)", added, err)
	}
	if _, _, err := db.HGet("shared", "field"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("HGet() error = %v, want ErrWrongType", err)
	}
}

func TestGenericKeyCommands(t *testing.T) {
	db := NewDB()
	if _, err := db.HSet("hash", "field", "value"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ZAdd("zset", 1, "member"); err != nil {
		t.Fatal(err)
	}

	if got := db.Exists("hash", "missing", "zset", "hash"); got != 3 {
		t.Fatalf("Exists() = %d, want 3", got)
	}
	if got := db.Del("hash", "missing", "hash"); got != 1 {
		t.Fatalf("Del() = %d, want 1", got)
	}
	if got := db.Type("hash"); got != KindNone {
		t.Fatalf("Type(hash) = %s, want none", got)
	}
	if got := db.Type("zset"); got != KindSortedSet {
		t.Fatalf("Type(zset) = %s, want zset", got)
	}
}

func TestEmptyCollectionsRemoveTheirKeys(t *testing.T) {
	db := NewDB()
	if _, err := db.HSet("hash", "field", "value"); err != nil {
		t.Fatal(err)
	}
	if removed, err := db.HDel("hash", "field"); err != nil || removed != 1 {
		t.Fatalf("HDel() = (%d, %v), want (1, nil)", removed, err)
	}
	if got := db.Type("hash"); got != KindNone {
		t.Fatalf("Type(hash) = %s after deleting last field, want none", got)
	}

	if _, err := db.ZAdd("zset", 1, "member"); err != nil {
		t.Fatal(err)
	}
	if removed, err := db.ZRem("zset", "member"); err != nil || removed != 1 {
		t.Fatalf("ZRem() = (%d, %v), want (1, nil)", removed, err)
	}
	if got := db.Type("zset"); got != KindNone {
		t.Fatalf("Type(zset) = %s after deleting last member, want none", got)
	}
}

func TestStringSetGetAndOverwrite(t *testing.T) {
	db := NewDB()
	original := []byte("value\x00with\nbytes")
	db.Set("key", original)
	original[0] = 'X'

	value, found, err := db.Get("key")
	if err != nil || !found || string(value) != "value\x00with\nbytes" {
		t.Fatalf("Get() = (%q, %t, %v)", value, found, err)
	}
	value[0] = 'X'
	value, _, _ = db.Get("key")
	if string(value) != "value\x00with\nbytes" {
		t.Fatalf("mutating Get result changed stored value to %q", value)
	}

	if _, err := db.HSet("hash", "field", "value"); err != nil {
		t.Fatal(err)
	}
	db.Set("hash", []byte("replacement"))
	if got := db.Type("hash"); got != KindString {
		t.Fatalf("Type(hash) = %s, want string", got)
	}
	if _, _, err := db.HGet("hash", "field"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("HGet() error = %v, want ErrWrongType", err)
	}
}

func TestMSetAndMGet(t *testing.T) {
	db := NewDB()
	if _, err := db.HSet("hash", "field", "value"); err != nil {
		t.Fatal(err)
	}
	db.MSet(
		StringPair{Key: "first", Value: []byte("one")},
		StringPair{Key: "second", Value: []byte("two")},
		StringPair{Key: "first", Value: []byte("last")},
	)

	results := db.MGet("first", "missing", "hash", "second")
	if len(results) != 4 {
		t.Fatalf("len(MGet()) = %d, want 4", len(results))
	}
	if !results[0].Found || string(results[0].Value) != "last" {
		t.Fatalf("first result = %#v", results[0])
	}
	if results[1].Found || results[2].Found {
		t.Fatalf("missing and non-string results = %#v, %#v", results[1], results[2])
	}
	if !results[3].Found || string(results[3].Value) != "two" {
		t.Fatalf("second result = %#v", results[3])
	}
}

func TestAppendAndStrLen(t *testing.T) {
	db := NewDB()

	if length, err := db.Append("key", []byte{'a', 0}); err != nil || length != 2 {
		t.Fatalf("Append() = (%d, %v), want (2, nil)", length, err)
	}
	if length, err := db.Append("key", []byte{'b', '\n'}); err != nil || length != 4 {
		t.Fatalf("Append() = (%d, %v), want (4, nil)", length, err)
	}
	if length, err := db.StrLen("key"); err != nil || length != 4 {
		t.Fatalf("StrLen() = (%d, %v), want (4, nil)", length, err)
	}
	value, _, _ := db.Get("key")
	if !bytes.Equal(value, []byte{'a', 0, 'b', '\n'}) {
		t.Fatalf("Get() = %v", value)
	}

	if _, err := db.HSet("hash", "field", "value"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Append("hash", []byte("value")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Append() error = %v, want ErrWrongType", err)
	}
}

func TestIntegerMutations(t *testing.T) {
	db := NewDB()

	if got, err := db.IncrBy("counter", 1); err != nil || got != 1 {
		t.Fatalf("IncrBy() = (%d, %v), want (1, nil)", got, err)
	}
	if got, err := db.IncrBy("counter", 24); err != nil || got != 25 {
		t.Fatalf("IncrBy() = (%d, %v), want (25, nil)", got, err)
	}
	if got, err := db.DecrBy("counter", 5); err != nil || got != 20 {
		t.Fatalf("DecrBy() = (%d, %v), want (20, nil)", got, err)
	}
	if got, err := db.DecrBy("counter", -2); err != nil || got != 22 {
		t.Fatalf("DecrBy(-2) = (%d, %v), want (22, nil)", got, err)
	}

	db.Set("invalid", []byte("not-a-number"))
	if _, err := db.IncrBy("invalid", 1); !errors.Is(err, ErrInvalidInteger) {
		t.Fatalf("IncrBy(invalid) error = %v, want ErrInvalidInteger", err)
	}
	assertStringValue(t, db, "invalid", "not-a-number")

	db.Set("max", []byte("9223372036854775807"))
	if _, err := db.IncrBy("max", 1); !errors.Is(err, ErrInvalidInteger) {
		t.Fatalf("IncrBy(max) error = %v, want ErrInvalidInteger", err)
	}
	assertStringValue(t, db, "max", "9223372036854775807")

	db.Set("min", []byte("-9223372036854775808"))
	if _, err := db.DecrBy("min", 1); !errors.Is(err, ErrInvalidInteger) {
		t.Fatalf("DecrBy(min) error = %v, want ErrInvalidInteger", err)
	}
	assertStringValue(t, db, "min", "-9223372036854775808")
}

func TestConcurrentIncrements(t *testing.T) {
	db := NewDB()
	const workers = 100

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.IncrBy("counter", 1)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	assertStringValue(t, db, "counter", "100")
}

func assertStringValue(t *testing.T, db *DB, key, want string) {
	t.Helper()
	value, found, err := db.Get(key)
	if err != nil || !found || string(value) != want {
		t.Fatalf("Get(%q) = (%q, %t, %v), want %q", key, value, found, err, want)
	}
}
