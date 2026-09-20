package engine

import (
	"errors"
	"reflect"
	"testing"
)

func TestHashFieldOperations(t *testing.T) {
	db := NewDB()
	added, err := db.HSetMany("hash",
		HashPair{Field: "alpha", Value: "one"},
		HashPair{Field: "beta", Value: "two"},
		HashPair{Field: "alpha", Value: "updated"},
	)
	if err != nil || added != 2 {
		t.Fatalf("HSetMany() = (%d, %v), want (2, nil)", added, err)
	}
	results, err := db.HMGet("hash", "alpha", "missing", "beta")
	if err != nil || !reflect.DeepEqual(results, []HashResult{
		{Value: "updated", Found: true}, {}, {Value: "two", Found: true},
	}) {
		t.Fatalf("HMGet() = (%#v, %v)", results, err)
	}
	if found, err := db.HExists("hash", "alpha"); err != nil || !found {
		t.Fatalf("HExists() = (%t, %v), want (true, nil)", found, err)
	}
	if length, err := db.HStrLen("hash", "alpha"); err != nil || length != 7 {
		t.Fatalf("HStrLen() = (%d, %v), want (7, nil)", length, err)
	}
	if keys, err := db.HKeys("hash"); err != nil || !reflect.DeepEqual(keys, []string{"alpha", "beta"}) {
		t.Fatalf("HKeys() = (%v, %v)", keys, err)
	}
	if values, err := db.HVals("hash"); err != nil || !reflect.DeepEqual(values, []string{"updated", "two"}) {
		t.Fatalf("HVals() = (%v, %v)", values, err)
	}
	if removed, err := db.HDel("hash", "alpha", "missing", "alpha", "beta"); err != nil || removed != 2 {
		t.Fatalf("HDel() = (%d, %v), want (2, nil)", removed, err)
	}
	if db.Type("hash") != KindNone {
		t.Fatal("empty hash key was not removed")
	}
}

func TestHashConditionalAndNumericMutations(t *testing.T) {
	db := NewDB()
	if stored, err := db.HSetNX("hash", "field", "first"); err != nil || !stored {
		t.Fatalf("HSetNX(first) = (%t, %v)", stored, err)
	}
	if stored, err := db.HSetNX("hash", "field", "second"); err != nil || stored {
		t.Fatalf("HSetNX(second) = (%t, %v)", stored, err)
	}
	if value, _, _ := db.HGet("hash", "field"); value != "first" {
		t.Fatalf("field = %q, want first", value)
	}

	if value, err := db.HIncrBy("numbers", "integer", 4); err != nil || value != 4 {
		t.Fatalf("HIncrBy() = (%d, %v)", value, err)
	}
	if value, err := db.HIncrBy("numbers", "integer", -7); err != nil || value != -3 {
		t.Fatalf("HIncrBy() = (%d, %v)", value, err)
	}
	if value, err := db.HIncrByFloat("numbers", "float", 1.25); err != nil || value != "1.25" {
		t.Fatalf("HIncrByFloat() = (%q, %v)", value, err)
	}
	if value, err := db.HIncrByFloat("numbers", "float", .75); err != nil || value != "2" {
		t.Fatalf("HIncrByFloat() = (%q, %v)", value, err)
	}

	if _, err := db.HSet("numbers", "invalid", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.HIncrBy("numbers", "invalid", 1); !errors.Is(err, ErrInvalidInteger) {
		t.Fatalf("HIncrBy(empty) error = %v", err)
	}
	if value, _, _ := db.HGet("numbers", "invalid"); value != "" {
		t.Fatalf("failed increment changed field to %q", value)
	}
	if _, err := db.HSet("numbers", "maximum", "9223372036854775807"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.HIncrBy("numbers", "maximum", 1); !errors.Is(err, ErrInvalidInteger) {
		t.Fatalf("overflow error = %v", err)
	}
}

func TestHashScan(t *testing.T) {
	db := NewDB()
	_, err := db.HSetMany("hash",
		HashPair{Field: "charlie", Value: "3"},
		HashPair{Field: "alpha", Value: "1"},
		HashPair{Field: "bravo", Value: "2"},
	)
	if err != nil {
		t.Fatal(err)
	}
	next, pairs, err := db.HScan("hash", 0, ScanOptions{Count: 2, Match: "a*", UseMatch: true})
	if err != nil || next != 2 || !reflect.DeepEqual(pairs, []HashPair{{Field: "alpha", Value: "1"}}) {
		t.Fatalf("first HScan() = (%d, %#v, %v)", next, pairs, err)
	}
	next, pairs, err = db.HScan("hash", next, ScanOptions{Count: 2})
	if err != nil || next != 0 || !reflect.DeepEqual(pairs, []HashPair{{Field: "charlie", Value: "3"}}) {
		t.Fatalf("second HScan() = (%d, %#v, %v)", next, pairs, err)
	}
}
