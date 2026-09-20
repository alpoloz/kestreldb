package engine

import (
	"bytes"
	"errors"
	"testing"
)

func TestConditionalAndReplacementStrings(t *testing.T) {
	db := NewDB()
	if !db.SetNX("key", []byte("first")) || db.SetNX("key", []byte("second")) {
		t.Fatal("SetNX did not enforce non-existence")
	}
	result, err := db.SetWithOptions("key", []byte("updated"), SetOptions{XX: true, Get: true})
	if err != nil || !result.Stored || string(result.Previous) != "first" {
		t.Fatalf("SetWithOptions() = (%#v, %v)", result, err)
	}
	old, found, err := db.GetSet("key", []byte("replacement"))
	if err != nil || !found || string(old) != "updated" {
		t.Fatalf("GetSet() = (%q, %t, %v)", old, found, err)
	}
	old, found, err = db.GetDel("key")
	if err != nil || !found || string(old) != "replacement" || db.Type("key") != KindNone {
		t.Fatalf("GetDel() = (%q, %t, %v)", old, found, err)
	}

	db.Set("exists", []byte("old"))
	if db.MSetNX(StringPair{Key: "new", Value: []byte("new")}, StringPair{Key: "exists", Value: []byte("changed")}) {
		t.Fatal("MSetNX unexpectedly stored values")
	}
	if db.Exists("new") != 0 {
		t.Fatal("MSetNX partially mutated the keyspace")
	}
}

func TestStringRangesAndFloatIncrement(t *testing.T) {
	db := NewDB()
	if length, err := db.SetRange("key", 2, []byte("ab")); err != nil || length != 4 {
		t.Fatalf("SetRange() = (%d, %v)", length, err)
	}
	value, _, _ := db.Get("key")
	if !bytes.Equal(value, []byte{0, 0, 'a', 'b'}) {
		t.Fatalf("Get() = %v", value)
	}
	rangeValue, err := db.GetRange("key", -2, -1)
	if err != nil || string(rangeValue) != "ab" {
		t.Fatalf("GetRange() = (%q, %v)", rangeValue, err)
	}

	db.Set("float", []byte("10.5"))
	if result, err := db.IncrByFloat("float", 0.25); err != nil || result != "10.75" {
		t.Fatalf("IncrByFloat() = (%q, %v)", result, err)
	}
	db.Set("invalid", []byte("text"))
	if _, err := db.IncrByFloat("invalid", 1); !errors.Is(err, ErrInvalidFloat) {
		t.Fatalf("IncrByFloat() error = %v", err)
	}
	assertStringValue(t, db, "invalid", "text")
}
