package engine

import (
	"errors"
	"testing"
)

func TestDequeWrapAndGrow(t *testing.T) {
	list := newDeque("a", "b", "c", "d")
	if got := list.popLeft(); got != "a" {
		t.Fatalf("popLeft() = %q, want a", got)
	}
	if got := list.popLeft(); got != "b" {
		t.Fatalf("popLeft() = %q, want b", got)
	}
	list.pushRight("e")
	list.pushRight("f")
	list.pushRight("g")
	list.pushLeft("x")
	assertValues(t, list.values(), "x", "c", "d", "e", "f", "g")
	if got := list.popRight(); got != "g" {
		t.Fatalf("popRight() = %q, want g", got)
	}
	assertValues(t, list.values(), "x", "c", "d", "e", "f")
}

func TestListPushPopLengthAndRange(t *testing.T) {
	db := NewDB()

	if length, err := db.LPush("list", "a", "b"); err != nil || length != 2 {
		t.Fatalf("LPush() = (%d, %v), want (2, nil)", length, err)
	}
	if length, err := db.RPush("list", "c", "d"); err != nil || length != 4 {
		t.Fatalf("RPush() = (%d, %v), want (4, nil)", length, err)
	}
	if got := db.Type("list"); got != KindList {
		t.Fatalf("Type(list) = %s, want list", got)
	}
	if length, err := db.LLen("list"); err != nil || length != 4 {
		t.Fatalf("LLen() = (%d, %v), want (4, nil)", length, err)
	}
	assertListRange(t, db, "list", 0, -1, "b", "a", "c", "d")
	assertListRange(t, db, "list", -3, -1, "a", "c", "d")
	assertListRange(t, db, "list", -100, 100, "b", "a", "c", "d")
	assertListRange(t, db, "list", 10, 20)

	values, err := db.RPop("list", 2)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, values, "d", "c")
	values, err = db.LPop("list", 10)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, values, "b", "a")
	if got := db.Type("list"); got != KindNone {
		t.Fatalf("Type(list) = %s after final pop, want none", got)
	}

	if values, err := db.LPop("missing", 1); err != nil || len(values) != 0 {
		t.Fatalf("LPop(missing) = (%v, %v), want empty result", values, err)
	}
	if _, err := db.RPop("missing", -1); !errors.Is(err, ErrInvalidInteger) {
		t.Fatalf("RPop(-1) error = %v, want ErrInvalidInteger", err)
	}
}

func TestListIndexSetAndTrim(t *testing.T) {
	db := NewDB()
	if _, err := db.RPush("list", "a", "b", "c", "d"); err != nil {
		t.Fatal(err)
	}

	if value, found, err := db.LIndex("list", -2); err != nil || !found || value != "c" {
		t.Fatalf("LIndex(-2) = (%q, %t, %v), want (c, true, nil)", value, found, err)
	}
	if _, found, err := db.LIndex("list", 10); err != nil || found {
		t.Fatalf("LIndex(10) = (_, %t, %v), want (_, false, nil)", found, err)
	}
	if err := db.LSet("list", -1, "last"); err != nil {
		t.Fatal(err)
	}
	assertListRange(t, db, "list", 0, -1, "a", "b", "c", "last")

	if err := db.LSet("list", 10, "value"); !errors.Is(err, ErrIndexOutOfRange) {
		t.Fatalf("LSet() error = %v, want ErrIndexOutOfRange", err)
	}
	if err := db.LSet("missing", 0, "value"); !errors.Is(err, ErrIndexOutOfRange) {
		t.Fatalf("LSet(missing) error = %v, want ErrIndexOutOfRange", err)
	}

	if err := db.LTrim("list", 1, -2); err != nil {
		t.Fatal(err)
	}
	assertListRange(t, db, "list", 0, -1, "b", "c")
	if err := db.LTrim("list", 5, 10); err != nil {
		t.Fatal(err)
	}
	if got := db.Type("list"); got != KindNone {
		t.Fatalf("Type(list) = %s after empty trim, want none", got)
	}
	if err := db.LTrim("missing", 0, -1); err != nil {
		t.Fatalf("LTrim(missing) error = %v", err)
	}
}

func TestListInsertAndRemove(t *testing.T) {
	db := NewDB()
	if _, err := db.RPush("list", "a", "b", "a", "c"); err != nil {
		t.Fatal(err)
	}

	if length, err := db.LInsert("list", true, "a", "before"); err != nil || length != 5 {
		t.Fatalf("LInsert(before) = (%d, %v), want (5, nil)", length, err)
	}
	if length, err := db.LInsert("list", false, "a", "after"); err != nil || length != 6 {
		t.Fatalf("LInsert(after) = (%d, %v), want (6, nil)", length, err)
	}
	assertListRange(t, db, "list", 0, -1, "before", "a", "after", "b", "a", "c")
	if length, err := db.LInsert("list", true, "missing", "value"); err != nil || length != -1 {
		t.Fatalf("LInsert(missing pivot) = (%d, %v), want (-1, nil)", length, err)
	}
	if length, err := db.LInsert("missing", true, "pivot", "value"); err != nil || length != 0 {
		t.Fatalf("LInsert(missing key) = (%d, %v), want (0, nil)", length, err)
	}

	if removed, err := db.LRem("list", 1, "a"); err != nil || removed != 1 {
		t.Fatalf("LRem(1) = (%d, %v), want (1, nil)", removed, err)
	}
	assertListRange(t, db, "list", 0, -1, "before", "after", "b", "a", "c")
	if _, err := db.RPush("list", "a", "x", "a"); err != nil {
		t.Fatal(err)
	}
	if removed, err := db.LRem("list", -2, "a"); err != nil || removed != 2 {
		t.Fatalf("LRem(-2) = (%d, %v), want (2, nil)", removed, err)
	}
	assertListRange(t, db, "list", 0, -1, "before", "after", "b", "a", "c", "x")
	if removed, err := db.LRem("list", 0, "a"); err != nil || removed != 1 {
		t.Fatalf("LRem(0) = (%d, %v), want (1, nil)", removed, err)
	}
	assertListRange(t, db, "list", 0, -1, "before", "after", "b", "c", "x")

	if _, err := db.RPush("all", "same", "same"); err != nil {
		t.Fatal(err)
	}
	if removed, err := db.LRem("all", 0, "same"); err != nil || removed != 2 {
		t.Fatalf("LRem(all) = (%d, %v), want (2, nil)", removed, err)
	}
	if got := db.Type("all"); got != KindNone {
		t.Fatalf("Type(all) = %s after removing all elements, want none", got)
	}
}

func TestListMoveAndRotate(t *testing.T) {
	db := NewDB()
	if _, err := db.RPush("source", "a", "b", "c"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RPush("destination", "x", "y"); err != nil {
		t.Fatal(err)
	}

	value, found, err := db.LMove("source", "destination", ListRight, ListLeft)
	if err != nil || !found || value != "c" {
		t.Fatalf("LMove() = (%q, %t, %v), want (c, true, nil)", value, found, err)
	}
	assertListRange(t, db, "source", 0, -1, "a", "b")
	assertListRange(t, db, "destination", 0, -1, "c", "x", "y")

	value, found, err = db.LMove("source", "destination", ListLeft, ListRight)
	if err != nil || !found || value != "a" {
		t.Fatalf("LMove() = (%q, %t, %v), want (a, true, nil)", value, found, err)
	}
	assertListRange(t, db, "source", 0, -1, "b")
	assertListRange(t, db, "destination", 0, -1, "c", "x", "y", "a")

	if _, err := db.RPush("rotation", "1", "2", "3"); err != nil {
		t.Fatal(err)
	}
	value, found, err = db.RPopLPush("rotation", "rotation")
	if err != nil || !found || value != "3" {
		t.Fatalf("RPopLPush(rotation) = (%q, %t, %v)", value, found, err)
	}
	assertListRange(t, db, "rotation", 0, -1, "3", "1", "2")

	db.Set("wrong-type", []byte("value"))
	if _, _, err := db.LMove("source", "wrong-type", ListLeft, ListRight); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LMove() error = %v, want ErrWrongType", err)
	}
	assertListRange(t, db, "source", 0, -1, "b")
	if _, _, err := db.LMove("missing", "wrong-type", ListLeft, ListRight); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LMove() error = %v, want ErrWrongType", err)
	}
	assertListRange(t, db, "destination", 0, -1, "c", "x", "y", "a")
}

func TestListWrongType(t *testing.T) {
	db := NewDB()
	db.Set("string", []byte("value"))

	if _, err := db.LPush("string", "value"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LPush() error = %v, want ErrWrongType", err)
	}
	if _, err := db.LPop("string", 1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LPop() error = %v, want ErrWrongType", err)
	}
	if _, err := db.LLen("string"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LLen() error = %v, want ErrWrongType", err)
	}
	if _, err := db.LRange("string", 0, -1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LRange() error = %v, want ErrWrongType", err)
	}
	if _, _, err := db.LIndex("string", 0); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LIndex() error = %v, want ErrWrongType", err)
	}
	if err := db.LSet("string", 0, "value"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LSet() error = %v, want ErrWrongType", err)
	}
	if err := db.LTrim("string", 0, -1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LTrim() error = %v, want ErrWrongType", err)
	}
	if _, err := db.LInsert("string", true, "pivot", "value"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LInsert() error = %v, want ErrWrongType", err)
	}
	if _, err := db.LRem("string", 0, "value"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LRem() error = %v, want ErrWrongType", err)
	}
}

func assertListRange(t *testing.T, db *DB, key string, start, stop int64, want ...string) {
	t.Helper()
	values, err := db.LRange(key, start, stop)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, values, want...)
}

func assertValues(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("values = %v, want %v", got, want)
		}
	}
}
