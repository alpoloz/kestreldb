package engine

import (
	"errors"
	"slices"
	"testing"
)

func TestSetOperations(t *testing.T) {
	db := NewDB()

	if added, err := db.SAdd("colors", "red", "green", "red"); err != nil || added != 2 {
		t.Fatalf("SAdd() = (%d, %v), want (2, nil)", added, err)
	}
	if got := db.Type("colors"); got != KindSet {
		t.Fatalf("Type(colors) = %s, want set", got)
	}
	if count, err := db.SCard("colors"); err != nil || count != 2 {
		t.Fatalf("SCard() = (%d, %v), want (2, nil)", count, err)
	}
	if found, err := db.SIsMember("colors", "red"); err != nil || !found {
		t.Fatalf("SIsMember(red) = (%t, %v), want (true, nil)", found, err)
	}
	if found, err := db.SIsMember("colors", "blue"); err != nil || found {
		t.Fatalf("SIsMember(blue) = (%t, %v), want (false, nil)", found, err)
	}

	members, err := db.SMembers("colors")
	if err != nil {
		t.Fatal(err)
	}
	assertMembers(t, members, "red", "green")

	if removed, err := db.SRem("colors", "red", "missing", "red"); err != nil || removed != 1 {
		t.Fatalf("SRem() = (%d, %v), want (1, nil)", removed, err)
	}
	if removed, err := db.SRem("colors", "green"); err != nil || removed != 1 {
		t.Fatalf("SRem(last) = (%d, %v), want (1, nil)", removed, err)
	}
	if got := db.Type("colors"); got != KindNone {
		t.Fatalf("Type(colors) = %s after removing last member, want none", got)
	}
}

func TestSetMissingKeys(t *testing.T) {
	db := NewDB()

	if added, err := db.SAdd("empty"); err != nil || added != 0 {
		t.Fatalf("SAdd() = (%d, %v), want (0, nil)", added, err)
	}
	if got := db.Type("empty"); got != KindNone {
		t.Fatalf("Type(empty) = %s, want none", got)
	}
	if removed, err := db.SRem("missing", "member"); err != nil || removed != 0 {
		t.Fatalf("SRem() = (%d, %v), want (0, nil)", removed, err)
	}
	if found, err := db.SIsMember("missing", "member"); err != nil || found {
		t.Fatalf("SIsMember() = (%t, %v), want (false, nil)", found, err)
	}
	if count, err := db.SCard("missing"); err != nil || count != 0 {
		t.Fatalf("SCard() = (%d, %v), want (0, nil)", count, err)
	}
	if members, err := db.SMembers("missing"); err != nil || len(members) != 0 {
		t.Fatalf("SMembers() = (%v, %v), want empty result", members, err)
	}
}

func TestSetWrongType(t *testing.T) {
	db := NewDB()
	db.Set("string", []byte("value"))

	if _, err := db.SAdd("string", "member"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SAdd() error = %v, want ErrWrongType", err)
	}
	if _, err := db.SRem("string", "member"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SRem() error = %v, want ErrWrongType", err)
	}
	if _, err := db.SIsMember("string", "member"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SIsMember() error = %v, want ErrWrongType", err)
	}
	if _, err := db.SCard("string"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SCard() error = %v, want ErrWrongType", err)
	}
	if _, err := db.SMembers("string"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SMembers() error = %v, want ErrWrongType", err)
	}
}

func TestMultiMembershipAndMove(t *testing.T) {
	db := NewDB()
	if _, err := db.SAdd("source", "a", "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SAdd("destination", "b"); err != nil {
		t.Fatal(err)
	}

	membership, err := db.SMIsMember("source", "a", "missing", "b", "a")
	if err != nil || !slices.Equal(membership, []bool{true, false, true, true}) {
		t.Fatalf("SMIsMember() = (%v, %v)", membership, err)
	}

	if moved, err := db.SMove("source", "destination", "b"); err != nil || moved != 1 {
		t.Fatalf("SMove() = (%d, %v), want (1, nil)", moved, err)
	}
	assertStoredMembers(t, db, "source", "a")
	assertStoredMembers(t, db, "destination", "b")

	if moved, err := db.SMove("source", "new", "a"); err != nil || moved != 1 {
		t.Fatalf("SMove(last) = (%d, %v), want (1, nil)", moved, err)
	}
	if got := db.Type("source"); got != KindNone {
		t.Fatalf("Type(source) = %s, want none", got)
	}
	assertStoredMembers(t, db, "new", "a")

	if moved, err := db.SMove("new", "new", "a"); err != nil || moved != 1 {
		t.Fatalf("SMove(same key) = (%d, %v), want (1, nil)", moved, err)
	}
	if moved, err := db.SMove("missing", "new", "a"); err != nil || moved != 0 {
		t.Fatalf("SMove(missing) = (%d, %v), want (0, nil)", moved, err)
	}

	db.Set("wrong-type", []byte("value"))
	if _, err := db.SMove("missing", "wrong-type", "member"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SMove() error = %v, want ErrWrongType", err)
	}
	assertStoredMembers(t, db, "new", "a")
}

func TestRandomSetOperations(t *testing.T) {
	db := NewDB()
	if _, err := db.SAdd("set", "a", "b", "c"); err != nil {
		t.Fatal(err)
	}

	members, err := db.SRandMembers("set", 10)
	if err != nil {
		t.Fatal(err)
	}
	assertMembers(t, members, "a", "b", "c")

	members, err = db.SRandMembers("set", -5)
	if err != nil || len(members) != 5 {
		t.Fatalf("SRandMembers(-5) = (%v, %v), want five members", members, err)
	}
	for _, member := range members {
		if member != "a" && member != "b" && member != "c" {
			t.Fatalf("SRandMembers() returned unknown member %q", member)
		}
	}
	if count, err := db.SCard("set"); err != nil || count != 3 {
		t.Fatalf("SCard() after SRandMembers = (%d, %v), want (3, nil)", count, err)
	}

	popped, err := db.SPop("set", 2)
	if err != nil || len(popped) != 2 || popped[0] == popped[1] {
		t.Fatalf("SPop(2) = (%v, %v), want two distinct members", popped, err)
	}
	if count, err := db.SCard("set"); err != nil || count != 1 {
		t.Fatalf("SCard() after SPop = (%d, %v), want (1, nil)", count, err)
	}
	popped, err = db.SPop("set", 10)
	if err != nil || len(popped) != 1 {
		t.Fatalf("SPop(10) = (%v, %v), want final member", popped, err)
	}
	if got := db.Type("set"); got != KindNone {
		t.Fatalf("Type(set) = %s after SPop, want none", got)
	}

	if _, err := db.SPop("missing", -1); !errors.Is(err, ErrInvalidInteger) {
		t.Fatalf("SPop(-1) error = %v, want ErrInvalidInteger", err)
	}
	db.Set("string", []byte("value"))
	if _, err := db.SPop("string", 1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SPop() error = %v, want ErrWrongType", err)
	}
	if _, err := db.SRandMembers("string", 1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SRandMembers() error = %v, want ErrWrongType", err)
	}
}

func TestSetScan(t *testing.T) {
	db := NewDB()
	if _, err := db.SAdd("set", "alpha", "alpine", "beta", "bravo", "charlie"); err != nil {
		t.Fatal(err)
	}

	next, members, err := db.SScan("set", 0, ScanOptions{Count: 2})
	if err != nil || next != 2 {
		t.Fatalf("SScan(first) = (%d, %v, %v), want cursor 2", next, members, err)
	}
	assertMembers(t, members, "alpha", "alpine")

	next, members, err = db.SScan("set", next, ScanOptions{Count: 2, Match: "a*", UseMatch: true})
	if err != nil || next != 4 || len(members) != 0 {
		t.Fatalf("SScan(filtered) = (%d, %v, %v), want (4, empty, nil)", next, members, err)
	}

	next, members, err = db.SScan("set", next, ScanOptions{Count: 2})
	if err != nil || next != 0 {
		t.Fatalf("SScan(last) = (%d, %v, %v), want cursor 0", next, members, err)
	}
	assertMembers(t, members, "charlie")

	if next, members, err := db.SScan("missing", 42, ScanOptions{}); err != nil || next != 0 || len(members) != 0 {
		t.Fatalf("SScan(missing) = (%d, %v, %v), want empty result", next, members, err)
	}
	db.Set("string", []byte("value"))
	if _, _, err := db.SScan("string", 0, ScanOptions{}); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SScan() error = %v, want ErrWrongType", err)
	}
}

func TestSetAlgebra(t *testing.T) {
	db := NewDB()
	if _, err := db.SAdd("first", "a", "b", "c"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SAdd("second", "b", "c", "d"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SAdd("third", "c"); err != nil {
		t.Fatal(err)
	}

	union, err := db.SUnion("first", "second", "missing")
	if err != nil {
		t.Fatal(err)
	}
	assertMembers(t, union, "a", "b", "c", "d")

	intersection, err := db.SInter("first", "second", "third")
	if err != nil {
		t.Fatal(err)
	}
	assertMembers(t, intersection, "c")

	difference, err := db.SDiff("first", "second")
	if err != nil {
		t.Fatal(err)
	}
	assertMembers(t, difference, "a")

	intersection, err = db.SInter("first", "missing")
	if err != nil || len(intersection) != 0 {
		t.Fatalf("SInter() = (%v, %v), want empty result", intersection, err)
	}
	if empty, err := db.SUnion(); err != nil || len(empty) != 0 {
		t.Fatalf("SUnion() = (%v, %v), want empty result", empty, err)
	}
}

func TestSetAlgebraValidatesEverySourceType(t *testing.T) {
	db := NewDB()
	db.Set("string", []byte("value"))

	operations := map[string]func(...string) ([]string, error){
		"SUnion": db.SUnion,
		"SInter": db.SInter,
		"SDiff":  db.SDiff,
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if _, err := operation("missing", "string"); !errors.Is(err, ErrWrongType) {
				t.Fatalf("%s() error = %v, want ErrWrongType", name, err)
			}
		})
	}
}

func TestStoredSetAlgebra(t *testing.T) {
	db := NewDB()
	if _, err := db.SAdd("first", "a", "b", "c"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SAdd("second", "b", "c", "d"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SAdd("third", "c"); err != nil {
		t.Fatal(err)
	}

	db.Set("union", []byte("replace me"))
	if count, err := db.SUnionStore("union", "first", "second"); err != nil || count != 4 {
		t.Fatalf("SUnionStore() = (%d, %v), want (4, nil)", count, err)
	}
	assertStoredMembers(t, db, "union", "a", "b", "c", "d")

	if count, err := db.SInterStore("intersection", "first", "second", "third"); err != nil || count != 1 {
		t.Fatalf("SInterStore() = (%d, %v), want (1, nil)", count, err)
	}
	assertStoredMembers(t, db, "intersection", "c")

	if count, err := db.SDiffStore("difference", "first", "second"); err != nil || count != 1 {
		t.Fatalf("SDiffStore() = (%d, %v), want (1, nil)", count, err)
	}
	assertStoredMembers(t, db, "difference", "a")
}

func TestStoredSetAlgebraHandlesDestinationEdges(t *testing.T) {
	db := NewDB()
	if _, err := db.SAdd("first", "a", "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SAdd("second", "b", "c"); err != nil {
		t.Fatal(err)
	}

	if count, err := db.SUnionStore("first", "first", "second"); err != nil || count != 3 {
		t.Fatalf("SUnionStore(source destination) = (%d, %v), want (3, nil)", count, err)
	}
	assertStoredMembers(t, db, "first", "a", "b", "c")

	db.Set("empty-result", []byte("replace me"))
	if count, err := db.SInterStore("empty-result", "first", "missing"); err != nil || count != 0 {
		t.Fatalf("SInterStore(empty) = (%d, %v), want (0, nil)", count, err)
	}
	if got := db.Type("empty-result"); got != KindNone {
		t.Fatalf("Type(empty-result) = %s, want none", got)
	}

	if _, err := db.SAdd("destination", "old"); err != nil {
		t.Fatal(err)
	}
	db.Set("wrong-type", []byte("value"))
	if _, err := db.SUnionStore("destination", "first", "wrong-type"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SUnionStore() error = %v, want ErrWrongType", err)
	}
	assertStoredMembers(t, db, "destination", "old")
}

func assertMembers(t *testing.T, got []string, want ...string) {
	t.Helper()

	gotSet := make(map[string]struct{}, len(got))
	for _, member := range got {
		gotSet[member] = struct{}{}
	}
	if len(gotSet) != len(want) {
		t.Fatalf("members = %v, want %v", got, want)
	}
	for _, member := range want {
		if _, ok := gotSet[member]; !ok {
			t.Fatalf("members = %v, missing %q", got, member)
		}
	}
}

func assertStoredMembers(t *testing.T, db *DB, key string, want ...string) {
	t.Helper()
	members, err := db.SMembers(key)
	if err != nil {
		t.Fatal(err)
	}
	assertMembers(t, members, want...)
}
