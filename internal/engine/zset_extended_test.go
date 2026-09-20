package engine

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestZAddManyOptions(t *testing.T) {
	db := NewDB()
	result, err := db.ZAddMany("zset", ZAddOptions{},
		ZSetItem{Member: "one", Score: 1},
		ZSetItem{Member: "two", Score: 2},
		ZSetItem{Member: "one", Score: 3},
	)
	if err != nil || result.Count != 2 {
		t.Fatalf("ZAddMany() = (%#v, %v)", result, err)
	}
	if score, _, _ := db.ZScore("zset", "one"); score != 3 {
		t.Fatalf("duplicate member score = %g, want 3", score)
	}
	result, err = db.ZAddMany("zset", ZAddOptions{NX: true}, ZSetItem{Member: "one", Score: 4})
	if err != nil || result.Count != 0 {
		t.Fatalf("ZADD NX existing = (%#v, %v)", result, err)
	}
	result, err = db.ZAddMany("zset", ZAddOptions{XX: true, GT: true, CH: true}, ZSetItem{Member: "one", Score: 5})
	if err != nil || result.Count != 1 {
		t.Fatalf("ZADD XX GT CH = (%#v, %v)", result, err)
	}
	result, err = db.ZAddMany("zset", ZAddOptions{INCR: true}, ZSetItem{Member: "one", Score: -2})
	if err != nil || !result.ScoreFound || result.Score != 3 {
		t.Fatalf("ZADD INCR = (%#v, %v)", result, err)
	}
	if _, err := db.ZAddMany("zset", ZAddOptions{NX: true, XX: true}, ZSetItem{Member: "bad", Score: 1}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("conflicting options error = %v", err)
	}
	if _, err := db.ZAddMany("zset", ZAddOptions{}, ZSetItem{Member: "bad", Score: math.NaN()}); !errors.Is(err, ErrInvalidFloat) {
		t.Fatalf("NaN score error = %v", err)
	}
}

func TestZSetRangesCountsAndRemovals(t *testing.T) {
	db := NewDB()
	_, err := db.ZAddMany("zset", ZAddOptions{},
		ZSetItem{Member: "alpha", Score: 1}, ZSetItem{Member: "bravo", Score: 2},
		ZSetItem{Member: "charlie", Score: 3}, ZSetItem{Member: "delta", Score: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := db.ZCount("zset", ScoreBound{Value: 1, Exclusive: true}, ScoreBound{Value: 4}); err != nil || count != 3 {
		t.Fatalf("ZCount() = (%d, %v)", count, err)
	}
	if count, err := db.ZLexCount("zset", LexBound{Value: "bravo"}, LexBound{Value: "delta", Exclusive: true}); err != nil || count != 2 {
		t.Fatalf("ZLexCount() = (%d, %v)", count, err)
	}
	items, err := db.ZRangeByScore("zset", ScoreBound{Value: 1}, ScoreBound{Value: 4}, true, 1, 2)
	if err != nil || !reflect.DeepEqual(items, []ZSetItem{{Member: "charlie", Score: 3}, {Member: "bravo", Score: 2}}) {
		t.Fatalf("ZRangeByScore() = (%#v, %v)", items, err)
	}
	items, err = db.ZRangeByLex("zset", LexBound{Infinite: -1}, LexBound{Infinite: 1}, false, 1, 2)
	if err != nil || !reflect.DeepEqual(items, []ZSetItem{{Member: "bravo", Score: 2}, {Member: "charlie", Score: 3}}) {
		t.Fatalf("ZRangeByLex() = (%#v, %v)", items, err)
	}
	if removed, err := db.ZRemRangeByScore("zset", ScoreBound{Value: 2}, ScoreBound{Value: 3}); err != nil || removed != 2 {
		t.Fatalf("ZRemRangeByScore() = (%d, %v)", removed, err)
	}
	items, err = db.ZPop("zset", true, 1)
	if err != nil || !reflect.DeepEqual(items, []ZSetItem{{Member: "delta", Score: 4}}) {
		t.Fatalf("ZPop(max) = (%#v, %v)", items, err)
	}
}

func TestZSetScanAndAlgebra(t *testing.T) {
	db := NewDB()
	_, _ = db.ZAddMany("first", ZAddOptions{},
		ZSetItem{Member: "alpha", Score: 1}, ZSetItem{Member: "beta", Score: 2}, ZSetItem{Member: "shared", Score: 3},
	)
	_, _ = db.ZAddMany("second", ZAddOptions{},
		ZSetItem{Member: "gamma", Score: 4}, ZSetItem{Member: "shared", Score: 5},
	)
	next, items, err := db.ZScan("first", 0, ScanOptions{Count: 2, Match: "a*", UseMatch: true})
	if err != nil || next != 2 || !reflect.DeepEqual(items, []ZSetItem{{Member: "alpha", Score: 1}}) {
		t.Fatalf("ZScan() = (%d, %#v, %v)", next, items, err)
	}
	sources := []ZWeightedKey{{Key: "first", Weight: 2}, {Key: "second", Weight: 3}}
	items, err = db.ZUnion(sources, ZAggregateSum)
	if err != nil || len(items) != 4 {
		t.Fatalf("ZUnion() = (%#v, %v)", items, err)
	}
	if score := itemScore(items, "shared"); score != 21 {
		t.Fatalf("weighted union shared score = %g, want 21", score)
	}
	items, err = db.ZInter(sources, ZAggregateMax)
	if err != nil || !reflect.DeepEqual(items, []ZSetItem{{Member: "shared", Score: 15}}) {
		t.Fatalf("ZInter() = (%#v, %v)", items, err)
	}
	items, err = db.ZDiff("first", "second")
	if err != nil || len(items) != 2 || itemScore(items, "shared") != 0 {
		t.Fatalf("ZDiff() = (%#v, %v)", items, err)
	}
	if count, err := db.ZUnionStore("stored", sources, ZAggregateMin); err != nil || count != 4 {
		t.Fatalf("ZUnionStore() = (%d, %v)", count, err)
	}
}

func TestZSetPopsRandomAndRangeStore(t *testing.T) {
	db := NewDB()
	_, _ = db.ZAddMany("source", ZAddOptions{},
		ZSetItem{Member: "one", Score: 1}, ZSetItem{Member: "two", Score: 2}, ZSetItem{Member: "three", Score: 3},
	)
	db.Set("wrong", []byte("value"))
	if _, _, _, err := db.ZMPop([]string{"source", "wrong"}, false, 1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ZMPop wrong-type error = %v", err)
	}
	if card, _ := db.ZCard("source"); card != 3 {
		t.Fatalf("failed ZMPop changed source cardinality to %d", card)
	}
	key, items, found, err := db.ZMPop([]string{"missing", "source"}, false, 2)
	if err != nil || !found || key != "source" || !reflect.DeepEqual(items, []ZSetItem{{Member: "one", Score: 1}, {Member: "two", Score: 2}}) {
		t.Fatalf("ZMPop() = (%q, %#v, %t, %v)", key, items, found, err)
	}
	random, err := db.ZRandMember("source", 5, true)
	if err != nil || len(random) != 1 || random[0].Member != "three" {
		t.Fatalf("distinct ZRandMember() = (%#v, %v)", random, err)
	}
	random, err = db.ZRandMember("source", 5, false)
	if err != nil || len(random) != 5 {
		t.Fatalf("replacement ZRandMember() = (%#v, %v)", random, err)
	}
	query := ZRangeQuery{Mode: ZRangeRank, Start: 0, Stop: -1, Count: -1}
	if count, err := db.ZRangeStoreFrom("wrong", "source", query); err != nil || count != 1 {
		t.Fatalf("ZRangeStoreFrom() = (%d, %v)", count, err)
	}
	if kind := db.Type("wrong"); kind != KindSortedSet {
		t.Fatalf("range store destination type = %s", kind)
	}
}

func itemScore(items []ZSetItem, member string) float64 {
	for _, item := range items {
		if item.Member == member {
			return item.Score
		}
	}
	return 0
}

func BenchmarkZSetAddAndRange(b *testing.B) {
	for _, size := range []int{100, 10_000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			db := NewDB()
			for index := 0; index < size; index++ {
				_, _ = db.ZAdd("zset", float64(index), strconv.Itoa(index))
			}
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				_, _ = db.ZRangeByRank("zset", int64(size/4), int64(size/4+99), false)
			}
		})
	}
}
