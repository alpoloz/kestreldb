package engine

import (
	"fmt"
	"sync"
	"testing"
)

func TestKeysLiveOnlyOnTheirSelectedShard(t *testing.T) {
	db := NewDB()
	used := make(map[int]bool)
	for i := 0; i < 256; i++ {
		key := fmt.Sprintf("key:%d", i)
		db.Set(key, []byte(key))
		used[db.shardIndex(key)] = true
	}
	if len(used) < 2 {
		t.Fatalf("256 keys used only %d shard", len(used))
	}
	if len(db.entries) != 0 {
		t.Fatalf("root keyspace contains %d entries", len(db.entries))
	}
	for i := 0; i < 256; i++ {
		key := fmt.Sprintf("key:%d", i)
		for shardIndex, shard := range db.shards {
			shard.mu.RLock()
			_, found := shard.entries[key]
			shard.mu.RUnlock()
			if found != (shardIndex == db.shardIndex(key)) {
				t.Fatalf("key %q placement on shard %d = %t", key, shardIndex, found)
			}
		}
	}
}

func TestConcurrentOperationsAcrossShards(t *testing.T) {
	db := NewDB()
	first, second := keysOnDifferentShards(db)
	const increments = 1_000
	var wg sync.WaitGroup
	for _, key := range []string{first, second} {
		key := key
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < increments; i++ {
				if _, err := db.IncrBy(key, 1); err != nil {
					t.Errorf("IncrBy(%q): %v", key, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for _, key := range []string{first, second} {
		value, found, err := db.Get(key)
		if err != nil || !found || string(value) != "1000" {
			t.Fatalf("Get(%q) = (%q, %t, %v)", key, value, found, err)
		}
	}
}

func TestCrossShardMultiKeyMutation(t *testing.T) {
	db := NewDB()
	source, destination := keysOnDifferentShards(db)
	if _, err := db.SAdd(source, "member"); err != nil {
		t.Fatal(err)
	}
	if moved, err := db.SMove(source, destination, "member"); err != nil || moved != 1 {
		t.Fatalf("SMove = (%d, %v)", moved, err)
	}
	if db.Type(source) != KindNone || db.Type(destination) != KindSet {
		t.Fatalf("post-move types = (%s, %s)", db.Type(source), db.Type(destination))
	}

	db.Set(source, []byte("occupied"))
	if db.MSetNX(
		StringPair{Key: destination, Value: []byte("replacement")},
		StringPair{Key: source, Value: []byte("changed")},
	) {
		t.Fatal("MSetNX unexpectedly committed")
	}
	if kind := db.Type(destination); kind != KindSet {
		t.Fatalf("failed MSetNX changed destination to %s", kind)
	}
}

func keysOnDifferentShards(db *DB) (string, string) {
	first := "shard-key:0"
	for i := 1; ; i++ {
		candidate := fmt.Sprintf("shard-key:%d", i)
		if db.shardIndex(candidate) != db.shardIndex(first) {
			return first, candidate
		}
	}
}

func BenchmarkShardedIncrements(b *testing.B) {
	for _, benchmark := range []struct {
		name string
		keys int
	}{
		{name: "hot-key", keys: 1},
		{name: "distributed", keys: defaultShardCount},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			db := NewDB()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Sprintf("counter:%d", i%benchmark.keys)
					_, _ = db.IncrBy(key, 1)
					i++
				}
			})
		})
	}
}
