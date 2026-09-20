package engine

import "sort"

func (db *DB) shardIndex(key string) int {
	var hash uint64 = 14695981039346656037
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= 1099511628211
	}
	return int(hash % uint64(len(db.shards)))
}

func (db *DB) shardFor(key string) *DB { return db.shards[db.shardIndex(key)] }

// operationGate serializes mutations only while a full-state journal is
// active. In-memory operations hold the shared gate and acquire only the
// shards they touch. Journal mode currently trades parallelism for a single
// ordered stream of complete database images.
func (db *DB) operationGate() func() {
	db.gate.RLock()
	if db.journal == nil {
		return db.gate.RUnlock
	}
	db.gate.RUnlock()
	db.gate.Lock()
	db.mu.Lock()
	return func() { db.mu.Unlock(); db.gate.Unlock() }
}

// withView executes a multi-key command with all involved shard locks held in
// ascending shard order. The temporary view gives existing command code one
// typed keyspace for validation and commit. Only participating keys are copied
// back, as one atomic operation from other commands' perspective.
func (db *DB) withView(keys []string, run func(*DB)) {
	release := db.operationGate()
	defer release()
	seenKeys := make(map[string]struct{}, len(keys))
	idsSeen := make(map[int]struct{})
	ids := make([]int, 0)
	unique := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, ok := seenKeys[key]; ok {
			continue
		}
		seenKeys[key] = struct{}{}
		unique = append(unique, key)
		id := db.shardIndex(key)
		if _, ok := idsSeen[id]; !ok {
			idsSeen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	for _, id := range ids {
		db.shards[id].mu.Lock()
	}
	defer func() {
		for i := len(ids) - 1; i >= 0; i-- {
			db.shards[ids[i]].mu.Unlock()
		}
	}()
	view := newCoreDB(db.now)
	for _, key := range unique {
		shard := db.shardFor(key)
		if e, ok := shard.entries[key]; ok {
			view.entries[key] = e
		}
		if deadline, ok := shard.expiring[key]; ok {
			view.expiring[key] = deadline
		}
		for ref, deadline := range shard.expiringHashFields {
			if ref.key == key {
				view.expiringHashFields[ref] = deadline
			}
		}
		for ref, deadline := range shard.expiringSetMembers {
			if ref.key == key {
				view.expiringSetMembers[ref] = deadline
			}
		}
	}
	run(view)
	for _, key := range unique {
		shard := db.shardFor(key)
		delete(shard.entries, key)
		delete(shard.expiring, key)
		for ref := range shard.expiringHashFields {
			if ref.key == key {
				delete(shard.expiringHashFields, ref)
			}
		}
		for ref := range shard.expiringSetMembers {
			if ref.key == key {
				delete(shard.expiringSetMembers, ref)
			}
		}
		if e, ok := view.entries[key]; ok {
			shard.entries[key] = e
		}
		if deadline, ok := view.expiring[key]; ok {
			shard.expiring[key] = deadline
		}
		for ref, deadline := range view.expiringHashFields {
			if ref.key == key {
				shard.expiringHashFields[ref] = deadline
			}
		}
		for ref, deadline := range view.expiringSetMembers {
			if ref.key == key {
				shard.expiringSetMembers[ref] = deadline
			}
		}
	}
}

func stringPairKeys(pairs []StringPair) []string {
	keys := make([]string, len(pairs))
	for i, pair := range pairs {
		keys[i] = pair.Key
	}
	return keys
}

func zWeightedKeys(sources []ZWeightedKey) []string {
	keys := make([]string, len(sources))
	for i, source := range sources {
		keys[i] = source.Key
	}
	return keys
}
