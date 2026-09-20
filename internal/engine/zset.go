package engine

// ZSetItem is a public member+score pair returned by DB methods.
type ZSetItem struct {
	Member string
	Score  float64
}

// zset combines a hash map (O(1) score lookup) with a skip list (ordered iteration).
type zset struct {
	dict map[string]float64
	sl   *skipList
}

func newZSet() *zset {
	return &zset{
		dict: make(map[string]float64),
		sl:   newSkipList(),
	}
}

// add inserts or updates member. Returns true if the member is new.
func (z *zset) add(score float64, member string) bool {
	oldScore, exists := z.dict[member]
	if exists {
		if oldScore != score {
			z.sl.delete(oldScore, member)
			z.sl.insert(score, member)
			z.dict[member] = score
		}
		return false
	}
	z.dict[member] = score
	z.sl.insert(score, member)
	return true
}

func (z *zset) remove(member string) bool {
	score, exists := z.dict[member]
	if !exists {
		return false
	}
	delete(z.dict, member)
	z.sl.delete(score, member)
	return true
}

func (z *zset) score(member string) (float64, bool) {
	s, ok := z.dict[member]
	return s, ok
}

// rank returns the 0-indexed rank of member (ascending order).
func (z *zset) rank(member string) (int, bool) {
	score, exists := z.dict[member]
	if !exists {
		return 0, false
	}
	return z.sl.rankOf(score, member)
}

func (z *zset) card() int {
	return len(z.dict)
}

func (z *zset) rangeByRank(start, stop int) []ZSetItem {
	items := z.sl.rangeByRank(start, stop)
	result := make([]ZSetItem, len(items))
	for i, item := range items {
		result[i] = ZSetItem{Member: item.Member, Score: item.Score}
	}
	return result
}

func (z *zset) items() []ZSetItem {
	return z.rangeByRank(0, -1)
}

// ZAdd adds or updates member with score. Returns 1 if the member is new, 0 if updated.
func (db *DB) ZAdd(key string, score float64, member string) (int, error) {
	result, err := db.ZAddMany(key, ZAddOptions{}, ZSetItem{Member: member, Score: score})
	return result.Count, err
}

// ZRem removes member from the sorted set. Returns 1 if removed, 0 if not found.
func (db *DB) ZRem(key string, members ...string) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ZRem(key, members...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return 0, nil
	}
	if e.kind != KindSortedSet {
		return 0, ErrWrongType
	}

	z := e.value.(*zset)
	removed := 0
	for _, member := range members {
		if z.remove(member) {
			removed++
		}
	}
	if z.card() == 0 {
		db.removeEntryLocked(key)
	}
	return removed, nil
}

// ZScore returns the score of member, whether it was found, and any type error.
func (db *DB) ZScore(key, member string) (float64, bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ZScore(key, member)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return 0, false, nil
	}
	if e.kind != KindSortedSet {
		return 0, false, ErrWrongType
	}
	score, found := e.value.(*zset).score(member)
	return score, found, nil
}

// ZRank returns the 0-indexed rank of member (ascending by score), whether it
// was found, and any type error.
func (db *DB) ZRank(key, member string) (int, bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ZRank(key, member)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return 0, false, nil
	}
	if e.kind != KindSortedSet {
		return 0, false, ErrWrongType
	}
	rank, found := e.value.(*zset).rank(member)
	return rank, found, nil
}

// ZCard returns the number of members in the sorted set.
func (db *DB) ZCard(key string) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ZCard(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return 0, nil
	}
	if e.kind != KindSortedSet {
		return 0, ErrWrongType
	}
	return e.value.(*zset).card(), nil
}

// ZRange returns members in the inclusive [start, stop] rank range (0-indexed,
// negatives wrap).
func (db *DB) ZRange(key string, start, stop int) ([]ZSetItem, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ZRange(key, start, stop)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return []ZSetItem{}, nil
	}
	if e.kind != KindSortedSet {
		return nil, ErrWrongType
	}
	return e.value.(*zset).rangeByRank(start, stop), nil
}
