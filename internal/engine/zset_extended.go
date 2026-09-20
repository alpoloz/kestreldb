package engine

import (
	"math"
	"math/rand"
	"sort"
)

type ZAddOptions struct {
	NX, XX, GT, LT, CH, INCR bool
}

type ZAddResult struct {
	Count      int
	Score      float64
	ScoreFound bool
}

type ScoreBound struct {
	Value     float64
	Exclusive bool
}

type LexBound struct {
	Value     string
	Exclusive bool
	Infinite  int
}

type ZRangeMode uint8

const (
	ZRangeRank ZRangeMode = iota
	ZRangeScore
	ZRangeLex
)

type ZRangeQuery struct {
	Mode               ZRangeMode
	Start, Stop        int64
	MinScore, MaxScore ScoreBound
	MinLex, MaxLex     LexBound
	Reverse            bool
	Offset, Count      int64
}

type ZAggregate uint8

const (
	ZAggregateSum ZAggregate = iota
	ZAggregateMin
	ZAggregateMax
)

type ZWeightedKey struct {
	Key    string
	Weight float64
}

func (db *DB) ZAddMany(key string, options ZAddOptions, items ...ZSetItem) (ZAddResult, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ZAddMany(key, options, items...)
	}
	var result ZAddResult
	if options.NX && options.XX || options.GT && options.LT || options.NX && (options.GT || options.LT) || options.INCR && len(items) != 1 {
		return result, ErrInvalidOptions
	}
	for _, item := range items {
		if math.IsNaN(item.Score) {
			return result, ErrInvalidFloat
		}
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entries[key]
	if exists && e.kind != KindSortedSet {
		return result, ErrWrongType
	}
	z := newZSet()
	if exists {
		z = e.value.(*zset)
	}
	last := make(map[string]int, len(items))
	for i, item := range items {
		last[item.Member] = i
	}
	for i, item := range items {
		if last[item.Member] != i {
			continue
		}
		old, found := z.score(item.Member)
		if options.NX && found || options.XX && !found {
			continue
		}
		newScore := item.Score
		if options.INCR && found {
			newScore = old + item.Score
			if math.IsNaN(newScore) {
				return ZAddResult{}, ErrInvalidFloat
			}
		}
		if found && (options.GT && newScore <= old || options.LT && newScore >= old) {
			continue
		}
		if !found || old != newScore {
			if z.add(newScore, item.Member) || options.CH {
				result.Count++
			}
		}
		if options.INCR {
			result.Score = newScore
			result.ScoreFound = true
		}
	}
	if z.card() > 0 && !exists {
		db.entries[key] = &entry{kind: KindSortedSet, value: z}
	}
	return result, nil
}

func (db *DB) ZMScore(key string, members ...string) ([]ZSetItem, []bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ZMScore(key, members...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	items := make([]ZSetItem, len(members))
	found := make([]bool, len(members))
	e, ok := db.entries[key]
	if !ok {
		return items, found, nil
	}
	if e.kind != KindSortedSet {
		return nil, nil, ErrWrongType
	}
	z := e.value.(*zset)
	for i, member := range members {
		score, exists := z.score(member)
		items[i] = ZSetItem{Member: member, Score: score}
		found[i] = exists
	}
	return items, found, nil
}

func (db *DB) ZRevRank(key, member string) (int, bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ZRevRank(key, member)
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
	z := e.value.(*zset)
	rank, found := z.rank(member)
	if !found {
		return 0, false, nil
	}
	return z.card() - rank - 1, true, nil
}

func (db *DB) ZCount(key string, min, max ScoreBound) (int, error) {
	items, err := db.zsetItems(key)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, item := range items {
		if scoreWithin(item.Score, min, max) {
			count++
		}
	}
	return count, nil
}

func (db *DB) ZLexCount(key string, min, max LexBound) (int, error) {
	items, err := db.zsetItems(key)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, item := range items {
		if lexWithin(item.Member, min, max) {
			count++
		}
	}
	return count, nil
}

func (db *DB) ZRangeByRank(key string, start, stop int64, reverse bool) ([]ZSetItem, error) {
	return db.ZRangeQuery(key, ZRangeQuery{Mode: ZRangeRank, Start: start, Stop: stop, Reverse: reverse, Count: -1})
}

func (db *DB) ZRangeByScore(key string, min, max ScoreBound, reverse bool, offset, count int64) ([]ZSetItem, error) {
	return db.ZRangeQuery(key, ZRangeQuery{Mode: ZRangeScore, MinScore: min, MaxScore: max, Reverse: reverse, Offset: offset, Count: count})
}

func (db *DB) ZRangeByLex(key string, min, max LexBound, reverse bool, offset, count int64) ([]ZSetItem, error) {
	return db.ZRangeQuery(key, ZRangeQuery{Mode: ZRangeLex, MinLex: min, MaxLex: max, Reverse: reverse, Offset: offset, Count: count})
}

func (db *DB) ZRangeQuery(key string, query ZRangeQuery) ([]ZSetItem, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ZRangeQuery(key, query)
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
	return rangeZSet(e.value.(*zset), query), nil
}

func (db *DB) ZRangeStoreFrom(destination, source string, query ZRangeQuery) (int, error) {
	if db.shards != nil {
		var count int
		var err error
		db.withView([]string{destination, source}, func(view *DB) {
			count, err = view.ZRangeStoreFrom(destination, source, query)
		})
		return count, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[source]
	if ok && e.kind != KindSortedSet {
		return 0, ErrWrongType
	}
	var items []ZSetItem
	if ok {
		items = rangeZSet(e.value.(*zset), query)
	}
	db.removeEntryLocked(destination)
	if len(items) == 0 {
		return 0, nil
	}
	z := newZSet()
	for _, item := range items {
		z.add(item.Score, item.Member)
	}
	db.entries[destination] = &entry{kind: KindSortedSet, value: z}
	return z.card(), nil
}

func (db *DB) ZIncrBy(key, member string, increment float64) (float64, error) {
	result, err := db.ZAddMany(key, ZAddOptions{INCR: true}, ZSetItem{Member: member, Score: increment})
	if err != nil {
		return 0, err
	}
	return result.Score, nil
}

func (db *DB) ZRemRangeByRank(key string, start, stop int64) (int, error) {
	return db.removeZSetRange(key, ZRangeQuery{Mode: ZRangeRank, Start: start, Stop: stop, Count: -1})
}

func (db *DB) ZRemRangeByScore(key string, min, max ScoreBound) (int, error) {
	return db.removeZSetRange(key, ZRangeQuery{Mode: ZRangeScore, MinScore: min, MaxScore: max, Count: -1})
}

func (db *DB) ZRemRangeByLex(key string, min, max LexBound) (int, error) {
	return db.removeZSetRange(key, ZRangeQuery{Mode: ZRangeLex, MinLex: min, MaxLex: max, Count: -1})
}

func (db *DB) ZPop(key string, maximum bool, count int64) ([]ZSetItem, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ZPop(key, maximum, count)
	}
	if count < 0 {
		return nil, ErrInvalidInteger
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok || count == 0 {
		return []ZSetItem{}, nil
	}
	if e.kind != KindSortedSet {
		return nil, ErrWrongType
	}
	z := e.value.(*zset)
	items := z.items()
	if maximum {
		reverseZSetItems(items)
	}
	if int64(len(items)) > count {
		items = items[:count]
	}
	for _, item := range items {
		z.remove(item.Member)
	}
	if z.card() == 0 {
		db.removeEntryLocked(key)
	}
	return items, nil
}

func (db *DB) ZMPop(keys []string, maximum bool, count int64) (string, []ZSetItem, bool, error) {
	if db.shards != nil {
		var key string
		var items []ZSetItem
		var found bool
		var err error
		db.withView(keys, func(view *DB) { key, items, found, err = view.ZMPop(keys, maximum, count) })
		return key, items, found, err
	}
	if count <= 0 {
		return "", nil, false, ErrInvalidInteger
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	for _, key := range keys {
		if e, ok := db.entries[key]; ok && e.kind != KindSortedSet {
			return "", nil, false, ErrWrongType
		}
	}
	for _, key := range keys {
		e, ok := db.entries[key]
		if !ok {
			continue
		}
		z := e.value.(*zset)
		items := z.items()
		if maximum {
			reverseZSetItems(items)
		}
		if int64(len(items)) > count {
			items = items[:count]
		}
		for _, item := range items {
			z.remove(item.Member)
		}
		if z.card() == 0 {
			db.removeEntryLocked(key)
		}
		return key, items, true, nil
	}
	return "", nil, false, nil
}

func (db *DB) ZRandMember(key string, count int64, distinct bool) ([]ZSetItem, error) {
	if count < 0 {
		return nil, ErrInvalidInteger
	}
	items, err := db.zsetItems(key)
	if err != nil || len(items) == 0 || count == 0 {
		return items, err
	}
	if distinct {
		if count > int64(len(items)) {
			count = int64(len(items))
		}
		rand.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })
		return items[:count], nil
	}
	result := make([]ZSetItem, int(count))
	for i := range result {
		result[i] = items[rand.Intn(len(items))]
	}
	return result, nil
}

func (db *DB) ZScan(key string, cursor uint64, options ScanOptions) (uint64, []ZSetItem, error) {
	items, err := db.zsetItemsByMember(key)
	if err != nil {
		return 0, nil, err
	}
	if cursor >= uint64(len(items)) {
		return 0, []ZSetItem{}, nil
	}
	count := options.Count
	if count == 0 {
		count = defaultScanCount
	}
	start := int(cursor)
	end := start + count
	if end > len(items) {
		end = len(items)
	}
	result := make([]ZSetItem, 0, end-start)
	for _, item := range items[start:end] {
		if !options.UseMatch || globMatch(options.Match, item.Member) {
			result = append(result, item)
		}
	}
	next := uint64(end)
	if end == len(items) {
		next = 0
	}
	return next, result, nil
}

func (db *DB) ZUnion(sources []ZWeightedKey, aggregate ZAggregate) ([]ZSetItem, error) {
	return db.combineZSets(sources, aggregate, "union")
}

func (db *DB) ZInter(sources []ZWeightedKey, aggregate ZAggregate) ([]ZSetItem, error) {
	return db.combineZSets(sources, aggregate, "inter")
}

func (db *DB) ZDiff(keys ...string) ([]ZSetItem, error) {
	sources := make([]ZWeightedKey, len(keys))
	for i, key := range keys {
		sources[i] = ZWeightedKey{Key: key, Weight: 1}
	}
	return db.combineZSets(sources, ZAggregateSum, "diff")
}

func (db *DB) combineZSets(sources []ZWeightedKey, aggregate ZAggregate, operation string) ([]ZSetItem, error) {
	if db.shards != nil {
		var items []ZSetItem
		var err error
		db.withView(zWeightedKeys(sources), func(view *DB) {
			items, err = view.combineZSets(sources, aggregate, operation)
		})
		return items, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	return db.combineZSetsLocked(sources, aggregate, operation)
}

func (db *DB) ZInterCard(keys []string, limit int64) (int, error) {
	sources := make([]ZWeightedKey, len(keys))
	for i, key := range keys {
		sources[i] = ZWeightedKey{Key: key, Weight: 1}
	}
	items, err := db.combineZSets(sources, ZAggregateSum, "inter")
	if err != nil {
		return 0, err
	}
	if limit > 0 && int64(len(items)) > limit {
		return int(limit), nil
	}
	return len(items), nil
}

func (db *DB) ZUnionStore(destination string, sources []ZWeightedKey, aggregate ZAggregate) (int, error) {
	return db.storeCombinedZSet(destination, sources, aggregate, "union")
}

func (db *DB) ZInterStore(destination string, sources []ZWeightedKey, aggregate ZAggregate) (int, error) {
	return db.storeCombinedZSet(destination, sources, aggregate, "inter")
}

func (db *DB) ZDiffStore(destination string, keys ...string) (int, error) {
	sources := make([]ZWeightedKey, len(keys))
	for i, key := range keys {
		sources[i] = ZWeightedKey{Key: key, Weight: 1}
	}
	return db.storeCombinedZSet(destination, sources, ZAggregateSum, "diff")
}

func (db *DB) storeCombinedZSet(destination string, sources []ZWeightedKey, aggregate ZAggregate, operation string) (int, error) {
	if db.shards != nil {
		keys := append([]string{destination}, zWeightedKeys(sources)...)
		var count int
		var err error
		db.withView(keys, func(view *DB) {
			count, err = view.storeCombinedZSet(destination, sources, aggregate, operation)
		})
		return count, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	items, err := db.combineZSetsLocked(sources, aggregate, operation)
	if err != nil {
		return 0, err
	}
	db.removeEntryLocked(destination)
	if len(items) == 0 {
		return 0, nil
	}
	z := newZSet()
	for _, item := range items {
		z.add(item.Score, item.Member)
	}
	db.entries[destination] = &entry{kind: KindSortedSet, value: z}
	return len(items), nil
}

func (db *DB) combineZSetsLocked(sources []ZWeightedKey, aggregate ZAggregate, operation string) ([]ZSetItem, error) {
	if len(sources) == 0 {
		return []ZSetItem{}, nil
	}
	sets := make([]*zset, len(sources))
	for i, source := range sources {
		if math.IsNaN(source.Weight) {
			return nil, ErrInvalidFloat
		}
		e, ok := db.entries[source.Key]
		if !ok {
			continue
		}
		if e.kind != KindSortedSet {
			return nil, ErrWrongType
		}
		sets[i] = e.value.(*zset)
	}
	scores := make(map[string]float64)
	switch operation {
	case "union":
		for i, z := range sets {
			if z == nil {
				continue
			}
			for member, score := range z.dict {
				weighted := score * sources[i].Weight
				if current, found := scores[member]; found {
					scores[member] = aggregateScores(current, weighted, aggregate)
				} else {
					scores[member] = weighted
				}
			}
		}
	case "inter":
		if sets[0] == nil {
			return []ZSetItem{}, nil
		}
		for member, score := range sets[0].dict {
			combined := score * sources[0].Weight
			present := true
			for i := 1; i < len(sets); i++ {
				if sets[i] == nil {
					present = false
					break
				}
				next, found := sets[i].dict[member]
				if !found {
					present = false
					break
				}
				combined = aggregateScores(combined, next*sources[i].Weight, aggregate)
			}
			if present {
				scores[member] = combined
			}
		}
	case "diff":
		if sets[0] == nil {
			return []ZSetItem{}, nil
		}
		for member, score := range sets[0].dict {
			present := false
			for _, z := range sets[1:] {
				if z != nil {
					_, present = z.dict[member]
				}
				if present {
					break
				}
			}
			if !present {
				scores[member] = score
			}
		}
	default:
		return nil, ErrInvalidOptions
	}
	items := make([]ZSetItem, 0, len(scores))
	for member, score := range scores {
		if math.IsNaN(score) {
			score = 0
		}
		items = append(items, ZSetItem{Member: member, Score: score})
	}
	sort.Slice(items, func(i, j int) bool {
		return slLess(items[i].Score, items[i].Member, items[j].Score, items[j].Member)
	})
	return items, nil
}

func aggregateScores(first, second float64, aggregate ZAggregate) float64 {
	switch aggregate {
	case ZAggregateMin:
		return math.Min(first, second)
	case ZAggregateMax:
		return math.Max(first, second)
	default:
		result := first + second
		if math.IsNaN(result) {
			return 0
		}
		return result
	}
}

func (db *DB) zsetItems(key string) ([]ZSetItem, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).zsetItems(key)
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
	return e.value.(*zset).items(), nil
}

func (db *DB) zsetItemsByMember(key string) ([]ZSetItem, error) {
	items, err := db.zsetItems(key)
	if err == nil {
		sort.Slice(items, func(i, j int) bool { return items[i].Member < items[j].Member })
	}
	return items, err
}

func scoreWithin(score float64, min, max ScoreBound) bool {
	if score < min.Value || min.Exclusive && score == min.Value {
		return false
	}
	return score < max.Value || score == max.Value && !max.Exclusive
}

func (db *DB) removeZSetRange(key string, query ZRangeQuery) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).removeZSetRange(key, query)
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
	items := rangeZSet(z, query)
	for _, item := range items {
		z.remove(item.Member)
	}
	if z.card() == 0 {
		db.removeEntryLocked(key)
	}
	return len(items), nil
}

func lexWithin(member string, min, max LexBound) bool {
	lower := min.Infinite < 0 || member > min.Value || member == min.Value && !min.Exclusive
	upper := max.Infinite > 0 || member < max.Value || member == max.Value && !max.Exclusive
	return lower && upper
}

func reverseZSetItems(items []ZSetItem) {
	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
	}
}

func limitZSetItems(items []ZSetItem, offset, count int64) []ZSetItem {
	if offset < 0 || offset >= int64(len(items)) || count == 0 {
		return []ZSetItem{}
	}
	end := int64(len(items))
	if count > 0 && offset+count < end {
		end = offset + count
	}
	return append([]ZSetItem(nil), items[offset:end]...)
}

func rangeZSet(z *zset, query ZRangeQuery) []ZSetItem {
	items := z.items()
	switch query.Mode {
	case ZRangeRank:
		if query.Reverse {
			reverseZSetItems(items)
		}
		first, last, ok := normalizeListRange(query.Start, query.Stop, len(items))
		if !ok {
			return []ZSetItem{}
		}
		return append([]ZSetItem(nil), items[first:last+1]...)
	case ZRangeScore:
		filtered := make([]ZSetItem, 0, len(items))
		for _, item := range items {
			if scoreWithin(item.Score, query.MinScore, query.MaxScore) {
				filtered = append(filtered, item)
			}
		}
		if query.Reverse {
			reverseZSetItems(filtered)
		}
		return limitZSetItems(filtered, query.Offset, query.Count)
	case ZRangeLex:
		sort.Slice(items, func(i, j int) bool { return items[i].Member < items[j].Member })
		filtered := make([]ZSetItem, 0, len(items))
		for _, item := range items {
			if lexWithin(item.Member, query.MinLex, query.MaxLex) {
				filtered = append(filtered, item)
			}
		}
		if query.Reverse {
			reverseZSetItems(filtered)
		}
		return limitZSetItems(filtered, query.Offset, query.Count)
	default:
		return []ZSetItem{}
	}
}
