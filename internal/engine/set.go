package engine

import (
	"math/rand/v2"
	"sort"
)

type stringSet map[string]struct{}

func (db *DB) SAdd(key string, members ...string) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SAdd(key, members...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	if len(members) == 0 {
		return 0, nil
	}
	e, ok := db.entries[key]
	if !ok {
		e = &entry{kind: KindSet, value: make(stringSet)}
		db.entries[key] = e
	}
	if e.kind != KindSet {
		return 0, ErrWrongType
	}
	set := e.value.(stringSet)
	added := 0
	for _, member := range members {
		if _, exists := set[member]; exists {
			continue
		}
		set[member] = struct{}{}
		added++
	}
	return added, nil
}

func (db *DB) SRem(key string, members ...string) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SRem(key, members...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return 0, nil
	}
	if e.kind != KindSet {
		return 0, ErrWrongType
	}
	set := e.value.(stringSet)
	removed := 0
	for _, member := range members {
		if _, exists := set[member]; !exists {
			continue
		}
		delete(set, member)
		delete(db.expiringSetMembers, setMemberRef{key: key, member: member})
		removed++
	}
	if len(set) == 0 {
		db.removeEntryLocked(key)
	}
	return removed, nil
}

func (db *DB) SIsMember(key, member string) (bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SIsMember(key, member)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return false, nil
	}
	if e.kind != KindSet {
		return false, ErrWrongType
	}
	_, found := e.value.(stringSet)[member]
	return found, nil
}

func (db *DB) SMIsMember(key string, members ...string) ([]bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SMIsMember(key, members...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	results := make([]bool, len(members))
	e, ok := db.entries[key]
	if !ok {
		return results, nil
	}
	if e.kind != KindSet {
		return nil, ErrWrongType
	}
	set := e.value.(stringSet)
	for i, member := range members {
		_, results[i] = set[member]
	}
	return results, nil
}

func (db *DB) SMove(source, destination, member string) (int, error) {
	if db.shards != nil {
		var result int
		var err error
		db.withView([]string{source, destination}, func(view *DB) {
			result, err = view.SMove(source, destination, member)
		})
		return result, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	sourceEntry, sourceExists := db.entries[source]
	if sourceExists && sourceEntry.kind != KindSet {
		return 0, ErrWrongType
	}
	destinationEntry, destinationExists := db.entries[destination]
	if destinationExists && destinationEntry.kind != KindSet {
		return 0, ErrWrongType
	}
	if !sourceExists {
		return 0, nil
	}
	sourceSet := sourceEntry.value.(stringSet)
	if _, exists := sourceSet[member]; !exists {
		return 0, nil
	}
	if source == destination {
		return 1, nil
	}
	delete(sourceSet, member)
	delete(db.expiringSetMembers, setMemberRef{key: source, member: member})
	if !destinationExists {
		destinationEntry = &entry{kind: KindSet, value: make(stringSet)}
		db.entries[destination] = destinationEntry
	}
	destinationEntry.value.(stringSet)[member] = struct{}{}
	if len(sourceSet) == 0 {
		db.removeEntryLocked(source)
	}
	return 1, nil
}

func (db *DB) SCard(key string) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SCard(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return 0, nil
	}
	if e.kind != KindSet {
		return 0, ErrWrongType
	}
	return len(e.value.(stringSet)), nil
}

func (db *DB) SMembers(key string) ([]string, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SMembers(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return []string{}, nil
	}
	if e.kind != KindSet {
		return nil, ErrWrongType
	}
	return setMembers(e.value.(stringSet)), nil
}

func (db *DB) SPop(key string, count int64) ([]string, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SPop(key, count)
	}
	if count < 0 {
		return nil, ErrInvalidInteger
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return []string{}, nil
	}
	if e.kind != KindSet {
		return nil, ErrWrongType
	}
	if count == 0 {
		return []string{}, nil
	}
	set := e.value.(stringSet)
	members := setMembers(set)
	take := len(members)
	if count < int64(take) {
		take = int(count)
	}
	selected := randomDistinct(members, take)
	for _, member := range selected {
		delete(set, member)
		delete(db.expiringSetMembers, setMemberRef{key: key, member: member})
	}
	if len(set) == 0 {
		db.removeEntryLocked(key)
	}
	return selected, nil
}

func (db *DB) SRandMembers(key string, count int64) ([]string, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SRandMembers(key, count)
	}
	db.mu.Lock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		db.mu.Unlock()
		return []string{}, nil
	}
	if e.kind != KindSet {
		db.mu.Unlock()
		return nil, ErrWrongType
	}
	members := setMembers(e.value.(stringSet))
	db.mu.Unlock()
	if count >= 0 {
		take := len(members)
		if count < int64(take) {
			take = int(count)
		}
		return randomDistinct(members, take), nil
	}
	magnitude := uint64(-(count + 1)) + 1
	if magnitude > uint64(^uint(0)>>1) {
		return nil, ErrInvalidInteger
	}
	result := make([]string, int(magnitude))
	for i := range result {
		result[i] = members[rand.IntN(len(members))]
	}
	return result, nil
}

func (db *DB) SScan(key string, cursor uint64, options ScanOptions) (uint64, []string, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SScan(key, cursor, options)
	}
	count := options.Count
	if count == 0 {
		count = defaultScanCount
	}
	db.mu.Lock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		db.mu.Unlock()
		return 0, []string{}, nil
	}
	if e.kind != KindSet {
		db.mu.Unlock()
		return 0, nil, ErrWrongType
	}
	members := setMembers(e.value.(stringSet))
	db.mu.Unlock()
	sort.Strings(members)
	if cursor >= uint64(len(members)) {
		return 0, []string{}, nil
	}
	start := int(cursor)
	end := start + count
	if end > len(members) {
		end = len(members)
	}
	page := members[start:end]
	if options.UseMatch {
		filtered := make([]string, 0, len(page))
		for _, member := range page {
			if globMatch(options.Match, member) {
				filtered = append(filtered, member)
			}
		}
		page = filtered
	}
	next := uint64(end)
	if end == len(members) {
		next = 0
	}
	return next, page, nil
}

func (db *DB) SUnion(keys ...string) ([]string, error) {
	return db.setAlgebra(keys, unionSets)
}

func (db *DB) SInter(keys ...string) ([]string, error) {
	return db.setAlgebra(keys, intersectSets)
}

func (db *DB) SDiff(keys ...string) ([]string, error) {
	return db.setAlgebra(keys, differenceSets)
}

func (db *DB) setAlgebra(keys []string, operation func([]stringSet) stringSet) ([]string, error) {
	if db.shards != nil {
		var result []string
		var err error
		db.withView(keys, func(view *DB) { result, err = view.setAlgebra(keys, operation) })
		return result, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	sets, err := db.getSets(keys)
	if err != nil {
		return nil, err
	}
	return setMembers(operation(sets)), nil
}

func (db *DB) SUnionStore(destination string, keys ...string) (int, error) {
	return db.storeSetAlgebra(destination, keys, unionSets)
}

func (db *DB) SInterStore(destination string, keys ...string) (int, error) {
	return db.storeSetAlgebra(destination, keys, intersectSets)
}

func (db *DB) SDiffStore(destination string, keys ...string) (int, error) {
	return db.storeSetAlgebra(destination, keys, differenceSets)
}

func (db *DB) storeSetAlgebra(destination string, keys []string, operation func([]stringSet) stringSet) (int, error) {
	if db.shards != nil {
		allKeys := append([]string{destination}, keys...)
		var result int
		var err error
		db.withView(allKeys, func(view *DB) {
			result, err = view.storeSetAlgebra(destination, keys, operation)
		})
		return result, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	sets, err := db.getSets(keys)
	if err != nil {
		return 0, err
	}
	result := operation(sets)
	db.removeEntryLocked(destination)
	if len(result) == 0 {
		return 0, nil
	}
	db.entries[destination] = &entry{kind: KindSet, value: result}
	return len(result), nil
}

func (db *DB) getSets(keys []string) ([]stringSet, error) {
	sets := make([]stringSet, len(keys))
	for i, key := range keys {
		e, ok := db.entries[key]
		if !ok {
			continue
		}
		if e.kind != KindSet {
			return nil, ErrWrongType
		}
		sets[i] = e.value.(stringSet)
	}
	return sets, nil
}

func unionSets(sets []stringSet) stringSet {
	result := make(stringSet)
	for _, set := range sets {
		for member := range set {
			result[member] = struct{}{}
		}
	}
	return result
}

func intersectSets(sets []stringSet) stringSet {
	result := make(stringSet)
	if len(sets) == 0 {
		return result
	}
	smallest := sets[0]
	for _, set := range sets[1:] {
		if len(set) < len(smallest) {
			smallest = set
		}
	}
	for member := range smallest {
		present := true
		for _, set := range sets {
			if _, exists := set[member]; !exists {
				present = false
				break
			}
		}
		if present {
			result[member] = struct{}{}
		}
	}
	return result
}

func differenceSets(sets []stringSet) stringSet {
	result := make(stringSet)
	if len(sets) == 0 {
		return result
	}
	for member := range sets[0] {
		result[member] = struct{}{}
	}
	for _, set := range sets[1:] {
		for member := range set {
			delete(result, member)
		}
	}
	return result
}

func setMembers(set stringSet) []string {
	members := make([]string, 0, len(set))
	for member := range set {
		members = append(members, member)
	}
	return members
}

func randomDistinct(members []string, count int) []string {
	for i := 0; i < count; i++ {
		j := i + rand.IntN(len(members)-i)
		members[i], members[j] = members[j], members[i]
	}
	return members[:count]
}
