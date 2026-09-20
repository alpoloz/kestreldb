package engine

import (
	"bytes"
	"sort"
)

// Rename moves source to destination, replacing destination unless nx is set.
// The returned boolean reports whether the rename occurred.
func (db *DB) Rename(source, destination string, nx bool) (bool, error) {
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		var renamed bool
		var err error
		db.withView([]string{source, destination}, func(view *DB) {
			renamed, err = view.Rename(source, destination, nx)
		})
		if renamed && source != destination {
			db.serveShardedWaitersLocked(destination)
		}
		return renamed, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	sourceEntry, exists := db.entries[source]
	if !exists {
		return false, ErrNoSuchKey
	}
	if source == destination {
		return !nx, nil
	}
	if nx {
		if _, exists := db.entries[destination]; exists {
			return false, nil
		}
	}

	keyDeadline := db.expiring[source]
	hashDeadlines := make(map[string]int64)
	for ref, deadline := range db.expiringHashFields {
		if ref.key == source {
			hashDeadlines[ref.field] = deadline
		}
	}
	setDeadlines := make(map[string]int64)
	for ref, deadline := range db.expiringSetMembers {
		if ref.key == source {
			setDeadlines[ref.member] = deadline
		}
	}

	db.removeEntryLocked(destination)
	db.removeEntryLocked(source)
	db.entries[destination] = sourceEntry
	if keyDeadline > 0 {
		db.expiring[destination] = keyDeadline
	}
	for field, deadline := range hashDeadlines {
		db.expiringHashFields[hashFieldRef{key: destination, field: field}] = deadline
	}
	for member, deadline := range setDeadlines {
		db.expiringSetMembers[setMemberRef{key: destination, member: member}] = deadline
	}
	return true, nil
}

// Copy duplicates source at destination. Existing destinations are retained
// unless replace is true. Values and expiration metadata are deep-copied.
func (db *DB) Copy(source, destination string, replace bool) (bool, error) {
	if source == destination {
		return false, ErrSameKey
	}
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		var copied bool
		var err error
		db.withView([]string{source, destination}, func(view *DB) {
			copied, err = view.Copy(source, destination, replace)
		})
		if copied {
			db.serveShardedWaitersLocked(destination)
		}
		return copied, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	sourceEntry, exists := db.entries[source]
	if !exists {
		return false, nil
	}
	if _, exists := db.entries[destination]; exists && !replace {
		return false, nil
	}

	db.removeEntryLocked(destination)
	db.entries[destination] = cloneEntry(sourceEntry)
	if deadline := db.expiring[source]; deadline > 0 {
		db.expiring[destination] = deadline
	}
	for ref, deadline := range db.expiringHashFields {
		if ref.key == source {
			db.expiringHashFields[hashFieldRef{key: destination, field: ref.field}] = deadline
		}
	}
	for ref, deadline := range db.expiringSetMembers {
		if ref.key == source {
			db.expiringSetMembers[setMemberRef{key: destination, member: ref.member}] = deadline
		}
	}
	return true, nil
}

func cloneEntry(source *entry) *entry {
	cloned := &entry{kind: source.kind, expireAt: source.expireAt, lastAccess: source.lastAccess}
	switch source.kind {
	case KindString:
		cloned.value = bytes.Clone(source.value.([]byte))
	case KindHash:
		value := source.value.(map[string]string)
		copyValue := make(map[string]string, len(value))
		for field, item := range value {
			copyValue[field] = item
		}
		cloned.value = copyValue
	case KindList:
		cloned.value = newDeque(source.value.(*deque).values()...)
	case KindSet:
		value := source.value.(stringSet)
		copyValue := make(stringSet, len(value))
		for member := range value {
			copyValue[member] = struct{}{}
		}
		cloned.value = copyValue
	case KindSortedSet:
		copyValue := newZSet()
		for _, item := range source.value.(*zset).items() {
			copyValue.add(item.Score, item.Member)
		}
		cloned.value = copyValue
	}
	return cloned
}

// Touch updates access metadata and returns the number of existing keys.
// Repeated keys are counted repeatedly, matching Redis semantics.
func (db *DB) Touch(keys ...string) int {
	if db.shards != nil {
		var touched int
		db.withView(keys, func(view *DB) { touched = view.Touch(keys...) })
		return touched
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	now := db.now().UnixMilli()
	touched := 0
	for _, key := range keys {
		if e, exists := db.entries[key]; exists {
			e.lastAccess = now
			touched++
		}
	}
	return touched
}

// DBSize returns the number of live keys across the database.
func (db *DB) DBSize() int {
	if db.shards == nil {
		db.mu.Lock()
		defer db.mu.Unlock()
		db.purgeExpiredLocked()
		return len(db.entries)
	}
	release := db.operationGate()
	defer release()
	for _, shard := range db.shards {
		shard.mu.Lock()
	}
	size := 0
	for _, shard := range db.shards {
		shard.purgeExpiredLocked()
		size += len(shard.entries)
	}
	for index := len(db.shards) - 1; index >= 0; index-- {
		db.shards[index].mu.Unlock()
	}
	return size
}

// FlushDB removes all keys and expiration indexes from the database.
func (db *DB) FlushDB() {
	if db.shards == nil {
		db.mu.Lock()
		db.resetKeyspaceLocked()
		db.mu.Unlock()
		return
	}
	release := db.operationGate()
	defer release()
	for _, shard := range db.shards {
		shard.mu.Lock()
	}
	for _, shard := range db.shards {
		shard.resetKeyspaceLocked()
	}
	for index := len(db.shards) - 1; index >= 0; index-- {
		db.shards[index].mu.Unlock()
	}
}

func (db *DB) resetKeyspaceLocked() {
	db.entries = make(map[string]*entry)
	db.expiring = make(map[string]int64)
	db.expiringHashFields = make(map[hashFieldRef]int64)
	db.expiringSetMembers = make(map[setMemberRef]int64)
}

// Keys returns all live keys matching pattern. Ordering is deterministic but
// is not part of the API contract.
func (db *DB) Keys(pattern string) []string {
	entries := db.keyspaceEntries()
	keys := make([]string, 0, len(entries))
	for _, item := range entries {
		if globMatch(pattern, item.key) {
			keys = append(keys, item.key)
		}
	}
	return keys
}

// Scan incrementally examines the sorted live keyspace. Count is a work hint:
// filters may make the returned page smaller, including empty with a nonzero
// next cursor.
func (db *DB) Scan(cursor uint64, options ScanOptions) (uint64, []string) {
	entries := db.keyspaceEntries()
	if cursor >= uint64(len(entries)) {
		return 0, []string{}
	}
	count := options.Count
	if count == 0 {
		count = defaultScanCount
	}
	start := int(cursor)
	end := start + count
	if end > len(entries) {
		end = len(entries)
	}
	keys := make([]string, 0, end-start)
	for _, item := range entries[start:end] {
		if options.UseMatch && !globMatch(options.Match, item.key) {
			continue
		}
		if options.UseType && item.kind.String() != options.Type {
			continue
		}
		keys = append(keys, item.key)
	}
	next := uint64(end)
	if end == len(entries) {
		next = 0
	}
	return next, keys
}

type keyspaceEntry struct {
	key  string
	kind Kind
}

func (db *DB) keyspaceEntries() []keyspaceEntry {
	if db.shards == nil {
		db.mu.Lock()
		defer db.mu.Unlock()
		db.purgeExpiredLocked()
		return sortedKeyspaceEntries(db.entries)
	}
	release := db.operationGate()
	defer release()
	for _, shard := range db.shards {
		shard.mu.Lock()
	}
	entries := make([]keyspaceEntry, 0)
	for _, shard := range db.shards {
		shard.purgeExpiredLocked()
		for key, value := range shard.entries {
			entries = append(entries, keyspaceEntry{key: key, kind: value.kind})
		}
	}
	for index := len(db.shards) - 1; index >= 0; index-- {
		db.shards[index].mu.Unlock()
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	return entries
}

func sortedKeyspaceEntries(entries map[string]*entry) []keyspaceEntry {
	result := make([]keyspaceEntry, 0, len(entries))
	for key, value := range entries {
		result = append(result, keyspaceEntry{key: key, kind: value.kind})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].key < result[j].key })
	return result
}
