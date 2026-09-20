package engine

import "time"

type setMemberRef struct {
	key    string
	member string
}

// SAddEx is Dragonfly's member-expiring set extension. It adds members like
// SADD and assigns or refreshes the same TTL on every supplied member.
func (db *DB) SAddEx(key string, ttl time.Duration, members ...string) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SAddEx(key, ttl, members...)
	}
	if ttl <= 0 {
		return 0, ErrInvalidInteger
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
	deadline := db.now().Add(ttl).UnixMilli()
	set := e.value.(stringSet)
	added := 0
	for _, member := range members {
		if _, exists := set[member]; !exists {
			set[member] = struct{}{}
			added++
		}
		db.expiringSetMembers[setMemberRef{key: key, member: member}] = deadline
	}
	return added, nil
}

func (db *DB) clearSetMemberExpirationsLocked(key string) {
	for ref := range db.expiringSetMembers {
		if ref.key == key {
			delete(db.expiringSetMembers, ref)
		}
	}
}
