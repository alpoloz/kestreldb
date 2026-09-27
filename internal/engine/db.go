package engine

import (
	"sync"
	"time"
)

const defaultShardCount = 64

// DB is the top-level in-memory store. Public databases own a fixed set of
// shards; the shard DBs themselves contain one canonical keyspace each.
type DB struct {
	gate               sync.RWMutex
	waitMu             sync.Mutex
	shards             []*DB
	mu                 dbMutex
	entries            map[string]*entry
	listWaiters        map[string][]*listWaiter
	streamWaiters      map[string][]*streamWaiter
	waitersClosed      bool
	now                func() time.Time
	expiring           map[string]int64
	expiringHashFields map[hashFieldRef]int64
	expiringSetMembers map[setMemberRef]int64
	expirationTurn     uint8
	expirationShard    uint32
	expirationStop     chan struct{}
	expirationDone     chan struct{}
	journal            *mutationJournal
	aof                *aofFile
}

func NewDB() *DB {
	return NewDBWithClock(time.Now)
}

func NewDBWithClock(now func() time.Time) *DB {
	if now == nil {
		now = time.Now
	}
	db := newCoreDB(now)
	db.shards = make([]*DB, defaultShardCount)
	for i := range db.shards {
		db.shards[i] = newCoreDB(now)
	}
	return db
}

func newCoreDB(now func() time.Time) *DB {
	if now == nil {
		now = time.Now
	}
	db := &DB{
		entries:            make(map[string]*entry),
		listWaiters:        make(map[string][]*listWaiter),
		streamWaiters:      make(map[string][]*streamWaiter),
		now:                now,
		expiring:           make(map[string]int64),
		expiringHashFields: make(map[hashFieldRef]int64),
		expiringSetMembers: make(map[setMemberRef]int64),
	}
	db.mu.owner = db
	return db
}

// Type returns the kind stored at key, or KindNone when the key does not exist.
func (db *DB) Type(key string) Kind {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).Type(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entryLocked(key)
	if !ok {
		return KindNone
	}
	return e.kind
}

// Del removes keys regardless of their value type and returns the number of
// keys that existed.
func (db *DB) Del(keys ...string) int {
	if db.shards != nil {
		var removed int
		db.withView(keys, func(view *DB) { removed = view.Del(keys...) })
		return removed
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	removed := 0
	for _, key := range keys {
		if db.deleteLocked(key) {
			removed++
		}
	}
	return removed
}

// Exists returns how many of keys exist. Repeated keys are counted repeatedly,
// matching Redis EXISTS semantics.
func (db *DB) Exists(keys ...string) int {
	if db.shards != nil {
		var found int
		db.withView(keys, func(view *DB) { found = view.Exists(keys...) })
		return found
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	found := 0
	for _, key := range keys {
		if _, ok := db.entryLocked(key); ok {
			found++
		}
	}
	return found
}

func (db *DB) entryLocked(key string) (*entry, bool) {
	e, ok := db.entries[key]
	if !ok {
		return nil, false
	}
	if e.expireAt > 0 && e.expireAt <= db.now().UnixMilli() {
		db.removeEntryLocked(key)
		return nil, false
	}
	return e, true
}

func (db *DB) deleteLocked(key string) bool {
	if _, ok := db.entryLocked(key); !ok {
		return false
	}
	db.removeEntryLocked(key)
	return true
}

func (db *DB) removeEntryLocked(key string) {
	delete(db.entries, key)
	delete(db.expiring, key)
	for ref := range db.expiringHashFields {
		if ref.key == key {
			delete(db.expiringHashFields, ref)
		}
	}
	for ref := range db.expiringSetMembers {
		if ref.key == key {
			delete(db.expiringSetMembers, ref)
		}
	}
}
