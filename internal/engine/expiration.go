package engine

import "time"

type ExpireOptions struct {
	NX bool
	XX bool
	GT bool
	LT bool
}

type hashFieldRef struct {
	key   string
	field string
}

func (db *DB) NowUnixMilli() int64 { return db.now().UnixMilli() }

func (options ExpireOptions) valid() bool {
	count := 0
	for _, enabled := range []bool{options.NX, options.XX, options.GT, options.LT} {
		if enabled {
			count++
		}
	}
	return count <= 1
}

func (db *DB) ExpireAt(key string, when time.Time, options ExpireOptions) (bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ExpireAt(key, when, options)
	}
	if !options.valid() {
		return false, ErrInvalidOptions
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return false, nil
	}
	deadline := when.UnixMilli()
	hasExpiration := e.expireAt > 0
	if options.NX && hasExpiration || options.XX && !hasExpiration ||
		options.GT && (!hasExpiration || deadline <= e.expireAt) ||
		options.LT && hasExpiration && deadline >= e.expireAt {
		return false, nil
	}
	if deadline <= db.now().UnixMilli() {
		db.removeEntryLocked(key)
		return true, nil
	}
	e.expireAt = deadline
	db.expiring[key] = deadline
	return true, nil
}

func (db *DB) TTL(key string, milliseconds bool) int64 {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).TTL(key, milliseconds)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return -2
	}
	if e.expireAt == 0 {
		return -1
	}
	remaining := e.expireAt - db.now().UnixMilli()
	if milliseconds {
		return remaining
	}
	return remaining / 1000
}

func (db *DB) ExpireTime(key string, milliseconds bool) int64 {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).ExpireTime(key, milliseconds)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return -2
	}
	if e.expireAt == 0 {
		return -1
	}
	if milliseconds {
		return e.expireAt
	}
	return e.expireAt / 1000
}

func (db *DB) Persist(key string) bool {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).Persist(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok || e.expireAt == 0 {
		return false
	}
	e.expireAt = 0
	delete(db.expiring, key)
	return true
}

func (db *DB) StartExpiration(interval time.Duration, budget int) {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	if budget <= 0 {
		budget = 100
	}
	db.mu.Lock()
	if db.expirationStop != nil {
		db.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	db.expirationStop = stop
	db.expirationDone = done
	db.mu.Unlock()
	go db.expirationLoop(interval, budget, stop, done)
}

func (db *DB) StopExpiration() {
	db.mu.Lock()
	stop := db.expirationStop
	done := db.expirationDone
	if stop == nil {
		db.mu.Unlock()
		return
	}
	db.expirationStop = nil
	db.expirationDone = nil
	close(stop)
	db.mu.Unlock()
	<-done
}

func (db *DB) expirationLoop(interval time.Duration, budget int, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			db.runExpirationCycle(budget)
		case <-stop:
			return
		}
	}
}

func (db *DB) runExpirationCycle(budget int) {
	if db.shards == nil {
		db.mu.Lock()
		db.expireCycleLocked(budget)
		db.mu.Unlock()
		return
	}
	release := db.operationGate()
	defer release()
	for i := 0; i < budget; i++ {
		index := int((db.expirationShard + uint32(i)) % uint32(len(db.shards)))
		shard := db.shards[index]
		shard.mu.Lock()
		shard.expireCycleLocked(1)
		shard.mu.Unlock()
	}
	db.expirationShard = (db.expirationShard + uint32(budget)) % uint32(len(db.shards))
}

func (db *DB) purgeExpiredLocked() {
	total := len(db.expiring) + len(db.expiringHashFields) + len(db.expiringSetMembers)
	db.expireCycleLocked(3 * total)
}

func (db *DB) expireCycleLocked(budget int) {
	now := db.now().UnixMilli()
	budgets := [3]int{}
	for assigned, attempts := 0, 0; assigned < budget && attempts < budget*3+3; attempts++ {
		category := int(db.expirationTurn % 3)
		db.expirationTurn++
		nonempty := category == 0 && len(db.expiring) > 0 ||
			category == 1 && len(db.expiringHashFields) > 0 ||
			category == 2 && len(db.expiringSetMembers) > 0
		if nonempty {
			budgets[category]++
			assigned++
		}
	}
	checked := 0
	for key, deadline := range db.expiring {
		if checked >= budgets[0] {
			break
		}
		checked++
		e, ok := db.entries[key]
		if !ok || e.expireAt != deadline {
			delete(db.expiring, key)
			continue
		}
		if deadline <= now {
			db.removeEntryLocked(key)
		}
	}
	checked = 0
	for ref, deadline := range db.expiringHashFields {
		if checked >= budgets[1] {
			break
		}
		checked++
		e, ok := db.entries[ref.key]
		if !ok || e.kind != KindHash {
			delete(db.expiringHashFields, ref)
			continue
		}
		h := e.value.(map[string]string)
		if _, exists := h[ref.field]; !exists {
			delete(db.expiringHashFields, ref)
			continue
		}
		if deadline <= now {
			delete(h, ref.field)
			delete(db.expiringHashFields, ref)
			if len(h) == 0 {
				db.removeEntryLocked(ref.key)
			}
		}
	}
	checked = 0
	for ref, deadline := range db.expiringSetMembers {
		if checked >= budgets[2] {
			break
		}
		checked++
		e, ok := db.entries[ref.key]
		if !ok || e.kind != KindSet {
			delete(db.expiringSetMembers, ref)
			continue
		}
		set := e.value.(stringSet)
		if _, exists := set[ref.member]; !exists {
			delete(db.expiringSetMembers, ref)
			continue
		}
		if deadline <= now {
			delete(set, ref.member)
			delete(db.expiringSetMembers, ref)
			if len(set) == 0 {
				db.removeEntryLocked(ref.key)
			}
		}
	}
}
