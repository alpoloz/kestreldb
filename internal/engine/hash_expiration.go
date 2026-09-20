package engine

import "time"

// HExpireAt applies one absolute deadline to each field. Results use Redis
// hash-expiration status codes: -2 missing, 0 condition rejected, 1 deadline
// set, and 2 deleted immediately.
func (db *DB) HExpireAt(key string, fields []string, when time.Time, options ExpireOptions) ([]int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HExpireAt(key, fields, when, options)
	}
	if !options.valid() {
		return nil, ErrInvalidOptions
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	results := make([]int, len(fields))
	e, ok := db.entries[key]
	if !ok {
		for i := range results {
			results[i] = -2
		}
		return results, nil
	}
	if e.kind != KindHash {
		return nil, ErrWrongType
	}
	h := e.value.(map[string]string)
	deadline := when.UnixMilli()
	now := db.now().UnixMilli()
	for i, field := range fields {
		if _, exists := h[field]; !exists {
			results[i] = -2
			continue
		}
		ref := hashFieldRef{key: key, field: field}
		current, hasExpiration := db.expiringHashFields[ref]
		if options.NX && hasExpiration || options.XX && !hasExpiration ||
			options.GT && (!hasExpiration || deadline <= current) ||
			options.LT && hasExpiration && deadline >= current {
			continue
		}
		if deadline <= now {
			delete(h, field)
			delete(db.expiringHashFields, ref)
			results[i] = 2
			continue
		}
		db.expiringHashFields[ref] = deadline
		results[i] = 1
	}
	if len(h) == 0 {
		db.removeEntryLocked(key)
	}
	return results, nil
}

func (db *DB) HFieldTTL(key string, fields []string, milliseconds, absolute bool) ([]int64, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HFieldTTL(key, fields, milliseconds, absolute)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	results := make([]int64, len(fields))
	e, ok := db.entries[key]
	if !ok {
		for i := range results {
			results[i] = -2
		}
		return results, nil
	}
	if e.kind != KindHash {
		return nil, ErrWrongType
	}
	h := e.value.(map[string]string)
	now := db.now().UnixMilli()
	for i, field := range fields {
		if _, exists := h[field]; !exists {
			results[i] = -2
			continue
		}
		deadline, exists := db.expiringHashFields[hashFieldRef{key: key, field: field}]
		if !exists {
			results[i] = -1
			continue
		}
		value := deadline
		if !absolute {
			value -= now
		}
		if !milliseconds {
			value /= 1000
		}
		results[i] = value
	}
	return results, nil
}

func (db *DB) HPersist(key string, fields []string) ([]int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HPersist(key, fields)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	results := make([]int, len(fields))
	e, ok := db.entries[key]
	if !ok {
		for i := range results {
			results[i] = -2
		}
		return results, nil
	}
	if e.kind != KindHash {
		return nil, ErrWrongType
	}
	h := e.value.(map[string]string)
	for i, field := range fields {
		if _, exists := h[field]; !exists {
			results[i] = -2
			continue
		}
		ref := hashFieldRef{key: key, field: field}
		if _, exists := db.expiringHashFields[ref]; !exists {
			continue
		}
		delete(db.expiringHashFields, ref)
		results[i] = 1
	}
	return results, nil
}
