package engine

import "sync"

type DB struct {
	mu     sync.RWMutex
	hashes map[string]map[string]string
	zsets  map[string]*ZSet
}

func NewDB() *DB {
	return &DB{
		hashes: make(map[string]map[string]string),
		zsets:  make(map[string]*ZSet),
	}
}

func (db *DB) HSet(key string, field string, value string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	h, ok := db.hashes[key]
	if !ok {
		h = make(map[string]string)
		db.hashes[key] = h
	}
	_, exists := h[field]
	h[field] = value
	if exists {
		return 0
	}
	return 1
}

func (db *DB) HGet(key string, field string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	h, ok := db.hashes[key]
	if !ok {
		return "", false
	}
	v, ok := h[field]
	return v, ok
}

func (db *DB) HDel(key string, field string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	h, ok := db.hashes[key]
	if !ok {
		return 0
	}
	if _, exists := h[field]; !exists {
		return 0
	}
	delete(h, field)
	if len(h) == 0 {
		delete(db.hashes, key)
	}
	return 1
}

func (db *DB) HLen(key string) int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	h, ok := db.hashes[key]
	if !ok {
		return 0
	}
	return len(h)
}

func (db *DB) HGetAll(key string) map[string]string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	h, ok := db.hashes[key]
	if !ok {
		return map[string]string{}
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}

func (db *DB) ZAdd(key string, score float64, member string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	zs, ok := db.zsets[key]
	if !ok {
		zs = NewZSet()
		db.zsets[key] = zs
	}
	if zs.Add(score, member) {
		return 1
	}
	return 0
}

func (db *DB) ZRem(key string, member string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	zs, ok := db.zsets[key]
	if !ok {
		return 0
	}
	if !zs.Remove(member) {
		return 0
	}
	if zs.Len() == 0 {
		delete(db.zsets, key)
	}
	return 1
}

func (db *DB) ZScore(key string, member string) (float64, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	zs, ok := db.zsets[key]
	if !ok {
		return 0, false
	}
	return zs.Score(member)
}

func (db *DB) ZCard(key string) int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	zs, ok := db.zsets[key]
	if !ok {
		return 0
	}
	return zs.Len()
}

func (db *DB) ZRange(key string, start int, stop int) []ZSetItem {
	db.mu.RLock()
	defer db.mu.RUnlock()

	zs, ok := db.zsets[key]
	if !ok {
		return nil
	}
	return zs.Range(start, stop)
}
