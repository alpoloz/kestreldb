package engine

import (
	"math"
	"sort"
	"strconv"
)

type HashPair struct {
	Field string
	Value string
}

type HashResult struct {
	Value string
	Found bool
}

func (db *DB) HSet(key, field, value string) (int, error) {
	return db.HSetMany(key, HashPair{Field: field, Value: value})
}

func (db *DB) HSetMany(key string, pairs ...HashPair) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HSetMany(key, pairs...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		if len(pairs) == 0 {
			return 0, nil
		}
		e = &entry{kind: KindHash, value: make(map[string]string, len(pairs))}
		db.entries[key] = e
	}
	if e.kind != KindHash {
		return 0, ErrWrongType
	}
	h := e.value.(map[string]string)
	added := 0
	for _, pair := range pairs {
		if _, exists := h[pair.Field]; !exists {
			added++
		}
		h[pair.Field] = pair.Value
		delete(db.expiringHashFields, hashFieldRef{key: key, field: pair.Field})
	}
	return added, nil
}

func (db *DB) HGet(key, field string) (string, bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HGet(key, field)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return "", false, nil
	}
	if e.kind != KindHash {
		return "", false, ErrWrongType
	}
	value, found := e.value.(map[string]string)[field]
	return value, found, nil
}

func (db *DB) HMGet(key string, fields ...string) ([]HashResult, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HMGet(key, fields...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	results := make([]HashResult, len(fields))
	e, ok := db.entries[key]
	if !ok {
		return results, nil
	}
	if e.kind != KindHash {
		return nil, ErrWrongType
	}
	h := e.value.(map[string]string)
	for i, field := range fields {
		value, found := h[field]
		results[i] = HashResult{Value: value, Found: found}
	}
	return results, nil
}

func (db *DB) HDel(key string, fields ...string) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HDel(key, fields...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return 0, nil
	}
	if e.kind != KindHash {
		return 0, ErrWrongType
	}
	h := e.value.(map[string]string)
	removed := 0
	for _, field := range fields {
		if _, exists := h[field]; !exists {
			continue
		}
		delete(h, field)
		delete(db.expiringHashFields, hashFieldRef{key: key, field: field})
		removed++
	}
	if len(h) == 0 {
		db.removeEntryLocked(key)
	}
	return removed, nil
}

func (db *DB) HLen(key string) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HLen(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return 0, nil
	}
	if e.kind != KindHash {
		return 0, ErrWrongType
	}
	return len(e.value.(map[string]string)), nil
}

func (db *DB) HExists(key, field string) (bool, error) {
	_, found, err := db.HGet(key, field)
	return found, err
}

func (db *DB) HStrLen(key, field string) (int, error) {
	value, found, err := db.HGet(key, field)
	if err != nil || !found {
		return 0, err
	}
	return len(value), nil
}

func (db *DB) HKeys(key string) ([]string, error) {
	pairs, err := db.hashPairs(key)
	if err != nil {
		return nil, err
	}
	fields := make([]string, len(pairs))
	for i, pair := range pairs {
		fields[i] = pair.Field
	}
	return fields, nil
}

func (db *DB) HVals(key string) ([]string, error) {
	pairs, err := db.hashPairs(key)
	if err != nil {
		return nil, err
	}
	values := make([]string, len(pairs))
	for i, pair := range pairs {
		values[i] = pair.Value
	}
	return values, nil
}

func (db *DB) HSetNX(key, field, value string) (bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HSetNX(key, field, value)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		db.entries[key] = &entry{kind: KindHash, value: map[string]string{field: value}}
		return true, nil
	}
	if e.kind != KindHash {
		return false, ErrWrongType
	}
	h := e.value.(map[string]string)
	if _, exists := h[field]; exists {
		return false, nil
	}
	h[field] = value
	delete(db.expiringHashFields, hashFieldRef{key: key, field: field})
	return true, nil
}

func (db *DB) HIncrBy(key, field string, increment int64) (int64, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HIncrBy(key, field, increment)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	h, current, found, err := db.hashNumericFieldLocked(key, field)
	if err != nil {
		return 0, err
	}
	var number int64
	if found {
		number, err = strconv.ParseInt(current, 10, 64)
		if err != nil {
			return 0, ErrInvalidInteger
		}
	}
	if increment > 0 && number > maxInt64-increment || increment < 0 && number < minInt64-increment {
		return 0, ErrInvalidInteger
	}
	result := number + increment
	h[field] = strconv.FormatInt(result, 10)
	return result, nil
}

func (db *DB) HIncrByFloat(key, field string, increment float64) (string, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HIncrByFloat(key, field, increment)
	}
	if math.IsNaN(increment) || math.IsInf(increment, 0) {
		return "", ErrInvalidFloat
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	h, current, found, err := db.hashNumericFieldLocked(key, field)
	if err != nil {
		return "", err
	}
	var number float64
	if found {
		number, err = strconv.ParseFloat(current, 64)
		if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
			return "", ErrInvalidFloat
		}
	}
	result := number + increment
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return "", ErrInvalidFloat
	}
	if result == 0 {
		result = 0
	}
	formatted := strconv.FormatFloat(result, 'f', -1, 64)
	h[field] = formatted
	return formatted, nil
}

func (db *DB) HScan(key string, cursor uint64, options ScanOptions) (uint64, []HashPair, error) {
	pairs, err := db.hashPairs(key)
	if err != nil {
		return 0, nil, err
	}
	if cursor >= uint64(len(pairs)) {
		return 0, []HashPair{}, nil
	}
	count := options.Count
	if count == 0 {
		count = defaultScanCount
	}
	start := int(cursor)
	end := start + count
	if end > len(pairs) {
		end = len(pairs)
	}
	result := make([]HashPair, 0, end-start)
	for _, pair := range pairs[start:end] {
		if !options.UseMatch || globMatch(options.Match, pair.Field) {
			result = append(result, pair)
		}
	}
	next := uint64(end)
	if end == len(pairs) {
		next = 0
	}
	return next, result, nil
}

func (db *DB) HGetAll(key string) (map[string]string, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).HGetAll(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return map[string]string{}, nil
	}
	if e.kind != KindHash {
		return nil, ErrWrongType
	}
	h := e.value.(map[string]string)
	out := make(map[string]string, len(h))
	for field, value := range h {
		out[field] = value
	}
	return out, nil
}

func (db *DB) hashPairs(key string) ([]HashPair, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).hashPairs(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return []HashPair{}, nil
	}
	if e.kind != KindHash {
		return nil, ErrWrongType
	}
	h := e.value.(map[string]string)
	pairs := make([]HashPair, 0, len(h))
	for field, value := range h {
		pairs = append(pairs, HashPair{Field: field, Value: value})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Field < pairs[j].Field })
	return pairs, nil
}

func (db *DB) hashNumericFieldLocked(key, field string) (map[string]string, string, bool, error) {
	e, ok := db.entries[key]
	if !ok {
		h := make(map[string]string)
		db.entries[key] = &entry{kind: KindHash, value: h}
		return h, "", false, nil
	}
	if e.kind != KindHash {
		return nil, "", false, ErrWrongType
	}
	h := e.value.(map[string]string)
	value, found := h[field]
	return h, value, found, nil
}
