package engine

import (
	"bytes"
	"math"
	"strconv"
)

const (
	maxInt64 int64 = 1<<63 - 1
	minInt64 int64 = -1 << 63
)

type StringPair struct {
	Key   string
	Value []byte
}

type StringResult struct {
	Value []byte
	Found bool
}

type SetOptions struct {
	NX            bool
	XX            bool
	Get           bool
	KeepTTL       bool
	HasExpiration bool
	ExpireAt      int64
}

type SetResult struct {
	Previous      []byte
	PreviousFound bool
	Stored        bool
}

func (db *DB) Set(key string, value []byte) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		db.shardFor(key).Set(key, value)
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	db.setString(key, value)
}

func (db *DB) SetWithOptions(key string, value []byte, options SetOptions) (SetResult, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SetWithOptions(key, value, options)
	}
	var result SetResult
	if options.NX && options.XX || options.KeepTTL && options.HasExpiration {
		return result, ErrInvalidOptions
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entries[key]
	if options.Get && exists {
		if e.kind != KindString {
			return result, ErrWrongType
		}
		result.Previous = bytes.Clone(e.value.([]byte))
		result.PreviousFound = true
	}
	if options.NX && exists || options.XX && !exists {
		return result, nil
	}
	previousExpiration := int64(0)
	if exists && options.KeepTTL {
		previousExpiration = e.expireAt
	}
	db.setString(key, value)
	stored := db.entries[key]
	if options.HasExpiration {
		if options.ExpireAt <= db.now().UnixMilli() {
			db.removeEntryLocked(key)
		} else {
			stored.expireAt = options.ExpireAt
			db.expiring[key] = options.ExpireAt
		}
	} else if previousExpiration > 0 {
		stored.expireAt = previousExpiration
		db.expiring[key] = previousExpiration
	}
	result.Stored = true
	return result, nil
}

func (db *DB) SetNX(key string, value []byte) bool {
	result, _ := db.SetWithOptions(key, value, SetOptions{NX: true})
	return result.Stored
}

func (db *DB) GetSet(key string, value []byte) ([]byte, bool, error) {
	result, err := db.SetWithOptions(key, value, SetOptions{Get: true})
	return result.Previous, result.PreviousFound, err
}

func (db *DB) GetDel(key string) ([]byte, bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).GetDel(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return nil, false, nil
	}
	if e.kind != KindString {
		return nil, false, ErrWrongType
	}
	value := bytes.Clone(e.value.([]byte))
	db.removeEntryLocked(key)
	return value, true, nil
}

func (db *DB) Get(key string) ([]byte, bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).Get(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		return nil, false, nil
	}
	if e.kind != KindString {
		return nil, false, ErrWrongType
	}
	return bytes.Clone(e.value.([]byte)), true, nil
}

func (db *DB) MSet(pairs ...StringPair) {
	if db.shards != nil {
		db.withView(stringPairKeys(pairs), func(view *DB) { view.MSet(pairs...) })
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	for _, pair := range pairs {
		db.setString(pair.Key, pair.Value)
	}
}

func (db *DB) MSetNX(pairs ...StringPair) bool {
	if db.shards != nil {
		var stored bool
		db.withView(stringPairKeys(pairs), func(view *DB) { stored = view.MSetNX(pairs...) })
		return stored
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	for _, pair := range pairs {
		if _, exists := db.entries[pair.Key]; exists {
			return false
		}
	}
	for _, pair := range pairs {
		db.setString(pair.Key, pair.Value)
	}
	return true
}

func (db *DB) MGet(keys ...string) []StringResult {
	if db.shards != nil {
		var results []StringResult
		db.withView(keys, func(view *DB) { results = view.MGet(keys...) })
		return results
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	results := make([]StringResult, len(keys))
	for i, key := range keys {
		e, ok := db.entries[key]
		if !ok || e.kind != KindString {
			continue
		}
		results[i] = StringResult{Value: bytes.Clone(e.value.([]byte)), Found: true}
	}
	return results
}

func (db *DB) Append(key string, value []byte) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).Append(key, value)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, ok := db.entries[key]
	if !ok {
		db.setString(key, value)
		return len(value), nil
	}
	if e.kind != KindString {
		return 0, ErrWrongType
	}
	current := e.value.([]byte)
	combined := make([]byte, len(current)+len(value))
	copy(combined, current)
	copy(combined[len(current):], value)
	e.value = combined
	return len(combined), nil
}

func (db *DB) StrLen(key string) (int, error) {
	value, found, err := db.Get(key)
	if err != nil || !found {
		return 0, err
	}
	return len(value), nil
}

func (db *DB) GetRange(key string, start, stop int64) ([]byte, error) {
	value, found, err := db.Get(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return []byte{}, nil
	}
	first, last, ok := normalizeListRange(start, stop, len(value))
	if !ok {
		return []byte{}, nil
	}
	return bytes.Clone(value[first : last+1]), nil
}

func (db *DB) SetRange(key string, offset int64, value []byte) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SetRange(key, offset, value)
	}
	if offset < 0 || uint64(offset) > uint64(^uint(0)>>1) || len(value) > int(^uint(0)>>1)-int(offset) {
		return 0, ErrInvalidOffset
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entries[key]
	if exists && e.kind != KindString {
		return 0, ErrWrongType
	}
	if len(value) == 0 {
		if !exists {
			return 0, nil
		}
		return len(e.value.([]byte)), nil
	}
	var old []byte
	if exists {
		old = e.value.([]byte)
	}
	length := int(offset) + len(value)
	if length < len(old) {
		length = len(old)
	}
	updated := make([]byte, length)
	copy(updated, old)
	copy(updated[int(offset):], value)
	previousExpiration := int64(0)
	if exists {
		previousExpiration = e.expireAt
	}
	db.setString(key, updated)
	if previousExpiration > 0 {
		db.entries[key].expireAt = previousExpiration
		db.expiring[key] = previousExpiration
	}
	return length, nil
}

func (db *DB) IncrBy(key string, increment int64) (int64, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).IncrBy(key, increment)
	}
	return db.changeInteger(key, increment, false)
}

func (db *DB) DecrBy(key string, decrement int64) (int64, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).DecrBy(key, decrement)
	}
	return db.changeInteger(key, decrement, true)
}

func (db *DB) IncrByFloat(key string, increment float64) (string, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).IncrByFloat(key, increment)
	}
	if math.IsNaN(increment) || math.IsInf(increment, 0) {
		return "", ErrInvalidFloat
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	current := float64(0)
	e, ok := db.entries[key]
	if ok {
		if e.kind != KindString {
			return "", ErrWrongType
		}
		parsed, err := strconv.ParseFloat(string(e.value.([]byte)), 64)
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return "", ErrInvalidFloat
		}
		current = parsed
	}
	result := current + increment
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return "", ErrInvalidFloat
	}
	if result == 0 {
		result = 0
	}
	formatted := strconv.FormatFloat(result, 'f', -1, 64)
	expiration := int64(0)
	if ok {
		expiration = e.expireAt
	}
	db.setString(key, []byte(formatted))
	if expiration > 0 {
		db.entries[key].expireAt = expiration
		db.expiring[key] = expiration
	}
	return formatted, nil
}

func (db *DB) changeInteger(key string, operand int64, subtract bool) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	current := int64(0)
	e, ok := db.entries[key]
	if ok {
		if e.kind != KindString {
			return 0, ErrWrongType
		}
		parsed, err := strconv.ParseInt(string(e.value.([]byte)), 10, 64)
		if err != nil {
			return 0, ErrInvalidInteger
		}
		current = parsed
	}
	var result int64
	if subtract {
		if operand > 0 && current < minInt64+operand || operand < 0 && current > maxInt64+operand {
			return 0, ErrInvalidInteger
		}
		result = current - operand
	} else {
		if operand > 0 && current > maxInt64-operand || operand < 0 && current < minInt64-operand {
			return 0, ErrInvalidInteger
		}
		result = current + operand
	}
	expiration := int64(0)
	if ok {
		expiration = e.expireAt
	}
	db.setString(key, []byte(strconv.FormatInt(result, 10)))
	if expiration > 0 {
		db.entries[key].expireAt = expiration
		db.expiring[key] = expiration
	}
	return result, nil
}

func (db *DB) setString(key string, value []byte) {
	db.removeEntryLocked(key)
	db.entries[key] = &entry{kind: KindString, value: bytes.Clone(value)}
}
