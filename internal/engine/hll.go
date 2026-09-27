package engine

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"
	"math/bits"
	"sort"
)

const hllPrecision = 14
const hllRegisters = 1 << hllPrecision
const hllHeader = "KDBHLL1"
const hllDenseThreshold = 4096

var ErrInvalidHLL = errors.New("invalid HyperLogLog value")

func hllDecode(data []byte) ([]byte, error) {
	if len(data) < len(hllHeader)+1 || string(data[:len(hllHeader)]) != hllHeader {
		return nil, ErrInvalidHLL
	}
	registers := make([]byte, hllRegisters)
	switch data[len(hllHeader)] {
	case 0:
		payload := data[len(hllHeader)+1:]
		if len(payload)%3 != 0 {
			return nil, ErrInvalidHLL
		}
		previous := -1
		for len(payload) > 0 {
			index := int(binary.BigEndian.Uint16(payload[:2]))
			rank := payload[2]
			if index <= previous || index >= hllRegisters || rank == 0 || rank > 51 {
				return nil, ErrInvalidHLL
			}
			registers[index] = rank
			previous = index
			payload = payload[3:]
		}
	case 1:
		if len(data) != len(hllHeader)+1+hllRegisters {
			return nil, ErrInvalidHLL
		}
		copy(registers, data[len(hllHeader)+1:])
		for _, rank := range registers {
			if rank > 51 {
				return nil, ErrInvalidHLL
			}
		}
	default:
		return nil, ErrInvalidHLL
	}
	return registers, nil
}

func hllEncode(registers []byte) []byte {
	indices := make([]int, 0)
	for index, rank := range registers {
		if rank != 0 {
			indices = append(indices, index)
		}
	}
	if len(indices) > hllDenseThreshold {
		data := make([]byte, len(hllHeader)+1+hllRegisters)
		copy(data, hllHeader)
		data[len(hllHeader)] = 1
		copy(data[len(hllHeader)+1:], registers)
		return data
	}
	sort.Ints(indices)
	data := make([]byte, len(hllHeader)+1+len(indices)*3)
	copy(data, hllHeader)
	for i, index := range indices {
		offset := len(hllHeader) + 1 + i*3
		binary.BigEndian.PutUint16(data[offset:], uint16(index))
		data[offset+2] = registers[index]
	}
	return data
}

func hllAdd(registers []byte, value string) bool {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(value))
	hash := hasher.Sum64()
	index := int(hash & uint64(hllRegisters-1))
	remaining := hash >> hllPrecision
	rank := byte(bits.LeadingZeros64(remaining) - hllPrecision + 1)
	if rank > 51 {
		rank = 51
	}
	if rank > registers[index] {
		registers[index] = rank
		return true
	}
	return false
}

func hllCount(registers []byte) int {
	sum := 0.0
	zeros := 0
	for _, rank := range registers {
		sum += math.Ldexp(1, -int(rank))
		if rank == 0 {
			zeros++
		}
	}
	estimate := 0.7213 / (1 + 1.079/float64(hllRegisters)) * float64(hllRegisters*hllRegisters) / sum
	if estimate <= 2.5*hllRegisters && zeros > 0 {
		estimate = float64(hllRegisters) * math.Log(float64(hllRegisters)/float64(zeros))
	}
	return int(math.Round(estimate))
}

func (db *DB) PFAdd(key string, values ...string) (bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).PFAdd(key, values...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entryLocked(key)
	if exists && e.kind != KindString {
		return false, ErrWrongType
	}
	registers := make([]byte, hllRegisters)
	if exists {
		var err error
		registers, err = hllDecode(e.value.([]byte))
		if err != nil {
			return false, err
		}
	}
	changed := false
	for _, value := range values {
		if hllAdd(registers, value) {
			changed = true
		}
	}
	if !exists {
		db.entries[key] = &entry{kind: KindString, value: hllEncode(registers)}
	} else if changed {
		e.value = hllEncode(registers)
	}
	return changed, nil
}

func (db *DB) PFCount(keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, ErrNoKeys
	}
	if db.shards != nil {
		var count int
		var err error
		db.withView(keys, func(view *DB) { count, err = view.PFCount(keys...) })
		return count, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	merged := make([]byte, hllRegisters)
	for _, key := range keys {
		e, exists := db.entryLocked(key)
		if !exists {
			continue
		}
		if e.kind != KindString {
			return 0, ErrWrongType
		}
		registers, err := hllDecode(e.value.([]byte))
		if err != nil {
			return 0, err
		}
		for i, rank := range registers {
			if rank > merged[i] {
				merged[i] = rank
			}
		}
	}
	return hllCount(merged), nil
}

func (db *DB) PFMerge(destination string, sources ...string) error {
	if len(sources) == 0 {
		return ErrNoKeys
	}
	if db.shards != nil {
		keys := append([]string{destination}, sources...)
		var err error
		db.withView(keys, func(view *DB) { err = view.PFMerge(destination, sources...) })
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	merged := make([]byte, hllRegisters)
	keys := append([]string{destination}, sources...)
	for _, key := range keys {
		e, exists := db.entryLocked(key)
		if !exists {
			continue
		}
		if e.kind != KindString {
			return ErrWrongType
		}
		registers, err := hllDecode(e.value.([]byte))
		if err != nil {
			return err
		}
		for i, rank := range registers {
			if rank > merged[i] {
				merged[i] = rank
			}
		}
	}
	if e, exists := db.entries[destination]; exists {
		e.value = hllEncode(merged)
	} else {
		db.entries[destination] = &entry{kind: KindString, value: hllEncode(merged)}
	}
	return nil
}
