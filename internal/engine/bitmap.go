package engine

import (
	"bytes"
	"errors"
	"math/bits"
)

const maxBitmapBytes = 512 << 20

var ErrBitmapRange = errors.New("bit offset is out of range")

func bitmapBit(value []byte, offset int64) int {
	if offset < 0 || offset/8 >= int64(len(value)) {
		return 0
	}
	if value[offset/8]&(1<<(7-uint(offset%8))) != 0 {
		return 1
	}
	return 0
}

func bitmapSet(value []byte, offset int64, bit int) []byte {
	needed := int(offset/8 + 1)
	if needed > len(value) {
		value = append(value, make([]byte, needed-len(value))...)
	}
	mask := byte(1 << (7 - uint(offset%8)))
	if bit == 1 {
		value[offset/8] |= mask
	} else {
		value[offset/8] &^= mask
	}
	return value
}

func validBitOffset(offset int64) bool { return offset >= 0 && offset/8 < maxBitmapBytes }

func (db *DB) GetBit(key string, offset int64) (int, error) {
	if !validBitOffset(offset) {
		return 0, ErrBitmapRange
	}
	value, found, err := db.Get(key)
	if err != nil || !found {
		return 0, err
	}
	return bitmapBit(value, offset), nil
}

func (db *DB) SetBit(key string, offset int64, bit int) (int, error) {
	if !validBitOffset(offset) || bit != 0 && bit != 1 {
		return 0, ErrBitmapRange
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).SetBit(key, offset, bit)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entryLocked(key)
	if exists && e.kind != KindString {
		return 0, ErrWrongType
	}
	if !exists {
		e = &entry{kind: KindString, value: []byte{}}
		db.entries[key] = e
	}
	old := e.value.([]byte)
	previous := bitmapBit(old, offset)
	e.value = bitmapSet(bytes.Clone(old), offset, bit)
	return previous, nil
}

func normalizeBitmapRange(start, end, length int64) (int64, int64) {
	if start < 0 {
		start += length
	}
	if end < 0 {
		end += length
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	return start, end
}

func (db *DB) BitCount(key string, start, end int64, hasRange, bitUnit bool) (int, error) {
	value, found, err := db.Get(key)
	if err != nil || !found {
		return 0, err
	}
	if !hasRange {
		start = 0
		end = int64(len(value))*8 - 1
		bitUnit = true
	}
	length := int64(len(value))
	if bitUnit {
		length *= 8
	}
	start, end = normalizeBitmapRange(start, end, length)
	if start > end {
		return 0, nil
	}
	if !bitUnit {
		start *= 8
		end = end*8 + 7
	}
	count := 0
	for start <= end && start%8 != 0 {
		count += bitmapBit(value, start)
		start++
	}
	for start+7 <= end {
		count += bits.OnesCount8(value[start/8])
		start += 8
	}
	for i := start; i <= end; i++ {
		count += bitmapBit(value, i)
	}
	return count, nil
}

func (db *DB) BitPos(key string, bit int, start, end int64, hasStart, hasEnd, bitUnit bool) (int64, error) {
	if bit != 0 && bit != 1 {
		return 0, ErrBitmapRange
	}
	value, found, err := db.Get(key)
	if err != nil {
		return 0, err
	}
	if !found {
		if bit == 0 {
			return 0, nil
		}
		return -1, nil
	}
	if len(value) == 0 && bit == 0 && !hasEnd {
		return 0, nil
	}
	length := int64(len(value))
	if bitUnit {
		length *= 8
	}
	if !hasStart {
		start = 0
	}
	if !hasEnd {
		end = length - 1
	}
	start, end = normalizeBitmapRange(start, end, length)
	if start > end {
		return -1, nil
	}
	if !bitUnit {
		start *= 8
		end = end*8 + 7
	}
	for i := start; i <= end; i++ {
		if bitmapBit(value, i) == bit {
			return i, nil
		}
	}
	if bit == 0 && !hasEnd {
		return int64(len(value)) * 8, nil
	}
	return -1, nil
}

func (db *DB) BitOp(operation, destination string, sources ...string) (int, error) {
	if len(sources) == 0 || operation == "NOT" && len(sources) != 1 || operation != "NOT" && operation != "AND" && operation != "OR" && operation != "XOR" {
		return 0, ErrInvalidOptions
	}
	if db.shards != nil {
		keys := append([]string{destination}, sources...)
		var length int
		var err error
		db.withView(keys, func(view *DB) { length, err = view.BitOp(operation, destination, sources...) })
		return length, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	inputs := make([][]byte, len(sources))
	maximum := 0
	for i, key := range sources {
		e, found := db.entryLocked(key)
		if !found {
			continue
		}
		if e.kind != KindString {
			return 0, ErrWrongType
		}
		inputs[i] = e.value.([]byte)
		if len(inputs[i]) > maximum {
			maximum = len(inputs[i])
		}
	}
	result := make([]byte, maximum)
	for i := range result {
		get := func(j int) byte {
			if i < len(inputs[j]) {
				return inputs[j][i]
			}
			return 0
		}
		result[i] = get(0)
		switch operation {
		case "NOT":
			result[i] = ^result[i]
		case "AND":
			for j := 1; j < len(inputs); j++ {
				result[i] &= get(j)
			}
		case "OR":
			for j := 1; j < len(inputs); j++ {
				result[i] |= get(j)
			}
		case "XOR":
			for j := 1; j < len(inputs); j++ {
				result[i] ^= get(j)
			}
		default:
			return 0, ErrInvalidOptions
		}
	}
	db.removeEntryLocked(destination)
	db.entries[destination] = &entry{kind: KindString, value: result}
	return len(result), nil
}

type BitFieldOp struct {
	Kind     string
	Signed   bool
	Width    int
	Offset   int64
	Value    int64
	Overflow string
}
type BitFieldResult struct {
	Value int64
	Nil   bool
}

func bitFieldRead(data []byte, op BitFieldOp) int64 {
	var result uint64
	for i := 0; i < op.Width; i++ {
		result = result<<1 | uint64(bitmapBit(data, op.Offset+int64(i)))
	}
	if op.Signed && op.Width < 64 && result&(uint64(1)<<uint(op.Width-1)) != 0 {
		result |= ^uint64(0) << uint(op.Width)
	}
	return int64(result)
}

func bitFieldWrite(data []byte, op BitFieldOp, value int64) []byte {
	end := op.Offset + int64(op.Width) - 1
	needed := int(end/8 + 1)
	if needed > len(data) {
		data = append(data, make([]byte, needed-len(data))...)
	}
	for i := 0; i < op.Width; i++ {
		bit := int(uint64(value) >> uint(op.Width-i-1) & 1)
		data = bitmapSet(data, op.Offset+int64(i), bit)
	}
	return data
}

func bitFieldFits(value int64, signed bool, width int) bool {
	if width == 64 {
		return signed
	}
	if signed {
		minimum := -(int64(1) << uint(width-1))
		maximum := (int64(1) << uint(width-1)) - 1
		return value >= minimum && value <= maximum
	}
	return value >= 0 && uint64(value) < uint64(1)<<uint(width)
}

func bitFieldSaturation(signed bool, width int, positive bool) int64 {
	if positive {
		if signed {
			return (int64(1) << uint(width-1)) - 1
		}
		return (int64(1) << uint(width)) - 1
	}
	if signed {
		return -(int64(1) << uint(width-1))
	}
	return 0
}

func (db *DB) BitField(key string, ops []BitFieldOp, readonly bool) ([]BitFieldResult, error) {
	for _, op := range ops {
		if op.Width < 1 || op.Width > 64 || !validBitOffset(op.Offset) || op.Offset+int64(op.Width)-1 >= maxBitmapBytes*8 {
			return nil, ErrBitmapRange
		}
		if !op.Signed && op.Width == 64 {
			return nil, ErrInvalidOptions
		}
		if readonly && op.Kind != "GET" {
			return nil, ErrInvalidOptions
		}
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).BitField(key, ops, readonly)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entryLocked(key)
	if exists && e.kind != KindString {
		return nil, ErrWrongType
	}
	var data []byte
	if exists {
		data = bytes.Clone(e.value.([]byte))
	}
	results := make([]BitFieldResult, 0, len(ops))
	changed := false
	for _, op := range ops {
		old := bitFieldRead(data, op)
		switch op.Kind {
		case "GET":
			results = append(results, BitFieldResult{Value: old})
		case "SET":
			value := op.Value
			if !bitFieldFits(value, op.Signed, op.Width) {
				if op.Overflow == "FAIL" {
					results = append(results, BitFieldResult{Nil: true})
					continue
				}
				if op.Overflow == "SAT" {
					value = bitFieldSaturation(op.Signed, op.Width, value > 0)
				}
			}
			data = bitFieldWrite(data, op, value)
			results = append(results, BitFieldResult{Value: old})
			changed = true
		case "INCRBY":
			value := old + op.Value
			overflow := op.Value > 0 && value < old || op.Value < 0 && value > old || !bitFieldFits(value, op.Signed, op.Width)
			if overflow {
				switch op.Overflow {
				case "FAIL":
					results = append(results, BitFieldResult{Nil: true})
					continue
				case "SAT":
					value = bitFieldSaturation(op.Signed, op.Width, op.Value > 0)
				}
			}
			data = bitFieldWrite(data, op, value)
			results = append(results, BitFieldResult{Value: bitFieldRead(data, op)})
			changed = true
		default:
			return nil, ErrInvalidOptions
		}
	}
	if changed {
		if !exists {
			db.entries[key] = &entry{kind: KindString, value: data}
		} else {
			e.value = data
		}
	}
	return results, nil
}
