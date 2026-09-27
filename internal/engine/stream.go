package engine

import (
	"math"
	"sort"
	"strconv"
)

// StreamID is the stable ordered identity of one stream entry.
type StreamID struct {
	Milliseconds uint64
	Sequence     uint64
}

func (id StreamID) String() string {
	return strconv.FormatUint(id.Milliseconds, 10) + "-" + strconv.FormatUint(id.Sequence, 10)
}

func (id StreamID) Compare(other StreamID) int {
	if id.Milliseconds < other.Milliseconds {
		return -1
	}
	if id.Milliseconds > other.Milliseconds {
		return 1
	}
	if id.Sequence < other.Sequence {
		return -1
	}
	if id.Sequence > other.Sequence {
		return 1
	}
	return 0
}

// StreamIDRequest describes an XADD ID. Auto generates the complete ID;
// AutoSequence generates only the sequence for Milliseconds.
type StreamIDRequest struct {
	Milliseconds uint64
	Sequence     uint64
	Auto         bool
	AutoSequence bool
}

// StreamField intentionally uses an ordered slice representation. Field names
// may repeat and both names and values are binary-safe Go strings.
type StreamField struct {
	Name  string
	Value string
}

type StreamEntry struct {
	ID     StreamID
	Fields []StreamField
}

type StreamReadRequest struct {
	Key       string
	After     StreamID
	UseLatest bool
	NewOnly   bool // XREADGROUP's > selector.
}

type StreamReadResult struct {
	Key     string
	Entries []StreamEntry
}

type StreamRangeBound struct {
	ID        StreamID
	Exclusive bool
	Unbounded int8 // -1 is negative infinity; +1 is positive infinity.
}

type StreamTrimMode uint8

const (
	StreamTrimMaxLen StreamTrimMode = iota
	StreamTrimMinID
)

type StreamTrimOptions struct {
	Mode        StreamTrimMode
	MaxLen      int64
	MinID       StreamID
	Approximate bool
	Limit       int64 // zero means unlimited.
}

type StreamAddOptions struct {
	NoMkStream bool
	Trim       *StreamTrimOptions
}

type streamValue struct {
	entries       []StreamEntry
	lastGenerated StreamID
	groups        map[string]*streamConsumerGroup
}

func newStreamValue() *streamValue {
	return &streamValue{groups: make(map[string]*streamConsumerGroup)}
}

func (s *streamValue) clone() *streamValue {
	cloned := &streamValue{
		entries:       cloneStreamEntries(s.entries),
		lastGenerated: s.lastGenerated,
		groups:        make(map[string]*streamConsumerGroup, len(s.groups)),
	}
	for name, group := range s.groups {
		cloned.groups[name] = group.clone()
	}
	return cloned
}

func cloneStreamEntries(entries []StreamEntry) []StreamEntry {
	cloned := make([]StreamEntry, len(entries))
	for index, item := range entries {
		cloned[index] = StreamEntry{ID: item.ID, Fields: append([]StreamField(nil), item.Fields...)}
	}
	return cloned
}

// XAdd appends an entry and returns its generated ID and whether it was added.
func (db *DB) XAdd(key string, request StreamIDRequest, fields []StreamField, options StreamAddOptions) (StreamID, bool, error) {
	if len(fields) == 0 {
		return StreamID{}, false, ErrInvalidOptions
	}
	if options.Trim != nil {
		if err := validateStreamTrim(*options.Trim); err != nil {
			return StreamID{}, false, err
		}
	}
	if db.shards != nil {
		db.waitMu.Lock()
		defer db.waitMu.Unlock()
		release := db.operationGate()
		id, added, err := db.shardFor(key).XAdd(key, request, fields, options)
		release()
		if err == nil && added {
			db.serveStreamWaitersLocked(key)
		}
		return id, added, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entries[key]
	if !exists {
		if options.NoMkStream {
			return StreamID{}, false, nil
		}
		e = &entry{kind: KindStream, value: newStreamValue()}
		db.entries[key] = e
	}
	if e.kind != KindStream {
		return StreamID{}, false, ErrWrongType
	}
	stream := e.value.(*streamValue)
	id, err := db.nextStreamID(stream.lastGenerated, request)
	if err != nil {
		return StreamID{}, false, err
	}
	stream.entries = append(stream.entries, StreamEntry{ID: id, Fields: append([]StreamField(nil), fields...)})
	stream.lastGenerated = id
	if options.Trim != nil {
		trimStreamLocked(stream, *options.Trim)
	}
	return id, true, nil
}

func (db *DB) nextStreamID(last StreamID, request StreamIDRequest) (StreamID, error) {
	if request.Auto && request.AutoSequence {
		return StreamID{}, ErrInvalidStreamID
	}
	var id StreamID
	switch {
	case request.Auto:
		now := db.now().UnixMilli()
		milliseconds := uint64(0)
		if now > 0 {
			milliseconds = uint64(now)
		}
		if milliseconds > last.Milliseconds {
			id = StreamID{Milliseconds: milliseconds}
		} else {
			if last.Sequence == math.MaxUint64 {
				return StreamID{}, ErrStreamIDOverflow
			}
			id = StreamID{Milliseconds: last.Milliseconds, Sequence: last.Sequence + 1}
		}
		if id == (StreamID{}) {
			id.Sequence = 1
		}
	case request.AutoSequence:
		id.Milliseconds = request.Milliseconds
		if id.Milliseconds < last.Milliseconds {
			return StreamID{}, ErrStreamIDTooSmall
		}
		if id.Milliseconds == last.Milliseconds {
			if last.Sequence == math.MaxUint64 {
				return StreamID{}, ErrStreamIDOverflow
			}
			id.Sequence = last.Sequence + 1
		} else if id.Milliseconds == 0 {
			id.Sequence = 1
		}
	default:
		id = StreamID{Milliseconds: request.Milliseconds, Sequence: request.Sequence}
	}
	if id == (StreamID{}) {
		return StreamID{}, ErrInvalidStreamID
	}
	if id.Compare(last) <= 0 {
		return StreamID{}, ErrStreamIDTooSmall
	}
	return id, nil
}

func (db *DB) XLen(key string) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XLen(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entries[key]
	if !exists {
		return 0, nil
	}
	if e.kind != KindStream {
		return 0, ErrWrongType
	}
	return len(e.value.(*streamValue).entries), nil
}

func (db *DB) XRange(key string, start, end StreamRangeBound, count int64, reverse bool) ([]StreamEntry, error) {
	if count < 0 {
		return nil, ErrInvalidInteger
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XRange(key, start, end, count, reverse)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entries[key]
	if !exists {
		return []StreamEntry{}, nil
	}
	if e.kind != KindStream {
		return nil, ErrWrongType
	}
	return streamRange(e.value.(*streamValue).entries, start, end, count, reverse), nil
}

func streamRange(entries []StreamEntry, start, end StreamRangeBound, count int64, reverse bool) []StreamEntry {
	if len(entries) == 0 {
		return []StreamEntry{}
	}
	lower := 0
	if start.Unbounded >= 0 {
		lower = sort.Search(len(entries), func(index int) bool {
			comparison := entries[index].ID.Compare(start.ID)
			return comparison > 0 || comparison == 0 && !start.Exclusive
		})
	}
	upper := len(entries)
	if end.Unbounded <= 0 {
		upper = sort.Search(len(entries), func(index int) bool {
			comparison := entries[index].ID.Compare(end.ID)
			return comparison > 0 || comparison == 0 && end.Exclusive
		})
	}
	if lower >= upper {
		return []StreamEntry{}
	}
	limit := upper - lower
	if count > 0 && int64(limit) > count {
		limit = int(count)
	}
	result := make([]StreamEntry, 0, limit)
	if reverse {
		for index := upper - 1; index >= lower && len(result) < limit; index-- {
			result = append(result, StreamEntry{ID: entries[index].ID, Fields: append([]StreamField(nil), entries[index].Fields...)})
		}
		return result
	}
	for index := lower; index < upper && len(result) < limit; index++ {
		result = append(result, StreamEntry{ID: entries[index].ID, Fields: append([]StreamField(nil), entries[index].Fields...)})
	}
	return result
}

func (db *DB) XDel(key string, ids ...StreamID) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XDel(key, ids...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entries[key]
	if !exists {
		return 0, nil
	}
	if e.kind != KindStream {
		return 0, ErrWrongType
	}
	requested := make(map[StreamID]struct{}, len(ids))
	for _, id := range ids {
		requested[id] = struct{}{}
	}
	stream := e.value.(*streamValue)
	kept := stream.entries[:0]
	removed := 0
	for _, item := range stream.entries {
		if _, ok := requested[item.ID]; ok {
			removed++
			continue
		}
		kept = append(kept, item)
	}
	stream.entries = compactStreamEntries(kept)
	return removed, nil
}

func (db *DB) XTrim(key string, options StreamTrimOptions) (int, error) {
	if err := validateStreamTrim(options); err != nil {
		return 0, err
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XTrim(key, options)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entries[key]
	if !exists {
		return 0, nil
	}
	if e.kind != KindStream {
		return 0, ErrWrongType
	}
	return trimStreamLocked(e.value.(*streamValue), options), nil
}

func validateStreamTrim(options StreamTrimOptions) error {
	if options.Limit < 0 || options.Mode == StreamTrimMaxLen && options.MaxLen < 0 {
		return ErrInvalidInteger
	}
	if options.Mode != StreamTrimMaxLen && options.Mode != StreamTrimMinID {
		return ErrInvalidOptions
	}
	return nil
}

func trimStreamLocked(stream *streamValue, options StreamTrimOptions) int {
	remove := 0
	switch options.Mode {
	case StreamTrimMaxLen:
		if int64(len(stream.entries)) > options.MaxLen {
			remove = len(stream.entries) - int(options.MaxLen)
		}
	case StreamTrimMinID:
		remove = sort.Search(len(stream.entries), func(index int) bool {
			return stream.entries[index].ID.Compare(options.MinID) >= 0
		})
	}
	if options.Limit > 0 && int64(remove) > options.Limit {
		remove = int(options.Limit)
	}
	if remove == 0 {
		return 0
	}
	stream.entries = compactStreamEntries(stream.entries[remove:])
	return remove
}

func compactStreamEntries(entries []StreamEntry) []StreamEntry {
	if len(entries) == 0 {
		return []StreamEntry{}
	}
	result := make([]StreamEntry, len(entries))
	copy(result, entries)
	return result
}

func (db *DB) XRead(requests []StreamReadRequest, count int64) ([]StreamReadResult, error) {
	if len(requests) == 0 {
		return nil, ErrNoKeys
	}
	if count < 0 {
		return nil, ErrInvalidInteger
	}
	if db.shards != nil {
		keys := streamRequestKeys(requests)
		var result []StreamReadResult
		var err error
		db.withView(keys, func(view *DB) { result, err = view.XRead(requests, count) })
		return result, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	return db.xReadLocked(requests, count)
}

func (db *DB) xReadLocked(requests []StreamReadRequest, count int64) ([]StreamReadResult, error) {
	result := make([]StreamReadResult, 0, len(requests))
	for _, request := range requests {
		e, exists := db.entries[request.Key]
		if !exists {
			continue
		}
		if e.kind != KindStream {
			return nil, ErrWrongType
		}
		stream := e.value.(*streamValue)
		after := request.After
		if request.UseLatest {
			after = stream.lastGenerated
		}
		start := StreamRangeBound{ID: after, Exclusive: true}
		end := StreamRangeBound{Unbounded: 1}
		entries := streamRange(stream.entries, start, end, count, false)
		if len(entries) > 0 {
			result = append(result, StreamReadResult{Key: request.Key, Entries: entries})
		}
	}
	return result, nil
}

func (db *DB) XLastID(key string) (StreamID, bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XLastID(key)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entries[key]
	if !exists {
		return StreamID{}, false, nil
	}
	if e.kind != KindStream {
		return StreamID{}, false, ErrWrongType
	}
	return e.value.(*streamValue).lastGenerated, true, nil
}

func streamRequestKeys(requests []StreamReadRequest) []string {
	keys := make([]string, len(requests))
	for index, request := range requests {
		keys[index] = request.Key
	}
	return keys
}
