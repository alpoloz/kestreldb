package engine

import (
	"math"
	"sort"
)

type streamConsumerGroup struct {
	lastDelivered StreamID
	entriesRead   uint64
	consumers     map[string]*streamConsumer
	pending       map[StreamID]*streamPendingEntry
}

type streamConsumer struct {
	seenAt   int64
	activeAt int64
	pending  map[StreamID]struct{}
}

type streamPendingEntry struct {
	consumer    string
	deliveredAt int64
	deliveries  uint64
}

func newStreamConsumerGroup(start StreamID) *streamConsumerGroup {
	return &streamConsumerGroup{
		lastDelivered: start,
		consumers:     make(map[string]*streamConsumer),
		pending:       make(map[StreamID]*streamPendingEntry),
	}
}

func (group *streamConsumerGroup) clone() *streamConsumerGroup {
	cloned := &streamConsumerGroup{
		lastDelivered: group.lastDelivered,
		entriesRead:   group.entriesRead,
		consumers:     make(map[string]*streamConsumer, len(group.consumers)),
		pending:       make(map[StreamID]*streamPendingEntry, len(group.pending)),
	}
	for name, consumer := range group.consumers {
		pending := make(map[StreamID]struct{}, len(consumer.pending))
		for id := range consumer.pending {
			pending[id] = struct{}{}
		}
		cloned.consumers[name] = &streamConsumer{seenAt: consumer.seenAt, activeAt: consumer.activeAt, pending: pending}
	}
	for id, pending := range group.pending {
		copyValue := *pending
		cloned.pending[id] = &copyValue
	}
	return cloned
}

type StreamGroupReadOptions struct {
	Count int64
	NoAck bool
}

type StreamPendingSummary struct {
	Count     int
	Smallest  StreamID
	Greatest  StreamID
	Consumers map[string]int
}

type StreamPending struct {
	ID         StreamID
	Consumer   string
	IdleMillis int64
	Deliveries uint64
}

type StreamClaimOptions struct {
	IdleMillis *int64
	TimeMillis *int64
	RetryCount *uint64
	Force      bool
	JustID     bool
}

type StreamAutoClaimResult struct {
	Next    StreamID
	Entries []StreamEntry
	IDs     []StreamID
	Deleted []StreamID
}

func (db *DB) XGroupCreate(key, group string, start StreamID, useLast, mkstream bool) error {
	if group == "" {
		return ErrInvalidOptions
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XGroupCreate(key, group, start, useLast, mkstream)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entries[key]
	if !exists {
		if !mkstream {
			return ErrNoSuchKey
		}
		e = &entry{kind: KindStream, value: newStreamValue()}
		db.entries[key] = e
	}
	if e.kind != KindStream {
		return ErrWrongType
	}
	stream := e.value.(*streamValue)
	if _, exists := stream.groups[group]; exists {
		return ErrGroupExists
	}
	if useLast {
		start = stream.lastGenerated
	}
	stream.groups[group] = newStreamConsumerGroup(start)
	return nil
}

func (db *DB) XGroupDestroy(key, group string) (bool, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XGroupDestroy(key, group)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	e, exists := db.entries[key]
	if !exists {
		return false, nil
	}
	if e.kind != KindStream {
		return false, ErrWrongType
	}
	stream := e.value.(*streamValue)
	if _, exists := stream.groups[group]; !exists {
		return false, nil
	}
	delete(stream.groups, group)
	return true, nil
}

func (db *DB) XGroupSetID(key, group string, id StreamID, useLast bool) error {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XGroupSetID(key, group, id, useLast)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	stream, groupValue, err := db.streamGroupLocked(key, group)
	if err != nil {
		return err
	}
	if useLast {
		id = stream.lastGenerated
	}
	groupValue.lastDelivered = id
	return nil
}

func (db *DB) XGroupCreateConsumer(key, group, consumer string) (bool, error) {
	if consumer == "" {
		return false, ErrInvalidOptions
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XGroupCreateConsumer(key, group, consumer)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	_, groupValue, err := db.streamGroupLocked(key, group)
	if err != nil {
		return false, err
	}
	if _, exists := groupValue.consumers[consumer]; exists {
		return false, nil
	}
	now := db.now().UnixMilli()
	groupValue.consumers[consumer] = &streamConsumer{seenAt: now, activeAt: now, pending: make(map[StreamID]struct{})}
	return true, nil
}

func (db *DB) XGroupDelConsumer(key, group, consumer string) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XGroupDelConsumer(key, group, consumer)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	_, groupValue, err := db.streamGroupLocked(key, group)
	if err != nil {
		return 0, err
	}
	consumerValue, exists := groupValue.consumers[consumer]
	if !exists {
		return 0, nil
	}
	removed := len(consumerValue.pending)
	for id := range consumerValue.pending {
		delete(groupValue.pending, id)
	}
	delete(groupValue.consumers, consumer)
	return removed, nil
}

func (db *DB) XReadGroup(groupName, consumerName string, requests []StreamReadRequest, options StreamGroupReadOptions) ([]StreamReadResult, error) {
	if groupName == "" || consumerName == "" || len(requests) == 0 {
		return nil, ErrInvalidOptions
	}
	if options.Count < 0 {
		return nil, ErrInvalidInteger
	}
	if db.shards != nil {
		var result []StreamReadResult
		var err error
		db.withView(streamRequestKeys(requests), func(view *DB) {
			result, err = view.XReadGroup(groupName, consumerName, requests, options)
		})
		return result, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()

	// Validate every stream and group before assigning any entry.
	for _, request := range requests {
		e, exists := db.entries[request.Key]
		if !exists {
			return nil, ErrNoGroup
		}
		if e.kind != KindStream {
			return nil, ErrWrongType
		}
		if _, exists := e.value.(*streamValue).groups[groupName]; !exists {
			return nil, ErrNoGroup
		}
	}

	now := db.now().UnixMilli()
	result := make([]StreamReadResult, 0, len(requests))
	for _, request := range requests {
		stream := db.entries[request.Key].value.(*streamValue)
		group := stream.groups[groupName]
		consumer := ensureStreamConsumer(group, consumerName, now)
		consumer.seenAt = now
		var entries []StreamEntry
		if request.NewOnly {
			entries = streamRange(
				stream.entries,
				StreamRangeBound{ID: group.lastDelivered, Exclusive: true},
				StreamRangeBound{Unbounded: 1},
				options.Count,
				false,
			)
			if len(entries) > 0 {
				group.lastDelivered = entries[len(entries)-1].ID
				group.entriesRead += uint64(len(entries))
			}
			for _, item := range entries {
				if options.NoAck {
					continue
				}
				pending := group.pending[item.ID]
				if pending == nil {
					pending = &streamPendingEntry{deliveries: 1}
					group.pending[item.ID] = pending
				} else {
					if previous := group.consumers[pending.consumer]; previous != nil {
						delete(previous.pending, item.ID)
					}
					pending.deliveries++
				}
				pending.consumer = consumerName
				pending.deliveredAt = now
				consumer.pending[item.ID] = struct{}{}
			}
		} else {
			ids := sortedPendingIDs(consumer.pending)
			for _, id := range ids {
				if id.Compare(request.After) <= 0 {
					continue
				}
				entry, found := findStreamEntry(stream.entries, id)
				if !found {
					continue
				}
				pending := group.pending[id]
				pending.deliveredAt = now
				pending.deliveries++
				entries = append(entries, entry)
				if options.Count > 0 && int64(len(entries)) >= options.Count {
					break
				}
			}
		}
		if len(entries) > 0 {
			consumer.activeAt = now
			result = append(result, StreamReadResult{Key: request.Key, Entries: entries})
		}
	}
	return result, nil
}

func (db *DB) XAck(key, groupName string, ids ...StreamID) (int, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XAck(key, groupName, ids...)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	_, group, err := db.streamGroupLocked(key, groupName)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, id := range ids {
		pending, exists := group.pending[id]
		if !exists {
			continue
		}
		if consumer := group.consumers[pending.consumer]; consumer != nil {
			delete(consumer.pending, id)
		}
		delete(group.pending, id)
		removed++
	}
	return removed, nil
}

func (db *DB) XPendingSummary(key, groupName string) (StreamPendingSummary, error) {
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XPendingSummary(key, groupName)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	_, group, err := db.streamGroupLocked(key, groupName)
	if err != nil {
		return StreamPendingSummary{}, err
	}
	summary := StreamPendingSummary{Count: len(group.pending), Consumers: make(map[string]int)}
	ids := sortedPendingMapIDs(group.pending)
	if len(ids) > 0 {
		summary.Smallest = ids[0]
		summary.Greatest = ids[len(ids)-1]
	}
	for _, pending := range group.pending {
		summary.Consumers[pending.consumer]++
	}
	return summary, nil
}

func (db *DB) XPendingRange(key, groupName string, start, end StreamRangeBound, count int64, consumerName string) ([]StreamPending, error) {
	if count <= 0 {
		return nil, ErrInvalidInteger
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XPendingRange(key, groupName, start, end, count, consumerName)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	_, group, err := db.streamGroupLocked(key, groupName)
	if err != nil {
		return nil, err
	}
	now := db.now().UnixMilli()
	result := make([]StreamPending, 0)
	for _, id := range sortedPendingMapIDs(group.pending) {
		if !streamIDWithinBounds(id, start, end) {
			continue
		}
		pending := group.pending[id]
		if consumerName != "" && pending.consumer != consumerName {
			continue
		}
		result = append(result, StreamPending{ID: id, Consumer: pending.consumer, IdleMillis: nonnegativeDifference(now, pending.deliveredAt), Deliveries: pending.deliveries})
		if int64(len(result)) >= count {
			break
		}
	}
	return result, nil
}

func (db *DB) XClaim(key, groupName, consumerName string, minIdleMillis int64, ids []StreamID, options StreamClaimOptions) ([]StreamEntry, []StreamID, error) {
	if minIdleMillis < 0 || consumerName == "" {
		return nil, nil, ErrInvalidInteger
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XClaim(key, groupName, consumerName, minIdleMillis, ids, options)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	stream, group, err := db.streamGroupLocked(key, groupName)
	if err != nil {
		return nil, nil, err
	}
	now := db.now().UnixMilli()
	consumer := ensureStreamConsumer(group, consumerName, now)
	entries := make([]StreamEntry, 0, len(ids))
	claimed := make([]StreamID, 0, len(ids))
	for _, id := range ids {
		pending, exists := group.pending[id]
		entry, entryExists := findStreamEntry(stream.entries, id)
		if exists && !entryExists {
			if previous := group.consumers[pending.consumer]; previous != nil {
				delete(previous.pending, id)
			}
			delete(group.pending, id)
			continue
		}
		if !exists {
			if !options.Force || !entryExists {
				continue
			}
			pending = &streamPendingEntry{deliveries: 1}
			group.pending[id] = pending
		} else if nonnegativeDifference(now, pending.deliveredAt) < minIdleMillis {
			continue
		}
		if previous := group.consumers[pending.consumer]; previous != nil {
			delete(previous.pending, id)
		}
		pending.consumer = consumerName
		pending.deliveredAt = claimDeliveryTime(now, options)
		if options.RetryCount != nil {
			pending.deliveries = *options.RetryCount
		} else if !options.JustID && exists {
			pending.deliveries++
		}
		if pending.deliveries == 0 {
			pending.deliveries = 1
		}
		consumer.pending[id] = struct{}{}
		consumer.seenAt = now
		consumer.activeAt = now
		claimed = append(claimed, id)
		if entryExists {
			entries = append(entries, entry)
		}
	}
	return entries, claimed, nil
}

func (db *DB) XAutoClaim(key, groupName, consumerName string, minIdleMillis int64, start StreamID, count int64, justID bool) (StreamAutoClaimResult, error) {
	if minIdleMillis < 0 || count <= 0 || consumerName == "" {
		return StreamAutoClaimResult{}, ErrInvalidInteger
	}
	if db.shards != nil {
		release := db.operationGate()
		defer release()
		return db.shardFor(key).XAutoClaim(key, groupName, consumerName, minIdleMillis, start, count, justID)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	stream, group, err := db.streamGroupLocked(key, groupName)
	if err != nil {
		return StreamAutoClaimResult{}, err
	}
	now := db.now().UnixMilli()
	consumer := ensureStreamConsumer(group, consumerName, now)
	ids := sortedPendingMapIDs(group.pending)
	position := sort.Search(len(ids), func(index int) bool { return ids[index].Compare(start) >= 0 })
	result := StreamAutoClaimResult{}
	scanned := int64(0)
	maxScan := int64(math.MaxInt64)
	if count <= math.MaxInt64/10 {
		maxScan = count * 10
	}
	for position < len(ids) && scanned < maxScan && int64(len(result.IDs)) < count {
		id := ids[position]
		position++
		scanned++
		pending := group.pending[id]
		entry, exists := findStreamEntry(stream.entries, id)
		if !exists {
			if previous := group.consumers[pending.consumer]; previous != nil {
				delete(previous.pending, id)
			}
			delete(group.pending, id)
			result.Deleted = append(result.Deleted, id)
			continue
		}
		if nonnegativeDifference(now, pending.deliveredAt) < minIdleMillis {
			continue
		}
		if previous := group.consumers[pending.consumer]; previous != nil {
			delete(previous.pending, id)
		}
		pending.consumer = consumerName
		pending.deliveredAt = now
		if !justID {
			pending.deliveries++
		}
		consumer.pending[id] = struct{}{}
		consumer.seenAt = now
		consumer.activeAt = now
		result.IDs = append(result.IDs, id)
		result.Entries = append(result.Entries, entry)
	}
	if position < len(ids) {
		result.Next = ids[position]
	}
	return result, nil
}

func (db *DB) streamGroupLocked(key, groupName string) (*streamValue, *streamConsumerGroup, error) {
	e, exists := db.entries[key]
	if !exists {
		return nil, nil, ErrNoGroup
	}
	if e.kind != KindStream {
		return nil, nil, ErrWrongType
	}
	stream := e.value.(*streamValue)
	group, exists := stream.groups[groupName]
	if !exists {
		return nil, nil, ErrNoGroup
	}
	return stream, group, nil
}

func ensureStreamConsumer(group *streamConsumerGroup, name string, now int64) *streamConsumer {
	consumer := group.consumers[name]
	if consumer == nil {
		consumer = &streamConsumer{seenAt: now, activeAt: now, pending: make(map[StreamID]struct{})}
		group.consumers[name] = consumer
	}
	return consumer
}

func sortedPendingIDs(pending map[StreamID]struct{}) []StreamID {
	ids := make([]StreamID, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].Compare(ids[j]) < 0 })
	return ids
}

func sortedPendingMapIDs(pending map[StreamID]*streamPendingEntry) []StreamID {
	ids := make([]StreamID, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].Compare(ids[j]) < 0 })
	return ids
}

func findStreamEntry(entries []StreamEntry, id StreamID) (StreamEntry, bool) {
	position := sort.Search(len(entries), func(index int) bool { return entries[index].ID.Compare(id) >= 0 })
	if position >= len(entries) || entries[position].ID != id {
		return StreamEntry{}, false
	}
	item := entries[position]
	item.Fields = append([]StreamField(nil), item.Fields...)
	return item, true
}

func streamIDWithinBounds(id StreamID, start, end StreamRangeBound) bool {
	if start.Unbounded >= 0 {
		comparison := id.Compare(start.ID)
		if comparison < 0 || comparison == 0 && start.Exclusive {
			return false
		}
	}
	if end.Unbounded <= 0 {
		comparison := id.Compare(end.ID)
		if comparison > 0 || comparison == 0 && end.Exclusive {
			return false
		}
	}
	return true
}

func nonnegativeDifference(now, then int64) int64 {
	if now <= then {
		return 0
	}
	return now - then
}

func claimDeliveryTime(now int64, options StreamClaimOptions) int64 {
	if options.TimeMillis != nil {
		return *options.TimeMillis
	}
	if options.IdleMillis != nil {
		if *options.IdleMillis >= now {
			return 0
		}
		return now - *options.IdleMillis
	}
	return now
}
