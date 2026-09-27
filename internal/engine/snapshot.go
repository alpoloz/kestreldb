package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
)

// Snapshot is a portable, versioned logical image of the database. Strings
// in JSON are escaped byte strings; []byte values use JSON's base64 encoding.
type snapshotImage struct {
	Version int             `json:"version"`
	Entries []snapshotEntry `json:"entries"`
}

type snapshotEntry struct {
	Key      []byte           `json:"key"`
	Kind     Kind             `json:"kind"`
	ExpireAt int64            `json:"expire_at,omitempty"`
	String   []byte           `json:"string,omitempty"`
	Hash     []snapshotField  `json:"hash,omitempty"`
	List     [][]byte         `json:"list,omitempty"`
	Set      []snapshotMember `json:"set,omitempty"`
	ZSet     []snapshotScore  `json:"zset,omitempty"`
	Stream   *snapshotStream  `json:"stream,omitempty"`
	JSON     json.RawMessage  `json:"json,omitempty"`
}

type snapshotStream struct {
	LastGenerated snapshotStreamID      `json:"last_generated"`
	Entries       []snapshotStreamEntry `json:"entries,omitempty"`
	Groups        []snapshotStreamGroup `json:"groups,omitempty"`
}

type snapshotStreamID struct {
	Milliseconds uint64 `json:"milliseconds"`
	Sequence     uint64 `json:"sequence"`
}

type snapshotStreamEntry struct {
	ID     snapshotStreamID      `json:"id"`
	Fields []snapshotStreamField `json:"fields"`
}

type snapshotStreamField struct {
	Name  []byte `json:"name"`
	Value []byte `json:"value"`
}

type snapshotStreamGroup struct {
	Name          []byte                   `json:"name"`
	LastDelivered snapshotStreamID         `json:"last_delivered"`
	EntriesRead   uint64                   `json:"entries_read"`
	Consumers     []snapshotStreamConsumer `json:"consumers,omitempty"`
	Pending       []snapshotStreamPending  `json:"pending,omitempty"`
}

type snapshotStreamConsumer struct {
	Name     []byte `json:"name"`
	SeenAt   int64  `json:"seen_at"`
	ActiveAt int64  `json:"active_at"`
}

type snapshotStreamPending struct {
	ID          snapshotStreamID `json:"id"`
	Consumer    []byte           `json:"consumer"`
	DeliveredAt int64            `json:"delivered_at"`
	Deliveries  uint64           `json:"deliveries"`
}

type snapshotScore struct {
	Member []byte `json:"member"`
	Score  string `json:"score"`
}

type snapshotField struct {
	Field    []byte `json:"field"`
	Value    []byte `json:"value"`
	ExpireAt int64  `json:"expire_at,omitempty"`
}

type snapshotMember struct {
	Member   []byte `json:"member"`
	ExpireAt int64  `json:"expire_at,omitempty"`
}

// Snapshot serializes all live keys and their key/member expiration deadlines.
func (db *DB) Snapshot() ([]byte, error) {
	if db.shards != nil {
		db.gate.Lock()
		defer db.gate.Unlock()
		db.purgeAllShards()
		return db.snapshotLocked()
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.purgeExpiredLocked()
	return db.snapshotLocked()
}

func (db *DB) snapshotLocked() ([]byte, error) {
	if db.shards != nil {
		for _, shard := range db.shards {
			shard.mu.Lock()
		}
		defer func() {
			for i := len(db.shards) - 1; i >= 0; i-- {
				db.shards[i].mu.Unlock()
			}
		}()
		view := newCoreDB(db.now)
		for _, shard := range db.shards {
			for key, e := range shard.entries {
				view.entries[key] = e
			}
			for key, deadline := range shard.expiring {
				view.expiring[key] = deadline
			}
			for ref, deadline := range shard.expiringHashFields {
				view.expiringHashFields[ref] = deadline
			}
			for ref, deadline := range shard.expiringSetMembers {
				view.expiringSetMembers[ref] = deadline
			}
		}
		return view.snapshotLocked()
	}
	keys := make([]string, 0, len(db.entries))
	for key := range db.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	image := snapshotImage{Version: 1, Entries: make([]snapshotEntry, 0, len(keys))}
	for _, key := range keys {
		e := db.entries[key]
		item := snapshotEntry{Key: []byte(key), Kind: e.kind, ExpireAt: e.expireAt}
		switch e.kind {
		case KindString:
			item.String = bytes.Clone(e.value.([]byte))
		case KindHash:
			h := e.value.(map[string]string)
			fields := make([]string, 0, len(h))
			for field := range h {
				fields = append(fields, field)
			}
			sort.Strings(fields)
			for _, field := range fields {
				item.Hash = append(item.Hash, snapshotField{Field: []byte(field), Value: []byte(h[field]), ExpireAt: db.expiringHashFields[hashFieldRef{key: key, field: field}]})
			}
		case KindList:
			for _, value := range e.value.(*deque).values() {
				item.List = append(item.List, []byte(value))
			}
		case KindSet:
			set := e.value.(stringSet)
			members := make([]string, 0, len(set))
			for member := range set {
				members = append(members, member)
			}
			sort.Strings(members)
			for _, member := range members {
				item.Set = append(item.Set, snapshotMember{Member: []byte(member), ExpireAt: db.expiringSetMembers[setMemberRef{key: key, member: member}]})
			}
		case KindSortedSet:
			for _, member := range e.value.(*zset).items() {
				item.ZSet = append(item.ZSet, snapshotScore{Member: []byte(member.Member), Score: strconv.FormatFloat(member.Score, 'g', -1, 64)})
			}
		case KindStream:
			item.Stream = snapshotStreamValue(e.value.(*streamValue))
		case KindJSON:
			var err error
			item.JSON, err = json.Marshal(e.value)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("snapshot: unsupported kind %d", e.kind)
		}
		image.Entries = append(image.Entries, item)
	}
	return json.Marshal(image)
}

func snapshotStreamValue(stream *streamValue) *snapshotStream {
	result := &snapshotStream{LastGenerated: snapshotID(stream.lastGenerated)}
	for _, entry := range stream.entries {
		item := snapshotStreamEntry{ID: snapshotID(entry.ID)}
		for _, field := range entry.Fields {
			item.Fields = append(item.Fields, snapshotStreamField{Name: []byte(field.Name), Value: []byte(field.Value)})
		}
		result.Entries = append(result.Entries, item)
	}
	groupNames := make([]string, 0, len(stream.groups))
	for name := range stream.groups {
		groupNames = append(groupNames, name)
	}
	sort.Strings(groupNames)
	for _, name := range groupNames {
		group := stream.groups[name]
		item := snapshotStreamGroup{Name: []byte(name), LastDelivered: snapshotID(group.lastDelivered), EntriesRead: group.entriesRead}
		consumerNames := make([]string, 0, len(group.consumers))
		for consumer := range group.consumers {
			consumerNames = append(consumerNames, consumer)
		}
		sort.Strings(consumerNames)
		for _, consumerName := range consumerNames {
			consumer := group.consumers[consumerName]
			item.Consumers = append(item.Consumers, snapshotStreamConsumer{Name: []byte(consumerName), SeenAt: consumer.seenAt, ActiveAt: consumer.activeAt})
		}
		for _, id := range sortedPendingMapIDs(group.pending) {
			pending := group.pending[id]
			item.Pending = append(item.Pending, snapshotStreamPending{ID: snapshotID(id), Consumer: []byte(pending.consumer), DeliveredAt: pending.deliveredAt, Deliveries: pending.deliveries})
		}
		result.Groups = append(result.Groups, item)
	}
	return result
}

func snapshotID(id StreamID) snapshotStreamID {
	return snapshotStreamID{Milliseconds: id.Milliseconds, Sequence: id.Sequence}
}

func restoreSnapshotID(id snapshotStreamID) StreamID {
	return StreamID{Milliseconds: id.Milliseconds, Sequence: id.Sequence}
}

func loadSnapshotStream(item *snapshotStream) (*streamValue, error) {
	if item == nil {
		return nil, errors.New("missing stream value")
	}
	stream := newStreamValue()
	stream.lastGenerated = restoreSnapshotID(item.LastGenerated)
	var previous StreamID
	for index, encoded := range item.Entries {
		id := restoreSnapshotID(encoded.ID)
		if id == (StreamID{}) || index > 0 && id.Compare(previous) <= 0 {
			return nil, errors.New("invalid stream entry order")
		}
		fields := make([]StreamField, 0, len(encoded.Fields))
		for _, field := range encoded.Fields {
			fields = append(fields, StreamField{Name: string(field.Name), Value: string(field.Value)})
		}
		if len(fields) == 0 {
			return nil, errors.New("stream entry has no fields")
		}
		stream.entries = append(stream.entries, StreamEntry{ID: id, Fields: fields})
		previous = id
	}
	if len(stream.entries) > 0 && stream.lastGenerated.Compare(previous) < 0 {
		return nil, errors.New("stream last ID precedes an entry")
	}
	for _, encodedGroup := range item.Groups {
		name := string(encodedGroup.Name)
		if name == "" {
			return nil, errors.New("empty stream group name")
		}
		if _, duplicate := stream.groups[name]; duplicate {
			return nil, errors.New("duplicate stream group")
		}
		group := newStreamConsumerGroup(restoreSnapshotID(encodedGroup.LastDelivered))
		group.entriesRead = encodedGroup.EntriesRead
		for _, encodedConsumer := range encodedGroup.Consumers {
			consumerName := string(encodedConsumer.Name)
			if consumerName == "" {
				return nil, errors.New("empty stream consumer name")
			}
			if _, duplicate := group.consumers[consumerName]; duplicate {
				return nil, errors.New("duplicate stream consumer")
			}
			group.consumers[consumerName] = &streamConsumer{seenAt: encodedConsumer.SeenAt, activeAt: encodedConsumer.ActiveAt, pending: make(map[StreamID]struct{})}
		}
		for _, encodedPending := range encodedGroup.Pending {
			id := restoreSnapshotID(encodedPending.ID)
			consumerName := string(encodedPending.Consumer)
			consumer := group.consumers[consumerName]
			if id == (StreamID{}) || consumer == nil || encodedPending.Deliveries == 0 {
				return nil, errors.New("invalid stream pending entry")
			}
			if _, duplicate := group.pending[id]; duplicate {
				return nil, errors.New("duplicate stream pending entry")
			}
			group.pending[id] = &streamPendingEntry{consumer: consumerName, deliveredAt: encodedPending.DeliveredAt, deliveries: encodedPending.Deliveries}
			consumer.pending[id] = struct{}{}
		}
		stream.groups[name] = group
	}
	return stream, nil
}

func (db *DB) purgeAllShards() {
	for _, shard := range db.shards {
		shard.mu.Lock()
		shard.purgeExpiredLocked()
		shard.mu.Unlock()
	}
}

// LoadSnapshot validates the entire image before replacing the keyspace.
// Expired keys and members are discarded according to the database clock.
func (db *DB) LoadSnapshot(data []byte) error {
	var image snapshotImage
	if err := json.Unmarshal(data, &image); err != nil {
		return err
	}
	if image.Version != 1 {
		return errors.New("unsupported snapshot version")
	}
	entries := make(map[string]*entry, len(image.Entries))
	expiring := make(map[string]int64)
	hashExp := make(map[hashFieldRef]int64)
	setExp := make(map[setMemberRef]int64)
	now := db.now().UnixMilli()
	for _, item := range image.Entries {
		key := string(item.Key)
		if _, duplicate := entries[key]; duplicate {
			return errors.New("duplicate snapshot key")
		}
		if item.ExpireAt < 0 {
			return errors.New("invalid key expiration")
		}
		if item.ExpireAt > 0 && item.ExpireAt <= now {
			continue
		}
		e := &entry{kind: item.Kind, expireAt: item.ExpireAt}
		switch item.Kind {
		case KindString:
			e.value = bytes.Clone(item.String)
		case KindHash:
			h := make(map[string]string, len(item.Hash))
			for _, field := range item.Hash {
				if field.ExpireAt < 0 {
					return errors.New("invalid hash field expiration")
				}
				fieldName := string(field.Field)
				if _, duplicate := h[fieldName]; duplicate {
					return errors.New("duplicate hash field")
				}
				if field.ExpireAt > 0 && field.ExpireAt <= now {
					continue
				}
				h[fieldName] = string(field.Value)
				if field.ExpireAt > 0 {
					hashExp[hashFieldRef{key: key, field: fieldName}] = field.ExpireAt
				}
			}
			if len(h) == 0 {
				continue
			}
			e.value = h
		case KindList:
			if len(item.List) == 0 {
				continue
			}
			values := make([]string, len(item.List))
			for i, value := range item.List {
				values[i] = string(value)
			}
			e.value = newDeque(values...)
		case KindSet:
			set := make(stringSet, len(item.Set))
			for _, member := range item.Set {
				if member.ExpireAt < 0 {
					return errors.New("invalid set member expiration")
				}
				name := string(member.Member)
				if _, duplicate := set[name]; duplicate {
					return errors.New("duplicate set member")
				}
				if member.ExpireAt > 0 && member.ExpireAt <= now {
					continue
				}
				set[name] = struct{}{}
				if member.ExpireAt > 0 {
					setExp[setMemberRef{key: key, member: name}] = member.ExpireAt
				}
			}
			if len(set) == 0 {
				continue
			}
			e.value = set
		case KindSortedSet:
			if len(item.ZSet) == 0 {
				continue
			}
			z := newZSet()
			for _, member := range item.ZSet {
				score, err := strconv.ParseFloat(member.Score, 64)
				if err != nil || math.IsNaN(score) {
					return errors.New("invalid sorted-set score")
				}
				name := string(member.Member)
				if _, duplicate := z.dict[name]; duplicate {
					return errors.New("duplicate sorted-set member")
				}
				z.add(score, name)
			}
			e.value = z
		case KindStream:
			stream, err := loadSnapshotStream(item.Stream)
			if err != nil {
				return err
			}
			e.value = stream
		case KindJSON:
			value, err := parseJSONValue(item.JSON)
			if err != nil {
				return fmt.Errorf("invalid JSON snapshot value: %w", err)
			}
			e.value = value
		default:
			return fmt.Errorf("unsupported snapshot kind %d", item.Kind)
		}
		entries[key] = e
		if item.ExpireAt > 0 {
			expiring[key] = item.ExpireAt
		}
	}
	if db.shards == nil {
		db.mu.Lock()
		db.entries = entries
		db.expiring = expiring
		db.expiringHashFields = hashExp
		db.expiringSetMembers = setExp
		db.mu.Unlock()
		return nil
	}

	db.gate.Lock()
	db.mu.Lock()
	for _, shard := range db.shards {
		shard.mu.Lock()
		shard.entries = make(map[string]*entry)
		shard.expiring = make(map[string]int64)
		shard.expiringHashFields = make(map[hashFieldRef]int64)
		shard.expiringSetMembers = make(map[setMemberRef]int64)
		shard.mu.Unlock()
	}
	for key, e := range entries {
		shard := db.shardFor(key)
		shard.mu.Lock()
		shard.entries[key] = e
		if deadline, ok := expiring[key]; ok {
			shard.expiring[key] = deadline
		}
		for ref, deadline := range hashExp {
			if ref.key == key {
				shard.expiringHashFields[ref] = deadline
			}
		}
		for ref, deadline := range setExp {
			if ref.key == key {
				shard.expiringSetMembers[ref] = deadline
			}
		}
		shard.mu.Unlock()
	}
	db.mu.Unlock()
	db.gate.Unlock()
	return nil
}
