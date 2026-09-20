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
		default:
			return nil, fmt.Errorf("snapshot: unsupported kind %d", e.kind)
		}
		image.Entries = append(image.Entries, item)
	}
	return json.Marshal(image)
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
