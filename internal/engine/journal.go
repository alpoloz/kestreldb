package engine

import (
	"bytes"
	"errors"
	"sync"
)

// MutationRecord is one ordered, committed database state. A full logical
// image is used as the replay payload so multi-key changes remain indivisible.
// This first format favors correctness over journal size.
type MutationRecord struct {
	Offset   uint64
	Snapshot []byte
}

var ErrJournalGap = errors.New("requested journal offset is no longer available")

const journalHistoryLimit = 64

type mutationJournal struct {
	offset      uint64
	history     []MutationRecord
	subscribers map[chan MutationRecord]struct{}
	appendFile  func(MutationRecord) error
	err         error
}

// dbMutex preserves the existing transaction boundaries. When journaling is
// enabled it compares deterministic images at the lock boundary and appends
// only committed changes. Failed and read-only commands produce no record.
type dbMutex struct {
	sync.RWMutex
	owner   *DB
	before  []byte
	tracked bool
}

func (m *dbMutex) Lock() {
	m.RWMutex.Lock()
	m.tracked = m.owner != nil && m.owner.journal != nil
	if m.tracked {
		var err error
		m.before, err = m.owner.snapshotLocked()
		if err != nil {
			m.owner.journal.err = err
			m.tracked = false
		}
	}
}

func (m *dbMutex) Unlock() {
	if m.tracked && m.owner.journal != nil {
		after, err := m.owner.snapshotLocked()
		if err != nil {
			m.owner.journal.err = err
		} else if !bytes.Equal(m.before, after) {
			m.owner.journal.appendLocked(after)
		}
	}
	m.before = nil
	m.tracked = false
	m.RWMutex.Unlock()
}

func (j *mutationJournal) appendLocked(snapshot []byte) {
	j.offset++
	record := MutationRecord{Offset: j.offset, Snapshot: bytes.Clone(snapshot)}
	j.history = append(j.history, record)
	if len(j.history) > journalHistoryLimit {
		j.history = append([]MutationRecord(nil), j.history[len(j.history)-journalHistoryLimit:]...)
	}
	if j.appendFile != nil && j.err == nil {
		j.err = j.appendFile(record)
	}
	for subscriber := range j.subscribers {
		select {
		case subscriber <- record:
		default:
			close(subscriber)
			delete(j.subscribers, subscriber)
		}
	}
}

// EnableJournal begins recording committed mutations. It is idempotent.
func (db *DB) EnableJournal() {
	db.gate.Lock()
	defer db.gate.Unlock()
	db.mu.Lock()
	if db.journal == nil {
		db.journal = &mutationJournal{subscribers: make(map[chan MutationRecord]struct{})}
	}
	db.mu.Unlock()
}

func (db *DB) JournalOffset() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.journal == nil {
		return 0
	}
	return db.journal.offset
}

func (db *DB) JournalError() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.journal == nil {
		return nil
	}
	if db.aof != nil {
		db.aof.mu.Lock()
		defer db.aof.mu.Unlock()
		if db.aof.err != nil {
			return db.aof.err
		}
	}
	return db.journal.err
}

// SnapshotAtOffset atomically captures a state and its last journal offset.
func (db *DB) SnapshotAtOffset() ([]byte, uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.shards != nil {
		db.purgeAllShards()
	} else {
		db.purgeExpiredLocked()
	}
	data, err := db.snapshotLocked()
	if err != nil {
		return nil, 0, err
	}
	if db.journal == nil {
		return data, 0, nil
	}
	// Expiration during this call is committed by the deferred Unlock; report
	// the offset that the committed snapshot will have.
	if !bytes.Equal(db.mu.before, data) {
		return data, db.journal.offset + 1, nil
	}
	return data, db.journal.offset, nil
}

// SubscribeFrom streams records after offset. A missing history range requires
// a fresh snapshot. A slow subscriber is closed instead of blocking writers.
func (db *DB) SubscribeFrom(offset uint64) (<-chan MutationRecord, func(), error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.journal == nil {
		return nil, nil, errors.New("journal is disabled")
	}
	j := db.journal
	if offset > j.offset {
		return nil, nil, errors.New("journal offset is in the future")
	}
	if len(j.history) > 0 && offset < j.history[0].Offset-1 {
		return nil, nil, ErrJournalGap
	}
	ch := make(chan MutationRecord, journalHistoryLimit+1)
	for _, record := range j.history {
		if record.Offset > offset {
			ch <- record
		}
	}
	j.subscribers[ch] = struct{}{}
	cancel := func() {
		db.mu.Lock()
		if _, ok := j.subscribers[ch]; ok {
			delete(j.subscribers, ch)
			close(ch)
		}
		db.mu.Unlock()
	}
	return ch, cancel, nil
}
