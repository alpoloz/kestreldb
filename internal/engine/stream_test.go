package engine

import (
	"context"
	"testing"
	"time"
)

func TestStreamAddRangeTrimAndDelete(t *testing.T) {
	now := time.UnixMilli(1000)
	db := NewDBWithClock(func() time.Time { return now })
	first, added, err := db.XAdd("events", StreamIDRequest{Auto: true}, []StreamField{{Name: "type", Value: "created"}}, StreamAddOptions{})
	if err != nil || !added || first != (StreamID{Milliseconds: 1000}) {
		t.Fatalf("first XADD = %v, %v, %v", first, added, err)
	}
	second, _, err := db.XAdd("events", StreamIDRequest{Auto: true}, []StreamField{{Name: "type", Value: "updated"}}, StreamAddOptions{})
	if err != nil || second != (StreamID{Milliseconds: 1000, Sequence: 1}) {
		t.Fatalf("second XADD = %v, %v", second, err)
	}
	third, _, err := db.XAdd("events", StreamIDRequest{Milliseconds: 1001, AutoSequence: true}, []StreamField{{Name: "type", Value: "done"}}, StreamAddOptions{})
	if err != nil || third != (StreamID{Milliseconds: 1001}) {
		t.Fatalf("third XADD = %v, %v", third, err)
	}

	entries, err := db.XRange("events", StreamRangeBound{Unbounded: -1}, StreamRangeBound{Unbounded: 1}, 2, true)
	if err != nil || len(entries) != 2 || entries[0].ID != third || entries[1].ID != second {
		t.Fatalf("reverse range = %#v, %v", entries, err)
	}
	removed, err := db.XTrim("events", StreamTrimOptions{Mode: StreamTrimMaxLen, MaxLen: 2})
	if err != nil || removed != 1 {
		t.Fatalf("XTRIM = %d, %v", removed, err)
	}
	removed, err = db.XDel("events", second)
	if err != nil || removed != 1 {
		t.Fatalf("XDEL = %d, %v", removed, err)
	}
	if length, err := db.XLen("events"); err != nil || length != 1 {
		t.Fatalf("XLEN = %d, %v", length, err)
	}
	if _, _, err := db.XAdd("events", StreamIDRequest{Milliseconds: 999}, []StreamField{{Name: "x", Value: "y"}}, StreamAddOptions{}); err != ErrStreamIDTooSmall {
		t.Fatalf("out-of-order XADD error = %v", err)
	}
}

func TestStreamConsumerGroupPendingClaimAndAck(t *testing.T) {
	now := time.UnixMilli(2000)
	db := NewDBWithClock(func() time.Time { return now })
	for sequence := uint64(1); sequence <= 3; sequence++ {
		if _, _, err := db.XAdd("jobs", StreamIDRequest{Milliseconds: 1, Sequence: sequence}, []StreamField{{Name: "job", Value: string(rune('a' + sequence - 1))}}, StreamAddOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.XGroupCreate("jobs", "workers", StreamID{}, false, false); err != nil {
		t.Fatal(err)
	}
	result, err := db.XReadGroup("workers", "alice", []StreamReadRequest{{Key: "jobs", NewOnly: true}}, StreamGroupReadOptions{Count: 2})
	if err != nil || len(result) != 1 || len(result[0].Entries) != 2 {
		t.Fatalf("XREADGROUP = %#v, %v", result, err)
	}
	summary, err := db.XPendingSummary("jobs", "workers")
	if err != nil || summary.Count != 2 || summary.Consumers["alice"] != 2 {
		t.Fatalf("XPENDING summary = %#v, %v", summary, err)
	}
	now = now.Add(time.Second)
	claimedEntries, claimedIDs, err := db.XClaim("jobs", "workers", "bob", 500, []StreamID{{Milliseconds: 1, Sequence: 1}}, StreamClaimOptions{})
	if err != nil || len(claimedEntries) != 1 || len(claimedIDs) != 1 {
		t.Fatalf("XCLAIM = %#v, %#v, %v", claimedEntries, claimedIDs, err)
	}
	pending, err := db.XPendingRange("jobs", "workers", StreamRangeBound{Unbounded: -1}, StreamRangeBound{Unbounded: 1}, 10, "bob")
	if err != nil || len(pending) != 1 || pending[0].Deliveries != 2 {
		t.Fatalf("bob pending = %#v, %v", pending, err)
	}
	acked, err := db.XAck("jobs", "workers", claimedIDs[0])
	if err != nil || acked != 1 {
		t.Fatalf("XACK = %d, %v", acked, err)
	}
}

func TestStreamSnapshotPreservesGroupsAndPending(t *testing.T) {
	db := NewDB()
	id, _, err := db.XAdd("stream", StreamIDRequest{Milliseconds: 10}, []StreamField{{Name: "binary\x00field", Value: "binary\x00value"}}, StreamAddOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.XGroupCreate("stream", "group", StreamID{}, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := db.XReadGroup("group", "consumer", []StreamReadRequest{{Key: "stream", NewOnly: true}}, StreamGroupReadOptions{}); err != nil {
		t.Fatal(err)
	}
	image, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewDB()
	if err := restored.LoadSnapshot(image); err != nil {
		t.Fatal(err)
	}
	entries, err := restored.XRange("stream", StreamRangeBound{Unbounded: -1}, StreamRangeBound{Unbounded: 1}, 0, false)
	if err != nil || len(entries) != 1 || entries[0].ID != id || entries[0].Fields[0].Value != "binary\x00value" {
		t.Fatalf("restored entries = %#v, %v", entries, err)
	}
	summary, err := restored.XPendingSummary("stream", "group")
	if err != nil || summary.Count != 1 || summary.Consumers["consumer"] != 1 {
		t.Fatalf("restored pending = %#v, %v", summary, err)
	}
}

func TestBlockingStreamReadWakesOnAppend(t *testing.T) {
	db := NewDB()
	result := make(chan []StreamReadResult, 1)
	errs := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		streams, err := db.WaitForStreamRead(ctx, []StreamReadRequest{{Key: "events", UseLatest: true}}, 0)
		result <- streams
		errs <- err
	}()
	time.Sleep(10 * time.Millisecond)
	id, _, err := db.XAdd("events", StreamIDRequest{Milliseconds: 1}, []StreamField{{Name: "event", Value: "ready"}}, StreamAddOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case streams := <-result:
		if err := <-errs; err != nil || len(streams) != 1 || streams[0].Entries[0].ID != id {
			t.Fatalf("blocking read = %#v, %v", streams, err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocking stream read did not wake")
	}
}

func TestStreamMutationsAreJournaled(t *testing.T) {
	db := NewDB()
	db.EnableJournal()
	records, cancel, err := db.SubscribeFrom(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if _, _, err := db.XAdd("events", StreamIDRequest{Milliseconds: 1}, []StreamField{{Name: "event", Value: "created"}}, StreamAddOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case record := <-records:
		restored := NewDB()
		if err := restored.LoadSnapshot(record.Snapshot); err != nil {
			t.Fatal(err)
		}
		if length, err := restored.XLen("events"); err != nil || length != 1 {
			t.Fatalf("journaled XLEN = %d, %v", length, err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream mutation was not journaled")
	}
}
