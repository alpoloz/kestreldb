package engine

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotRoundTripAllKinds(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	clock := func() time.Time { return now }
	db := NewDBWithClock(clock)
	key := "str\xff"
	db.Set(key, []byte{0, 0xff, '\n'})
	db.HSet("hash", "field\xff", "value\x00")
	if _, err := db.HExpireAt("hash", []string{"field\xff"}, now.Add(time.Hour), ExpireOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RPush("list", "left\xff", "right\x00"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SAddEx("set", time.Hour, "member\xff"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ZAdd("zset", 1.25, "sorted\xff"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExpireAt(key, now.Add(2*time.Hour), ExpireOptions{}); err != nil {
		t.Fatal(err)
	}
	want, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "data.snapshot")
	if err := db.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	restored := NewDBWithClock(clock)
	if err := restored.LoadSnapshotFile(path); err != nil {
		t.Fatal(err)
	}
	got, err := restored.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("snapshot round trip differs\nwant %s\ngot  %s", want, got)
	}
	if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := restored.LoadSnapshotFile(path); !errors.Is(err, ErrCorruptData) {
		t.Fatalf("corrupt snapshot: %v", err)
	}
	got, _ = restored.Snapshot()
	if !bytes.Equal(got, want) {
		t.Fatal("corrupt snapshot changed database")
	}
	now = now.Add(3 * time.Hour)
	if restored.Type(key) != KindNone || restored.Type("hash") != KindNone || restored.Type("set") != KindNone {
		t.Fatal("expired snapshot data remained visible")
	}
}

func TestJournalAndAOFRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	db := NewDB()
	if err := db.OpenAOF(path, FsyncAlways); err != nil {
		t.Fatal(err)
	}
	db.Set("key", []byte("one"))
	if got := db.JournalOffset(); got != 1 {
		t.Fatalf("offset = %d", got)
	}
	ch, cancel, err := db.SubscribeFrom(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if record := <-ch; record.Offset != 1 {
		t.Fatalf("record = %#v", record)
	}
	db.Set("key", []byte("two"))
	if record := <-ch; record.Offset != 2 {
		t.Fatalf("record = %#v", record)
	}
	if err := db.CloseAOF(); err != nil {
		t.Fatal(err)
	}
	// An incomplete tail is discarded; all complete records remain replayable.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	file.Close()
	recovered := NewDB()
	if err := recovered.OpenAOF(path, FsyncAlways); err != nil {
		t.Fatal(err)
	}
	value, found, err := recovered.Get("key")
	if err != nil || !found || string(value) != "two" {
		t.Fatalf("recovered = %q, %v, %v", value, found, err)
	}
	if recovered.JournalOffset() != 2 {
		t.Fatalf("recovered offset = %d", recovered.JournalOffset())
	}
	if err := recovered.CloseAOF(); err != nil {
		t.Fatal(err)
	}
}
