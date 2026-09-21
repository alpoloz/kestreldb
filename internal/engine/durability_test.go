package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
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

func TestSnapshotLargeValueAndSizeBoundary(t *testing.T) {
	const largeValueSize = 8 << 20
	value := bytes.Repeat([]byte{0, 1, 2, 0xff}, largeValueSize/4)
	db := NewDB()
	db.Set("large", value)
	data, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewDB()
	if err := restored.LoadSnapshot(data); err != nil {
		t.Fatal(err)
	}
	got, found, err := restored.Get("large")
	if err != nil || !found || !bytes.Equal(got, value) {
		t.Fatalf("large value round trip = %d bytes, %v, %v", len(got), found, err)
	}

	path := filepath.Join(t.TempDir(), "oversized.snapshot")
	header := make([]byte, 16)
	copy(header, snapshotMagic)
	binary.BigEndian.PutUint32(header[8:12], uint32(maxDiskImage+1))
	if err := os.WriteFile(path, header, 0600); err != nil {
		t.Fatal(err)
	}
	if err := restored.LoadSnapshotFile(path); !errors.Is(err, ErrCorruptData) {
		t.Fatalf("oversized snapshot error = %v", err)
	}
}

func TestAOFFsyncPoliciesAndCleanShutdown(t *testing.T) {
	policies := []struct {
		name   string
		policy FsyncPolicy
	}{
		{name: "always", policy: FsyncAlways},
		{name: "everysec", policy: FsyncEverySecond},
		{name: "no", policy: FsyncNever},
	}
	for _, test := range policies {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.aof")
			db := NewDB()
			if err := db.OpenAOF(path, test.policy); err != nil {
				t.Fatal(err)
			}
			db.Set("policy", []byte(test.name))
			if err := db.CloseAOF(); err != nil {
				t.Fatal(err)
			}
			if err := db.CloseAOF(); err != nil {
				t.Fatalf("second close: %v", err)
			}

			recovered := NewDB()
			if err := recovered.OpenAOF(path, FsyncNever); err != nil {
				t.Fatal(err)
			}
			value, found, err := recovered.Get("policy")
			if err != nil || !found || string(value) != test.name {
				t.Fatalf("recovered value = %q, %v, %v", value, found, err)
			}
			if err := recovered.CloseAOF(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAOFRejectsCompleteCorruptionAndOffsetGap(t *testing.T) {
	source := NewDB()
	source.Set("key", []byte("value"))
	image, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("checksum", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "checksum.aof")
		writeTestAOFRecord(t, path, 1, image, crc32.ChecksumIEEE(image)+1)
		db := NewDB()
		if err := db.OpenAOF(path, FsyncNever); !errors.Is(err, ErrCorruptData) {
			t.Fatalf("checksum error = %v", err)
		}
	})

	t.Run("offset gap", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "gap.aof")
		writeTestAOFRecord(t, path, 2, image, crc32.ChecksumIEEE(image))
		db := NewDB()
		if err := db.OpenAOF(path, FsyncNever); !errors.Is(err, ErrCorruptData) {
			t.Fatalf("offset-gap error = %v", err)
		}
	})

	t.Run("oversized record", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "oversized.aof")
		data := make([]byte, 24)
		copy(data, aofMagic)
		binary.BigEndian.PutUint64(data[8:16], 1)
		binary.BigEndian.PutUint32(data[16:20], uint32(maxDiskImage+1))
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		db := NewDB()
		if err := db.OpenAOF(path, FsyncNever); !errors.Is(err, ErrCorruptData) {
			t.Fatalf("oversized-record error = %v", err)
		}
	})
}

func writeTestAOFRecord(t *testing.T, path string, offset uint64, image []byte, checksum uint32) {
	t.Helper()
	data := make([]byte, 8+16+len(image))
	copy(data, aofMagic)
	binary.BigEndian.PutUint64(data[8:16], offset)
	binary.BigEndian.PutUint32(data[16:20], uint32(len(image)))
	binary.BigEndian.PutUint32(data[20:24], checksum)
	copy(data[24:], image)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
