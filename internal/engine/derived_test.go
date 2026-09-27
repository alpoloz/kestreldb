package engine

import (
	"bytes"
	"errors"
	"math"
	"path/filepath"
	"strconv"
	"testing"
)

func TestJSONPathsCopyAndSnapshot(t *testing.T) {
	db := NewDB()
	if stored, err := db.JSONSet("doc", "$", []byte(`{"name":"Ada","scores":[1,2],"nested":{"enabled":true}}`), false, false); err != nil || !stored {
		t.Fatalf("JSONSet root = %v, %v", stored, err)
	}
	if stored, err := db.JSONSet("doc", "$.nested.count", []byte(`3`), true, false); err != nil || !stored {
		t.Fatalf("JSONSet path = %v, %v", stored, err)
	}
	if stored, err := db.JSONSet("doc", "$.nested.count", []byte(`4`), true, false); err != nil || stored {
		t.Fatalf("JSONSet NX = %v, %v", stored, err)
	}
	if value, found, err := db.JSONNumIncrBy("doc", "$.nested.count", "2"); err != nil || !found || string(value) != "5" {
		t.Fatalf("JSONNumIncrBy = %q, %v, %v", value, found, err)
	}
	if length, found, err := db.JSONArrAppend("doc", "$.scores", []byte(`3`)); err != nil || !found || length != 3 {
		t.Fatalf("JSONArrAppend = %d, %v, %v", length, found, err)
	}
	if _, _, err := db.JSONArrAppend("doc", "$.scores", []byte(`bad`)); !errors.Is(err, ErrInvalidJSON) {
		t.Fatalf("invalid append = %v", err)
	}
	if length, found, err := db.JSONArrLen("doc", "$.scores"); err != nil || !found || length != 3 {
		t.Fatalf("JSONArrLen = %d, %v, %v", length, found, err)
	}
	if count, err := db.JSONDel("doc", "$.scores[1]"); err != nil || count != 1 {
		t.Fatalf("JSONDel = %d, %v", count, err)
	}
	if kind, found, err := db.JSONType("doc", "$.nested.enabled"); err != nil || !found || kind != "boolean" {
		t.Fatalf("JSONType = %q, %v, %v", kind, found, err)
	}
	if _, err := db.Copy("doc", "copy", false); err != nil {
		t.Fatal(err)
	}
	if _, err := db.JSONSet("doc", "$.name", []byte(`"Grace"`), false, false); err != nil {
		t.Fatal(err)
	}
	copied, _, _ := db.JSONGet("copy", "$.name")
	if string(copied) != `"Ada"` {
		t.Fatalf("COPY shares JSON tree: %s", copied)
	}
	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewDB()
	if err := restored.LoadSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	got, err := restored.Snapshot()
	if err != nil || !bytes.Equal(got, snapshot) {
		t.Fatalf("snapshot round trip differs: %v", err)
	}
	if restored.Type("doc") != KindJSON {
		t.Fatalf("type = %v", restored.Type("doc"))
	}
	if _, err := restored.JSONSet("doc", "$.missing.child", []byte(`1`), false, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := restored.Get("doc"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("string GET = %v", err)
	}
}

func TestBitmapAndBitfield(t *testing.T) {
	db := NewDB()
	if old, err := db.SetBit("bits", 9, 1); err != nil || old != 0 {
		t.Fatalf("SetBit = %d, %v", old, err)
	}
	if bit, err := db.GetBit("bits", 9); err != nil || bit != 1 {
		t.Fatalf("GetBit = %d, %v", bit, err)
	}
	if count, err := db.BitCount("bits", 8, 15, true, true); err != nil || count != 1 {
		t.Fatalf("BitCount = %d, %v", count, err)
	}
	if position, err := db.BitPos("bits", 1, 0, 0, false, false, false); err != nil || position != 9 {
		t.Fatalf("BitPos = %d, %v", position, err)
	}
	results, err := db.BitField("field", []BitFieldOp{{Kind: "SET", Signed: true, Width: 4, Offset: 0, Value: 7, Overflow: "WRAP"}, {Kind: "INCRBY", Signed: true, Width: 4, Offset: 0, Value: 1, Overflow: "WRAP"}, {Kind: "GET", Signed: true, Width: 4, Offset: 0}}, false)
	if err != nil || len(results) != 3 || results[0].Value != 0 || results[1].Value != -8 || results[2].Value != -8 {
		t.Fatalf("BitField = %#v, %v", results, err)
	}
	if length, err := db.BitOp("NOT", "inverse", "bits"); err != nil || length != 2 {
		t.Fatalf("BitOp = %d, %v", length, err)
	}
	saturated, err := db.BitField("sat", []BitFieldOp{{Kind: "SET", Signed: true, Width: 4, Offset: 0, Value: 100, Overflow: "SAT"}, {Kind: "GET", Signed: true, Width: 4, Offset: 0}}, false)
	if err != nil || saturated[1].Value != 7 {
		t.Fatalf("saturated BitField = %#v, %v", saturated, err)
	}
	db.Set("empty", []byte{})
	if position, err := db.BitPos("empty", 0, 0, 0, false, false, false); err != nil || position != 0 {
		t.Fatalf("empty BitPos = %d, %v", position, err)
	}
	if _, err := db.BitOp("AND", "destination", "bits", "field"); err != nil {
		t.Fatal(err)
	}
	db.HSet("hash", "field", "value")
	if _, err := db.BitOp("OR", "destination", "bits", "hash"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("BitOp mutation on error = %v", err)
	}
}

func TestGeoAndHyperLogLog(t *testing.T) {
	db := NewDB()
	points := []GeoPoint{{Longitude: 13.361389, Latitude: 38.115556, Member: "Palermo"}, {Longitude: 15.087269, Latitude: 37.502669, Member: "Catania"}}
	if result, err := db.GeoAdd("cities", points, ZAddOptions{}); err != nil || result.Count != 2 {
		t.Fatalf("GeoAdd = %#v, %v", result, err)
	}
	positions, err := db.GeoPos("cities", "Palermo", "missing")
	if err != nil || positions[0] == nil || positions[1] != nil || math.Abs(positions[0].Longitude-13.361389) > 0.001 {
		t.Fatalf("GeoPos = %#v, %v", positions, err)
	}
	distance, found, err := db.GeoDist("cities", "Palermo", "Catania")
	if err != nil || !found || math.Abs(distance/1000-166.274) > 2 {
		t.Fatalf("GeoDist = %f, %v, %v", distance, found, err)
	}
	results, err := db.GeoSearch("cities", GeoSearchOptions{FromMember: "Palermo", UseMember: true, Radius: 200000})
	if err != nil || len(results) != 2 || results[0].Member != "Palermo" {
		t.Fatalf("GeoSearch = %#v, %v", results, err)
	}
	if changed, err := db.PFAdd("hll-a", "one", "two", "three"); err != nil || !changed {
		t.Fatalf("PFAdd = %v, %v", changed, err)
	}
	if changed, err := db.PFAdd("hll-a", "one"); err != nil || changed {
		t.Fatalf("duplicate PFAdd = %v, %v", changed, err)
	}
	if _, err := db.PFAdd("hll-b", "three", "four"); err != nil {
		t.Fatal(err)
	}
	if count, err := db.PFCount("hll-a", "hll-b"); err != nil || count != 4 {
		t.Fatalf("PFCount = %d, %v", count, err)
	}
	if err := db.PFMerge("merged", "hll-a", "hll-b"); err != nil {
		t.Fatal(err)
	}
	if count, err := db.PFCount("merged"); err != nil || count != 4 {
		t.Fatalf("PFMerge count = %d, %v", count, err)
	}
	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewDB()
	if err := restored.LoadSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if count, err := restored.PFCount("merged"); err != nil || count != 4 {
		t.Fatalf("restored HLL = %d, %v", count, err)
	}
}

func TestJSONAOFAndHyperLogLogDensePromotion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "derived.aof")
	db := NewDB()
	if err := db.OpenAOF(path, FsyncAlways); err != nil {
		t.Fatal(err)
	}
	if _, err := db.JSONSet("doc", "$", []byte(`{"counter":1}`), false, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.JSONNumIncrBy("doc", "$.counter", "2"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SetBit("bits", 7, 1); err != nil {
		t.Fatal(err)
	}
	values := make([]string, 12000)
	for i := range values {
		values[i] = strconv.Itoa(i)
	}
	if _, err := db.PFAdd("hll", values...); err != nil {
		t.Fatal(err)
	}
	data, _, err := db.Get("hll")
	if err != nil || data[len(hllHeader)] != 1 {
		t.Fatalf("HLL was not promoted: %v", err)
	}
	if err := db.CloseAOF(); err != nil {
		t.Fatal(err)
	}
	recovered := NewDB()
	if err := recovered.OpenAOF(path, FsyncAlways); err != nil {
		t.Fatal(err)
	}
	defer recovered.CloseAOF()
	value, found, err := recovered.JSONGet("doc", "$.counter")
	if err != nil || !found || string(value) != "3" {
		t.Fatalf("recovered JSON = %s, %v, %v", value, found, err)
	}
	if bit, err := recovered.GetBit("bits", 7); err != nil || bit != 1 {
		t.Fatalf("recovered bitmap = %d, %v", bit, err)
	}
	if count, err := recovered.PFCount("hll"); err != nil || count < 10800 || count > 13200 {
		t.Fatalf("recovered dense HLL = %d, %v", count, err)
	}
}
