package server

import (
	"testing"

	"kestreldb/internal/engine"
	"kestreldb/internal/proto"
)

func TestJSONDerivedCommands(t *testing.T) {
	h := &handler{db: engine.NewDB()}
	assertStringResponse(t, runCommand(t, h, "JSON.SET", "doc", "$", `{"name":"Ada","numbers":[1,2],"score":3}`), "OK")
	assertStringResponse(t, runCommand(t, h, "TYPE", "doc"), "ReJSON-RL")
	assertBlobResponse(t, runCommand(t, h, "JSON.GET", "doc", "$.name"), `["Ada"]`)
	assertBlobResponse(t, runCommand(t, h, "JSON.GET", "doc", ".name"), `"Ada"`)
	assertIntResponse(t, runCommand(t, h, "JSON.ARRAPPEND", "doc", ".numbers", "3"), 3)
	assertBlobResponse(t, runCommand(t, h, "JSON.NUMINCRBY", "doc", ".score", "2"), "5")
	assertIntResponse(t, runCommand(t, h, "JSON.ARRLEN", "doc", ".numbers"), 3)
	assertIntResponse(t, runCommand(t, h, "JSON.DEL", "doc", ".numbers[1]"), 1)
	assertBlobResponse(t, runCommand(t, h, "JSON.GET", "doc", ".numbers"), `[1,3]`)
	if response := runCommand(t, h, "JSON.SET", "doc", ".name", `invalid`); response.Type != proto.RError {
		t.Fatalf("invalid JSON response = %#v", response)
	}
	assertBlobResponse(t, runCommand(t, h, "JSON.GET", "doc", ".name"), `"Ada"`)
	if response := runCommand(t, h, "GET", "doc"); response.Type != proto.RError {
		t.Fatalf("wrong-type response = %#v", response)
	}

	assertIntResponse(t, runCommand(t, h, "SETBIT", "bits", "9", "1"), 0)
	assertIntResponse(t, runCommand(t, h, "GETBIT", "bits", "9"), 1)
	assertIntResponse(t, runCommand(t, h, "BITCOUNT", "bits"), 1)
	field := runCommand(t, h, "BITFIELD", "bits", "GET", "u2", "8", "SET", "u2", "8", "3", "GET", "u2", "8")
	if field.Type != proto.RArray || len(field.Elements) != 3 || field.Elements[0].Int != 1 || field.Elements[1].Int != 1 || field.Elements[2].Int != 3 {
		t.Fatalf("BITFIELD = %#v", field)
	}
	assertIntResponse(t, runCommand(t, h, "BITOP", "XOR", "xor", "bits", "bits"), 2)
	assertIntResponse(t, runCommand(t, h, "BITCOUNT", "xor"), 0)

	assertIntResponse(t, runCommand(t, h, "GEOADD", "cities", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"), 2)
	positions := runCommand(t, h, "GEOPOS", "cities", "Palermo", "missing")
	if positions.Type != proto.RArray || len(positions.Elements) != 2 || positions.Elements[0].Type != proto.RArray || positions.Elements[1].Type != proto.RNil {
		t.Fatalf("GEOPOS = %#v", positions)
	}
	search := runCommand(t, h, "GEOSEARCH", "cities", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "ASC", "WITHDIST")
	if search.Type != proto.RArray || len(search.Elements) != 2 || search.Elements[0].Elements[0].Str != "Palermo" {
		t.Fatalf("GEOSEARCH = %#v", search)
	}
	assertIntResponse(t, runCommand(t, h, "PFADD", "hll", "one", "two", "three"), 1)
	assertIntResponse(t, runCommand(t, h, "PFADD", "hll", "one"), 0)
	assertIntResponse(t, runCommand(t, h, "PFCOUNT", "hll"), 3)
	assertStringResponse(t, runCommand(t, h, "PFMERGE", "merged", "hll"), "OK")
	assertIntResponse(t, runCommand(t, h, "PFCOUNT", "merged"), 3)
}
