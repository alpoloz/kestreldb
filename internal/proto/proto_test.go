package proto

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestRESPCommandAndResponses(t *testing.T) {
	command := []byte("*3\r\n$3\r\nSET\r\n$3\r\na\x00b\r\n$6\r\nx\r\ny\x00z\r\n")
	got, mode, err := ReadRequest(bufio.NewReader(bytes.NewReader(command)))
	if err != nil {
		t.Fatal(err)
	}
	if mode != ModeRESP2 || !reflect.DeepEqual(got, []string{"SET", "a\x00b", "x\r\ny\x00z"}) {
		t.Fatalf("ReadRequest = %q, %v", got, mode)
	}
	var output bytes.Buffer
	w := NewWriter(bufio.NewWriter(&output))
	w.SetMode(ModeRESP2)
	if err := w.WriteArrayHeader(3); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBlobString([]byte("a\x00b")); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteFloat(1.25); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteNil(); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != "*3\r\n$3\r\na\x00b\r\n$4\r\n1.25\r\n$-1\r\n" {
		t.Fatalf("RESP2 response = %q", got)
	}
	response, err := ReadResponse(bufio.NewReader(&output))
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != RArray || len(response.Elements) != 3 || response.Elements[2].Type != RNil {
		t.Fatalf("parsed response = %#v", response)
	}
}

func TestBlobStringRoundTrip(t *testing.T) {
	value := []byte{'a', 0, '\n', '\r', 0xff}
	var encoded bytes.Buffer
	w := NewWriter(bufio.NewWriter(&encoded))
	if err := w.WriteBlobString(value); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	response, err := ReadResponse(bufio.NewReader(&encoded))
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != RBlobString || !bytes.Equal([]byte(response.Str), value) {
		t.Fatalf("ReadResponse() = %#v", response)
	}
}

func TestFormatCommandEscapesLineBreaks(t *testing.T) {
	want := []string{"SET", "key", "line one\nline two\r"}
	encoded := FormatCommand(want)
	got, err := ReadCommand(bufio.NewReader(bytes.NewBufferString(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadCommand(FormatCommand()) = %#v, want %#v", got, want)
	}
}

func TestInt64ResponseRoundTrip(t *testing.T) {
	const want int64 = -1 << 63
	var encoded bytes.Buffer
	w := NewWriter(bufio.NewWriter(&encoded))
	if err := w.WriteInt64(want); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	response, err := ReadResponse(bufio.NewReader(&encoded))
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != RInteger || response.Int != want {
		t.Fatalf("ReadResponse() = %#v, want %d", response, want)
	}
}
