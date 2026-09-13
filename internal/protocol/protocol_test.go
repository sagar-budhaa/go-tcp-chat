package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte("hello"),
		[]byte{},
		bytes.Repeat([]byte("a"), MaxMessageSize),
	} {
		var buf bytes.Buffer
		if err := WriteMessage(&buf, payload); err != nil {
			t.Fatalf("WriteMessage(%d bytes): %v", len(payload), err)
		}
		got, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("round trip mismatch: got %d bytes, want %d", len(got), len(payload))
		}
	}
}

func TestHeaderIsBigEndianLength(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	if len(raw) != 4+3 {
		t.Fatalf("framed size = %d, want 7", len(raw))
	}
	if n := binary.BigEndian.Uint32(raw[:4]); n != 3 {
		t.Fatalf("header length = %d, want 3", n)
	}
}

func TestWriteRejectsOversize(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, bytes.Repeat([]byte("x"), MaxMessageSize+1)); err == nil {
		t.Fatal("expected error for oversize payload, got nil")
	}
}

func TestReadRejectsOversizeFrame(t *testing.T) {
	var buf bytes.Buffer
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], MaxMessageSize+1)
	buf.Write(hdr[:])
	if _, err := ReadMessage(&buf); err == nil {
		t.Fatal("expected error for oversize frame, got nil")
	}
}

func TestReadTruncatedStream(t *testing.T) {
	var buf bytes.Buffer
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 10)
	buf.Write(hdr[:])
	buf.Write([]byte("short"))
	if _, err := ReadMessage(&buf); err == nil {
		t.Fatal("expected error for truncated payload, got nil")
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	for _, m := range []Message{
		{V: 1, Type: TypeHello, From: "alice"},
		{V: 1, Type: TypeMsg, From: "alice", Body: "hello"},
		{V: 1, Type: TypeJoin, From: "bob"},
		{V: 1, Type: TypeLeave, From: "bob"},
		{V: 1, Type: TypeError, Body: "name taken"},
		{V: 1, Type: TypeHello, From: "alice", Room: "sports"},
		{V: 1, Type: TypeMsg, From: "alice", Room: "sports", Body: "go team"},
		{V: 1, Type: TypeDM, From: "alice", Room: "general", To: "bob", Body: "secret"},
	} {
		var buf bytes.Buffer
		if err := WriteJSON(&buf, m); err != nil {
			t.Fatalf("WriteJSON(%+v): %v", m, err)
		}
		got, err := ReadJSON(&buf)
		if err != nil {
			t.Fatalf("ReadJSON: %v", err)
		}
		if got != m {
			t.Fatalf("round trip mismatch: got %+v, want %+v", got, m)
		}
	}
}

func TestEnvelopeValidation(t *testing.T) {
	var buf bytes.Buffer
	for _, m := range []Message{
		{V: 2, Type: TypeMsg, Body: "bad version"},
		{V: 1, Type: "nope", Body: "unknown type"},
		{V: 1, Type: TypeHello, From: string(bytes.Repeat([]byte("n"), MaxNameLen+1))},
		{V: 1, Type: TypeHello, From: "alice", Room: string(bytes.Repeat([]byte("r"), MaxRoomLen+1))},
		{V: 1, Type: TypeDM, From: "alice", Body: "missing target"},
		{V: 1, Type: TypeDM, From: "alice", To: string(bytes.Repeat([]byte("n"), MaxNameLen+1)), Body: "long target"},
		{V: 1, Type: TypeMsg, From: "alice", To: "bob", Body: "to only valid on dm"},
	} {
		buf.Reset()
		if err := WriteJSON(&buf, m); err == nil {
			t.Fatalf("WriteJSON(%+v): expected error, got nil", m)
		}
	}
}

func TestReadRejectsNonEnvelope(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, []byte("not json")); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadJSON(&buf); err == nil {
		t.Fatal("expected error for non-JSON frame, got nil")
	}
}
