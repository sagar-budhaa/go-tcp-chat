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
