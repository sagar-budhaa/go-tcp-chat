package store

import (
	"path/filepath"
	"testing"

	"tcp-chat/internal/protocol"
)

func TestRoundTripFile(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	msgs := []protocol.Message{
		{V: 1, Type: protocol.TypeMsg, From: "alice", Room: "general", Body: "hello"},
		{V: 1, Type: protocol.TypeDM, From: "alice", Room: "general", To: "bob", Body: "secret"},
		{V: 1, Type: protocol.TypeJoin, From: "bob", Room: "general"},
	}
	for _, m := range msgs {
		if err := s.Save(m); err != nil {
			t.Fatalf("Save(%+v): %v", m, err)
		}
	}
	got, err := s.Recent("general", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("Recent = %d msgs, want 2 (join not persisted)", len(got))
	}
	if got[0].Body != "hello" || got[1].To != "bob" {
		t.Fatalf("unexpected history: %+v", got)
	}
	empty, err := s.Recent("sports", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("other room history = %d, want 0", len(empty))
	}
}

func TestMemoryDB(t *testing.T) {
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Save(protocol.Message{V: 1, Type: protocol.TypeMsg, From: "a", Room: "r", Body: "x"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Recent("r", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Body != "x" {
		t.Fatalf("unexpected memory history: %+v", got)
	}
}
