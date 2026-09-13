// Package protocol implements the wire framing: a 4-byte big-endian
// length prefix around a JSON-encoded Message envelope.
package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// MaxMessageSize caps a single frame at 1 MiB to avoid OOM from a
// malicious or buggy peer declaring a huge length.
const MaxMessageSize = 1 << 20

// MaxNameLen caps client names; enforced during validation.
const MaxNameLen = 32

// Message types on the wire.
const (
	TypeHello = "hello" // client -> server, first frame, From = wanted name
	TypeMsg   = "msg"   // both directions; server stamps From itself
	TypeJoin  = "join"  // server -> all, From = newcomer
	TypeLeave = "leave" // server -> all, From = departee
	TypeError = "error" // server -> client, Body = reason, then close
)

// Message is the JSON envelope carried inside every frame.
type Message struct {
	V    int    `json:"v"`
	From string `json:"from,omitempty"`
	Type string `json:"type"`
	Body string `json:"body,omitempty"`
}

// Validate checks version, type, and field sizes.
func (m Message) Validate() error {
	if m.V != 1 {
		return fmt.Errorf("protocol: unsupported version %d", m.V)
	}
	switch m.Type {
	case TypeHello, TypeMsg, TypeJoin, TypeLeave, TypeError:
	default:
		return fmt.Errorf("protocol: unknown type %q", m.Type)
	}
	if len(m.From) > MaxNameLen {
		return fmt.Errorf("protocol: name %q exceeds max %d", m.From, MaxNameLen)
	}
	return nil
}

// WriteMessage writes one length-prefixed frame to w.
func WriteMessage(w io.Writer, payload []byte) error {
	if len(payload) > MaxMessageSize {
		return fmt.Errorf("protocol: payload %d bytes exceeds max %d", len(payload), MaxMessageSize)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// ReadMessage reads one length-prefixed frame from r, handling partial
// TCP reads via io.ReadFull.
func ReadMessage(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxMessageSize {
		return nil, fmt.Errorf("protocol: frame length %d exceeds max %d", n, MaxMessageSize)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// WriteJSON marshals m and sends it as one frame.
func WriteJSON(w io.Writer, m Message) error {
	if err := m.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return WriteMessage(w, raw)
}

// ReadJSON reads one frame and unmarshals it into a validated Message.
func ReadJSON(r io.Reader) (Message, error) {
	var m Message
	raw, err := ReadMessage(r)
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, fmt.Errorf("protocol: bad envelope: %w", err)
	}
	if err := m.Validate(); err != nil {
		return m, err
	}
	return m, nil
}
