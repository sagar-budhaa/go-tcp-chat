// Package protocol implements the v1 wire framing: raw bytes with a
// 4-byte big-endian length prefix. Payload is opaque (v1: UTF-8 text).
package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MaxMessageSize caps a single payload at 1 MiB to avoid OOM from a
// malicious or buggy peer declaring a huge length.
const MaxMessageSize = 1 << 20

// WriteMessage writes one length-prefixed frame to w.
func WriteMessage(w io.Writer, payload []byte) error {
	if len(payload) > MaxMessageSize {
		return fmt.Errorf("protocol: payload %d bytes excexeds max %d", len(payload), MaxMessageSize)
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
