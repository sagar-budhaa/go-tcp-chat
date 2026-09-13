// Package store persists chat history in SQLite (pure-Go modernc driver).
// Only room messages and DMs are stored; join/leave/error frames are ephemeral.
package store

import (
	"database/sql"
	"time"

	_ "modernc.org/sqlite"

	"tcp-chat/internal/protocol"
)

// historyLimit caps how many messages are replayed to a newcomer.
const historyLimit = 50

// Store wraps a SQLite DB holding chat history.
type Store struct {
	db *sql.DB
}

// Open creates/opens the DB at path and ensures the schema exists.
// Use ":memory:" for tests.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		room TEXT NOT NULL,
		from_name TEXT NOT NULL,
		to_name TEXT NOT NULL DEFAULT '',
		type TEXT NOT NULL,
		body TEXT NOT NULL,
		ts INTEGER NOT NULL
	)`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_messages_room_id ON messages (room, id)`); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close releases the DB handle.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Save persists one msg/dm envelope. No-op for other types or empty bodies.
// Fail-open is the caller's choice; Save itself returns the error.
func (s *Store) Save(m protocol.Message) error {
	if s == nil {
		return nil
	}
	if m.Type != protocol.TypeMsg && m.Type != protocol.TypeDM {
		return nil
	}
	if m.Body == "" || m.Room == "" {
		return nil
	}
	_, err := s.db.Exec(
		`INSERT INTO messages (room, from_name, to_name, type, body, ts) VALUES (?, ?, ?, ?, ?, ?)`,
		m.Room, m.From, m.To, m.Type, m.Body, time.Now().UnixMilli(),
	)
	return err
}

// Recent returns up to limit persisted msg/dm frames for room, oldest first.
// Public messages replay for everyone; DMs only replay for their sender or
// recipient so one rejoiner never sees another pair's private context.
func (s *Store) Recent(room, user string, limit int) ([]protocol.Message, error) {
	if s == nil {
		return nil, nil
	}
	if limit <= 0 || limit > historyLimit {
		limit = historyLimit
	}
	rows, err := s.db.Query(
		`SELECT from_name, to_name, type, body FROM messages
		 WHERE room = ? AND (type != 'dm' OR from_name = ? OR to_name = ?)
		 ORDER BY id DESC LIMIT ?`,
		room, user, user, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rev []protocol.Message
	for rows.Next() {
		var m protocol.Message
		if err := rows.Scan(&m.From, &m.To, &m.Type, &m.Body); err != nil {
			return nil, err
		}
		m.V = 1
		m.Room = room
		rev = append(rev, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return rev, nil
}
