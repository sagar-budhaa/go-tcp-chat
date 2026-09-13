// Command server: multi-client TCP chat with rooms. Clients introduce
// themselves with a hello frame naming a room; chat messages and
// join/leave notices stay within that room. Recent history is replayed
// from SQLite on join.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"tcp-chat/internal/protocol"
	"tcp-chat/internal/store"
)

// sendQueueSize bounds per-client backlog so one slow reader can never
// stall the hub or other clients.
const sendQueueSize = 32

// handshakeTimeout bounds the hello frame so half-open connections
// can't accumulate.
const handshakeTimeout = 10 * time.Second

type client struct {
	conn net.Conn
	name string
	room string
	send chan protocol.Message
}

type hub struct {
	mu sync.Mutex
	// rooms maps room -> name -> client. Names need only be unique
	// within their room.
	rooms map[string]map[string]*client
	store *store.Store
}

// persist saves msg/dm history, fail-open so a DB hiccup never drops chat.
func (h *hub) persist(m protocol.Message) {
	if h.store == nil {
		return
	}
	if err := h.store.Save(m); err != nil {
		log.Printf("history save: %v", err)
	}
}

// members returns the room's set, creating it on first use. Caller must
// hold h.mu.
func (h *hub) members(room string) map[string]*client {
	m, ok := h.rooms[room]
	if !ok {
		m = make(map[string]*client)
		h.rooms[room] = m
	}
	return m
}

func (h *hub) broadcast(room string, m protocol.Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.rooms[room] {
		select {
		case c.send <- m:
		default:
			// Queue full: drop this client; its pumps will exit and
			// remove it. Closing conn unblocks its reader.
			log.Printf("client slow, disconnecting: %s (%s)", c.name, c.conn.RemoteAddr())
			go c.conn.Close()
			delete(h.rooms[room], c.name)
			close(c.send)
		}
	}
}

// direct queues m for one named member of room. Returns false when the
// target is absent or its queue is full (caller drops slow clients).
func (h *hub) direct(room, name string, m protocol.Message) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.rooms[room][name]
	if !ok {
		return false
	}
	select {
	case c.send <- m:
		return true
	default:
		log.Printf("client slow, disconnecting: %s (%s)", c.name, c.conn.RemoteAddr())
		go c.conn.Close()
		delete(h.rooms[room], c.name)
		close(c.send)
		return false
	}
}

// sendError queues a non-fatal error frame to one client without closing.
// Dropped when the queue is full; the slow-client path handles cleanup.
func (h *hub) sendError(c *client, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.rooms[c.room][c.name]; !ok || cur != c {
		return
	}
	select {
	case c.send <- protocol.Message{V: 1, Type: protocol.TypeError, Body: reason}:
	default:
	}
}

// writer pumps queued messages to the socket. Single writer per conn, so
// concurrent broadcasts can't interleave frames.
func writer(c *client) {
	for m := range c.send {
		_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := protocol.WriteJSON(c.conn, m); err != nil {
			break
		}
	}
	// Ensure the reader side also exits.
	c.conn.Close()
}

// reject sends one error frame and closes, telling the client why its
// handshake failed instead of hanging up silently.
func reject(conn net.Conn, reason string) {
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_ = protocol.WriteJSON(conn, protocol.Message{V: 1, Type: protocol.TypeError, Body: reason})
	conn.Close()
}

func handleConn(h *hub, conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(handshakeTimeout))
	hello, err := protocol.ReadJSON(conn)
	if err != nil {
		conn.Close()
		return
	}
	if hello.Type != protocol.TypeHello || hello.From == "" {
		reject(conn, "first frame must be a hello with a name")
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	c := &client{conn: conn, name: hello.From, send: make(chan protocol.Message, sendQueueSize)}
	c.room = hello.Room
	if c.room == "" {
		c.room = protocol.DefaultRoom
	}

	h.mu.Lock()
	members := h.members(c.room)
	if _, taken := members[c.name]; taken {
		h.mu.Unlock()
		reject(conn, fmt.Sprintf("name %q is already taken in room %q", c.name, c.room))
		return
	}
	members[c.name] = c
	h.mu.Unlock()

	// Replay recent history straight to the newcomer before its writer
	// starts, so only it sees the backlog and frames can't interleave.
	if h.store != nil {
		if hist, err := h.store.Recent(c.room, 50); err != nil {
			log.Printf("history replay: %v", err)
		} else {
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			for _, m := range hist {
				if err := protocol.WriteJSON(conn, m); err != nil {
					conn.Close()
					return
				}
			}
		}
	}

	// Announce after registering so the newcomer also sees its own join.
	h.broadcast(c.room, protocol.Message{V: 1, Type: protocol.TypeJoin, From: c.name, Room: c.room})
	log.Printf("client connected: %s to #%s (%s)", c.name, c.room, conn.RemoteAddr())

	removed := false
	defer func() {
		h.mu.Lock()
		if cur, ok := h.rooms[c.room][c.name]; ok && cur == c {
			delete(h.rooms[c.room], c.name)
			close(c.send)
			removed = true
		}
		h.mu.Unlock()
		if removed {
			h.broadcast(c.room, protocol.Message{V: 1, Type: protocol.TypeLeave, From: c.name, Room: c.room})
			log.Printf("client disconnected: %s from #%s (%s)", c.name, c.room, conn.RemoteAddr())
		}
	}()

	go writer(c)
	for {
		m, err := protocol.ReadJSON(conn)
		if err != nil {
			return // EOF or bad frame: just drop the client
		}
		switch m.Type {
		case protocol.TypeMsg:
			if m.Body == "" {
				continue
			}
			// Stamp From and Room server-side; never trust the client's claim.
			out := protocol.Message{V: 1, Type: protocol.TypeMsg, From: c.name, Room: c.room, Body: m.Body}
			h.persist(out)
			h.broadcast(c.room, out)
		case protocol.TypeDM:
			if m.Body == "" || m.To == "" {
				continue
			}
			out := protocol.Message{V: 1, Type: protocol.TypeDM, From: c.name, Room: c.room, To: m.To, Body: m.Body}
			h.persist(out)
			if m.To == c.name {
				_ = h.direct(c.room, c.name, out)
				continue
			}
			if !h.direct(c.room, m.To, out) {
				h.sendError(c, fmt.Sprintf("no user %q in room %q", m.To, c.room))
				continue
			}
			// Echo to sender so its UI can show what was sent.
			_ = h.direct(c.room, c.name, out)
		default:
			continue
		}
	}
}

func main() {
	addr := flag.String("addr", ":9000", "listen address, e.g. :9000 or 127.0.0.1:9000")
	dbPath := flag.String("db", "chat.db", "sqlite history file (persisted per room)")
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open db %s: %v", *dbPath, err)
	}
	defer st.Close()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	defer ln.Close()
	fmt.Printf("tcp-chat server listening on %s (db %s)\n", ln.Addr(), *dbPath)

	h := &hub{rooms: make(map[string]map[string]*client), store: st}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(h, conn)
	}
}
