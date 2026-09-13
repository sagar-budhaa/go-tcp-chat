// Command server: multi-client TCP chat with rooms. Clients introduce
// themselves with a hello frame naming a room; chat messages and
// join/leave notices stay within that room.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"tcp-chat/internal/protocol"
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
		if m.Type != protocol.TypeMsg || m.Body == "" {
			continue
		}
		// Stamp From and Room server-side; never trust the client's claim.
		h.broadcast(c.room, protocol.Message{V: 1, Type: protocol.TypeMsg, From: c.name, Room: c.room, Body: m.Body})
	}
}

func main() {
	addr := flag.String("addr", ":9000", "listen address, e.g. :9000 or 127.0.0.1:9000")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	defer ln.Close()
	fmt.Printf("tcp-chat server listening on %s\n", ln.Addr())

	h := &hub{rooms: make(map[string]map[string]*client)}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(h, conn)
	}
}
