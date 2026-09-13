// Command server: multi-client TCP chat with rooms, SQLite history,
// optional TLS, and a WebSocket gateway serving the web GUI.
// TCP clients speak length-prefixed JSON envelopes; browsers speak the
// same envelopes over WebSocket (one WS message = one envelope).
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"

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
	conn net.Conn // nil for WebSocket peers
	ws   *websocket.Conn
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

// persist saves msg/dm history, fail-open so a DB hiccup never drops chat.
func (h *hub) persist(m protocol.Message) {
	if h.store == nil {
		return
	}
	if err := h.store.Save(m); err != nil {
		log.Printf("history save: %v", err)
	}
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
			log.Printf("client slow, disconnecting: %s", c.name)
			h.dropLocked(room, c)
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
		log.Printf("client slow, disconnecting: %s", c.name)
		h.dropLocked(room, c)
		return false
	}
}

// dropLocked removes c and closes its transports. Caller must hold h.mu.
func (h *hub) dropLocked(room string, c *client) {
	delete(h.rooms[room], c.name)
	close(c.send)
	if c.conn != nil {
		go c.conn.Close()
	}
	if c.ws != nil {
		go c.ws.Close(websocket.StatusPolicyViolation, "slow reader")
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

// add registers name in room, enforcing per-room uniqueness.
func (h *hub) add(name, room string, tcp net.Conn, ws *websocket.Conn) (*client, error) {
	c := &client{conn: tcp, ws: ws, name: name, room: room, send: make(chan protocol.Message, sendQueueSize)}
	h.mu.Lock()
	defer h.mu.Unlock()
	members := h.members(room)
	if _, taken := members[name]; taken {
		return nil, fmt.Errorf("name %q is already taken in room %q", name, room)
	}
	members[name] = c
	return c, nil
}

// remove unregisters c and reports whether it was present.
func (h *hub) remove(c *client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.rooms[c.room][c.name]; ok && cur == c {
		delete(h.rooms[c.room], c.name)
		close(c.send)
		return true
	}
	return false
}

// route relays one validated client frame, stamped server-side.
func (h *hub) route(c *client, m protocol.Message) {
	switch m.Type {
	case protocol.TypeMsg:
		if m.Body == "" {
			return
		}
		out := protocol.Message{V: 1, Type: protocol.TypeMsg, From: c.name, Room: c.room, Body: m.Body}
		h.persist(out)
		h.broadcast(c.room, out)
	case protocol.TypeDM:
		if m.Body == "" || m.To == "" {
			return
		}
		out := protocol.Message{V: 1, Type: protocol.TypeDM, From: c.name, Room: c.room, To: m.To, Body: m.Body}
		h.persist(out)
		if m.To == c.name {
			_ = h.direct(c.room, c.name, out)
			return
		}
		if !h.direct(c.room, m.To, out) {
			h.sendError(c, fmt.Sprintf("no user %q in room %q", m.To, c.room))
			return
		}
		_ = h.direct(c.room, c.name, out)
	}
}

// tcpWriter pumps queued messages to a raw TCP socket. Single writer per
// conn, so concurrent broadcasts can't interleave frames.
func tcpWriter(c *client) {
	for m := range c.send {
		_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := protocol.WriteJSON(c.conn, m); err != nil {
			break
		}
	}
	c.conn.Close()
}

// wsWriter pumps queued messages to a WebSocket peer.
func wsWriter(ctx context.Context, c *client) {
	for m := range c.send {
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := wsjson.Write(wctx, c.ws, m)
		cancel()
		if err != nil {
			break
		}
	}
	c.ws.Close(websocket.StatusNormalClosure, "")
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

	room := hello.Room
	if room == "" {
		room = protocol.DefaultRoom
	}
	c, err := h.add(hello.From, room, conn, nil)
	if err != nil {
		reject(conn, err.Error())
		return
	}

	// Replay history straight to the newcomer before its writer starts.
	if h.store != nil {
		if hist, herr := h.store.Recent(c.room, 50); herr != nil {
			log.Printf("history replay: %v", herr)
		} else {
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			for _, m := range hist {
				if werr := protocol.WriteJSON(conn, m); werr != nil {
					conn.Close()
					return
				}
			}
		}
	}

	go tcpWriter(c)
	// Announce after registering so the newcomer also sees its own join.
	h.broadcast(c.room, protocol.Message{V: 1, Type: protocol.TypeJoin, From: c.name, Room: c.room})
	log.Printf("client connected: %s to #%s (%s)", c.name, c.room, conn.RemoteAddr())

	defer func() {
		if h.remove(c) {
			h.broadcast(c.room, protocol.Message{V: 1, Type: protocol.TypeLeave, From: c.name, Room: c.room})
			log.Printf("client disconnected: %s from #%s", c.name, c.room)
		}
	}()

	for {
		m, err := protocol.ReadJSON(conn)
		if err != nil {
			return // EOF or bad frame: just drop the client
		}
		h.route(c, m)
	}
}

// wsHandler bridges browsers to the hub: one WS message = one envelope.
// Identity comes from query params: /ws?name=alice&room=sports.
func wsHandler(h *hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		room := r.URL.Query().Get("room")
		if room == "" {
			room = protocol.DefaultRoom
		}
		if name == "" || len(name) > protocol.MaxNameLen || len(room) > protocol.MaxRoomLen {
			http.Error(w, "need ?name= (1-32 chars) and ?room= (<=32 chars)", http.StatusBadRequest)
			return
		}
		c, err := h.add(name, room, nil, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			h.remove(c)
			return
		}
		c.ws = ws
		ctx := r.Context()

		if h.store != nil {
			if hist, herr := h.store.Recent(c.room, 50); herr != nil {
				log.Printf("history replay: %v", herr)
			} else {
				for _, m := range hist {
					wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
					werr := wsjson.Write(wctx, ws, m)
					cancel()
					if werr != nil {
						h.remove(c)
						ws.Close(websocket.StatusInternalError, "")
						return
					}
				}
			}
		}

		go wsWriter(ctx, c)
		h.broadcast(c.room, protocol.Message{V: 1, Type: protocol.TypeJoin, From: c.name, Room: c.room})
		log.Printf("web client connected: %s to #%s", c.name, c.room)

		defer func() {
			if h.remove(c) {
				h.broadcast(c.room, protocol.Message{V: 1, Type: protocol.TypeLeave, From: c.name, Room: c.room})
				log.Printf("web client disconnected: %s from #%s", c.name, c.room)
			}
		}()

		for {
			var m protocol.Message
			if err := wsjson.Read(ctx, ws, &m); err != nil {
				return
			}
			if err := m.Validate(); err != nil {
				continue
			}
			h.route(c, m)
		}
	}
}

func main() {
	addr := flag.String("addr", ":9000", "TCP listen address, e.g. :9000 or 127.0.0.1:9000")
	dbPath := flag.String("db", "chat.db", "sqlite history file (persisted per room)")
	tlsCert := flag.String("tls-cert", "", "TLS cert file (enables TLS when set with --tls-key)")
	tlsKey := flag.String("tls-key", "", "TLS key file")
	wsAddr := flag.String("ws-addr", "", "HTTP listen address for web GUI + websocket, e.g. :8080 (disabled when empty)")
	webDir := flag.String("web-dir", "web", "directory serving the web GUI")
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open db %s: %v", *dbPath, err)
	}
	defer st.Close()

	h := &hub{rooms: make(map[string]map[string]*client), store: st}

	if *wsAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/ws", wsHandler(h))
		mux.Handle("/", http.FileServer(http.Dir(*webDir)))
		srv := &http.Server{Addr: *wsAddr, Handler: mux}
		go func() {
			if *tlsCert != "" && *tlsKey != "" {
				log.Printf("web GUI serving https://%s from %s", *wsAddr, *webDir)
				if err := srv.ListenAndServeTLS(*tlsCert, *tlsKey); err != nil && err != http.ErrServerClosed {
					log.Fatalf("web https: %v", err)
				}
			} else {
				log.Printf("web GUI serving http://%s from %s", *wsAddr, *webDir)
				if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Fatalf("web http: %v", err)
				}
			}
		}()
	}

	var ln net.Listener
	if *tlsCert != "" || *tlsKey != "" {
		if *tlsCert == "" || *tlsKey == "" {
			log.Fatalf("need both --tls-cert and --tls-key")
		}
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			log.Fatalf("load tls: %v", err)
		}
		ln, err = tls.Listen("tcp", *addr, &tls.Config{Certificates: []tls.Certificate{cert}})
		if err != nil {
			log.Fatalf("listen tls %s: %v", *addr, err)
		}
		fmt.Printf("tcp-chat server listening with TLS on %s (db %s)\n", ln.Addr(), *dbPath)
	} else {
		ln, err = net.Listen("tcp", *addr)
		if err != nil {
			log.Fatalf("listen %s: %v", *addr, err)
		}
		fmt.Printf("tcp-chat server listening on %s (db %s)\n", ln.Addr(), *dbPath)
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(h, conn)
	}
}
