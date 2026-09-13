// Command server: multi-client TCP relay. Every length-prefixed message
// received from any client is broadcast to ALL connected clients.
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

type client struct {
	conn net.Conn
	send chan []byte
}

type hub struct {
	mu      sync.Mutex
	clients map[*client]struct{}
}

func (h *hub) add(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	n := len(h.clients)
	h.mu.Unlock()
	log.Printf("client connected: %s (%d total)", c.conn.RemoteAddr(), n)
}

func (h *hub) remove(c *client) {
	h.mu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
	n := len(h.clients)
	h.mu.Unlock()
	log.Printf("client disconnected: %s (%d total)", c.conn.RemoteAddr(), n)
}

// broadcast copies payload to every connected client's queue, including
// the sender. Slow clients with a full queue are disconnected instead of
// blocking everyone else.
func (h *hub) broadcast(payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c.send <- payload:
		default:
			// Queue full: drop this client; its pumps will exit and
			// remove it via defer. Closing conn unblocks its reader.
			log.Printf("client slow, disconnecting: %s", c.conn.RemoteAddr())
			go c.conn.Close()
			delete(h.clients, c)
			close(c.send)
		}
	}
}

// writer pumps queued messages to the socket. Single writer per conn, so
// concurrent broadcasts can't interleave frames.
func writer(c *client) {
	for payload := range c.send {
		_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := protocol.WriteMessage(c.conn, payload); err != nil {
			break
		}
	}
	// Ensure the reader side also exits.
	c.conn.Close()
}

func handleConn(h *hub, conn net.Conn) {
	c := &client{conn: conn, send: make(chan []byte, sendQueueSize)}
	h.add(c)
	defer h.remove(c)
	go writer(c)
	for {
		payload, err := protocol.ReadMessage(conn)
		if err != nil {
			return // EOF or corrupt frame: just drop the client
		}
		if len(payload) == 0 {
			continue
		}
		log.Printf("broadcast %d bytes from %s", len(payload), conn.RemoteAddr())
		h.broadcast(payload)
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

	h := &hub{clients: make(map[*client]struct{})}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(h, conn)
	}
}
