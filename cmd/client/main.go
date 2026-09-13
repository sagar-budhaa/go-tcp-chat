// Command client: connects to a tcp-chat server room as --name, sends
// each stdin line as a chat message, prints messages and join/leave.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"tcp-chat/internal/protocol"
)

func print(m protocol.Message) {
	switch m.Type {
	case protocol.TypeMsg:
		fmt.Printf("[%s] %s\n> ", m.From, m.Body)
	case protocol.TypeJoin:
		fmt.Printf("*** %s joined #%s ***\n> ", m.From, m.Room)
	case protocol.TypeLeave:
		fmt.Printf("*** %s left #%s ***\n> ", m.From, m.Room)
	}
}

func main() {
	addr := flag.String("addr", "localhost:9000", "server address, e.g. localhost:9000")
	name := flag.String("name", "", "chat name (required, max 32 chars, unique per room)")
	room := flag.String("room", protocol.DefaultRoom, "room to join")
	flag.Parse()
	if *name == "" || len(*name) > protocol.MaxNameLen {
		log.Fatalf("need --name of 1-%d chars", protocol.MaxNameLen)
	}
	if *room == "" || len(*room) > protocol.MaxRoomLen {
		log.Fatalf("need --room of 1-%d chars", protocol.MaxRoomLen)
	}

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	if err := protocol.WriteJSON(conn, protocol.Message{V: 1, Type: protocol.TypeHello, From: *name, Room: *room}); err != nil {
		log.Fatalf("hello: %v", err)
	}
	// The server answers a bad handshake with an error frame, not silence.
	first, err := protocol.ReadJSON(conn)
	if err != nil {
		log.Fatalf("handshake: %v", err)
	}
	if first.Type == protocol.TypeError {
		log.Fatalf("server rejected: %s", first.Body)
	}
	print(first)
	fmt.Printf("connected to %s as %s in #%s (type /quit to exit)\n> ", *addr, *name, first.Room)

	// Reader: server -> stdout. Exits process on disconnect since stdin
	// would otherwise block forever with nowhere to send.
	go func() {
		for {
			m, err := protocol.ReadJSON(conn)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					fmt.Println("\nserver closed the connection")
				} else {
					fmt.Printf("\nconnection error: %v\n", err)
				}
				os.Exit(0)
			}
			if m.Type == protocol.TypeError {
				fmt.Printf("\nserver error: %s\n", m.Body)
				os.Exit(1)
			}
			print(m)
		}
	}()

	// Writer: stdin lines -> server.
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), protocol.MaxMessageSize)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "/quit" {
			return
		}
		if line == "" {
			fmt.Print("> ")
			continue
		}
		if err := protocol.WriteJSON(conn, protocol.Message{V: 1, Type: protocol.TypeMsg, Body: line}); err != nil {
			log.Fatalf("send: %v", err)
		}
		fmt.Print("> ")
	}
	if err := scanner.Err(); err != nil {
		log.Printf("stdin: %v", err)
	}
}
