// Command client: connects to a tcp-chat server, sends each stdin line as
// one length-prefixed message, prints received broadcasts to stdout.
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

func main() {
	addr := flag.String("addr", "localhost:9000", "server address, e.g. localhost:9000")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()
	fmt.Printf("connected to %s (type /quit to exit)\n", *addr)

	// Reader: server -> stdout. Exits process on disconnect since stdin
	// would otherwise block forever with nowhere to send.
	go func() {
		for {
			payload, err := protocol.ReadMessage(conn)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					fmt.Println("\nserver closed the connection")
				} else {
					fmt.Printf("\nconnection error: %v\n", err)
				}
				os.Exit(0)
			}
			fmt.Printf("%s\n> ", payload)
		}
	}()

	// Writer: stdin lines -> server.
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), protocol.MaxMessageSize)
	fmt.Print("> ")
	for scanner.Scan() {
		line := scanner.Text()
		if line == "/quit" {
			return
		}
		if line == "" {
			fmt.Print("> ")
			continue
		}
		if err := protocol.WriteMessage(conn, []byte(line)); err != nil {
			log.Fatalf("send: %v", err)
		}
		fmt.Print("> ")
	}
	if err := scanner.Err(); err != nil {
		log.Printf("stdin: %v", err)
	}
}
