# go-tcp-chat

Small TCP chat with rooms, written in Go. Terminal client plus a browser GUI that talk to the same server. History is kept in SQLite and replayed when you join.

## Layout

- `cmd/server` — chat server (TCP + optional web/websocket)
- `cmd/client` — terminal client
- `cmd/gencert` — makes a dev self-signed cert
- `internal/protocol` — message envelopes
- `internal/store` — sqlite history
- `web/` — browser GUI

Needs Go 1.25.

## Run it

Start the server (web GUI on :8080):

```
go run ./cmd/server --addr :9000 --ws-addr :8080 --web-dir web
```

Join from a terminal:

```
go run ./cmd/client --addr localhost:9000 --name alice --room general
```

Or open `http://localhost:8080`, type a name and room, hit join.

## Commands

- Just type and hit enter to send to the room
- `/dm <name> <text>` — direct message to someone in the same room
- `/quit` — leave

Names and rooms are max 32 chars. Names have to be unique per room.

## TLS (dev only)

```
go run ./cmd/gencert
go run ./cmd/server --addr :9000 --tls-cert cert.pem --tls-key key.pem
go run ./cmd/client --addr localhost:9000 --name alice --tls --insecure
```

`--insecure` just skips verification for the self-signed cert. Don't use these certs for anything real.

## Notes

- History lives in `chat.db`, last 50 messages per room. It's gitignored, so each checkout starts fresh.
- `cert.pem` / `key.pem` are also gitignored.
