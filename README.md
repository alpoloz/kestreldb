# kestreldb

A minimal in-memory database in Go with two data structures:

- `hash` (map of fields to string values)
- `sorted set` (`zset`) backed by a skip list

It includes:

- REST HTTP server
- Go CLI client

## Run

Start server:

```bash
go run ./cmd/server -addr 127.0.0.1:6380
```

Run one-shot client commands:

```bash
go run ./cmd/client -addr 127.0.0.1:6380 PING
go run ./cmd/client -addr 127.0.0.1:6380 HSET user:1 name alice
go run ./cmd/client -addr 127.0.0.1:6380 HGET user:1 name
go run ./cmd/client -addr 127.0.0.1:6380 ZADD ranks 10 alice
go run ./cmd/client -addr 127.0.0.1:6380 ZRANGEWITHSCORES ranks 0 -1
```

Run interactive client:

```bash
go run ./cmd/client -addr 127.0.0.1:6380
```

## REST API

- `GET /v1/ping`
- `PUT /v1/hashes/{key}/fields/{field}` body: `{"value":"..."}`
- `GET /v1/hashes/{key}/fields/{field}`
- `DELETE /v1/hashes/{key}/fields/{field}`
- `GET /v1/hashes/{key}/length`
- `GET /v1/hashes/{key}`
- `PUT /v1/sorted-sets/{key}/members/{member}` body: `{"score":10}`
- `DELETE /v1/sorted-sets/{key}/members/{member}`
- `GET /v1/sorted-sets/{key}/members/{member}`
- `GET /v1/sorted-sets/{key}/cardinality`
- `GET /v1/sorted-sets/{key}/range?start=0&stop=-1`

## Supported commands

General:

- `PING`
- `QUIT` (client-local command to exit REPL)

Hash:

- `HSET key field value`
- `HGET key field`
- `HDEL key field`
- `HLEN key`
- `HGETALL key`

Sorted set:

- `ZADD key score member`
- `ZREM key member`
- `ZSCORE key member`
- `ZCARD key`
- `ZRANGE key start stop`
- `ZRANGEWITHSCORES key start stop`

`ZRANGE` supports negative indexes (e.g. `0 -1` for all members).
