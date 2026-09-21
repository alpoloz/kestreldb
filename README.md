# KestrelDB

KestrelDB is an in-memory key-value datastore written in Go. It provides a typed, sharded keyspace with expiration, persistence, replication, and cluster-aware request routing.

KestrelDB supports the RESP2 and RESP3 protocols, along with a simple line-oriented protocol used by the included command-line client.

## Features

- Strings, hashes, lists, sets, and sorted sets
- Key, hash-field, and set-member expiration
- Blocking list operations with cancellation and timeouts
- A 64-shard keyspace with atomic multi-key operations
- Versioned snapshots and append-only persistence
- Primary-replica synchronization and reconnect catch-up
- Cluster hash slots, hash tags, redirects, and cross-slot validation
- Binary-safe keys and values

## Requirements

- Go 1.24 or newer

## Build and test

```sh
make build
make test
make lint
```

Run the race detector for concurrency-sensitive changes:

```sh
go test -race ./...
```

## Start the server

```sh
go run ./cmd/server -addr 127.0.0.1:6380
```

The server listens on `127.0.0.1:6380` by default.

## Use the command-line client

Start an interactive session:

```sh
go run ./cmd/client -addr 127.0.0.1:6380
```

Run one command:

```sh
go run ./cmd/client -addr 127.0.0.1:6380 SET greeting hello
go run ./cmd/client -addr 127.0.0.1:6380 GET greeting
```

## Persistence

Load and save a snapshot when the server starts and stops:

```sh
go run ./cmd/server -snapshot ./kestrel.snapshot
```

Enable append-only persistence:

```sh
go run ./cmd/server \
  -appendonly ./kestrel.aof \
  -appendfsync everysec
```

The available append-only sync policies are `always`, `everysec`, and `no`.

## Replication

Start a primary:

```sh
go run ./cmd/server -addr 127.0.0.1:6380
```

Start a read-only replica:

```sh
go run ./cmd/server \
  -addr 127.0.0.1:6381 \
  -replicaof 127.0.0.1:6380 \
  -replica-id replica-1
```

## Cluster routing

Cluster topology is loaded from a JSON file. Each slot range is inclusive.

```json
{
  "local_id": "node-1",
  "nodes": [
    {
      "id": "node-1",
      "address": "127.0.0.1:7000",
      "slots": [{ "start": 0, "end": 8191 }]
    },
    {
      "id": "node-2",
      "address": "127.0.0.1:7001",
      "slots": [{ "start": 8192, "end": 16383 }]
    }
  ]
}
```

Start a node with the topology:

```sh
go run ./cmd/server \
  -addr 127.0.0.1:7000 \
  -cluster-config ./cluster.json
```

Replica mode and cluster mode cannot be enabled on the same server process.

## Project layout

- `cmd/server`: server executable
- `cmd/client`: interactive and one-shot client
- `internal/engine`: keyspace, data types, expiration, and durability
- `internal/proto`: request and response framing
- `internal/server`: command handling, replication, and cluster routing
