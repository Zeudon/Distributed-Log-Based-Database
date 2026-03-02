# Distributed Log-Based Database

A distributed, partitioned key-value store written in Go. The system uses the Raft consensus algorithm for replication, a log-structured write-ahead log (WAL) for durability, and a fully custom binary RPC framework over TCP — with no external frameworks.

---

## Architecture

- 3 partitions, each backed by an independent Raft consensus group
- 3 replicas per partition (9 nodes total) — tolerates 1 node failure per partition
- Keys are routed to partitions using FNV-1a hashing: `hash(key) % 3`
- Writes are appended to the WAL and committed only after a **majority of replicas** acknowledge them
- Snapshots are taken periodically and the WAL is compacted to prevent unbounded growth
- Each node runs in its own Docker container, orchestrated with Docker Compose


---

## Prerequisites

- [Go 1.25+](https://go.dev/dl/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

---

## Running the Cluster

**1. Build and start all 9 nodes:**

```bash
docker compose up --build
```

This builds the Go binary inside a container and starts 9 nodes across 3 partitions. Leader elections happen automatically within a few hundred milliseconds.

**2. Set the partition addresses in your shell** (so the client knows where to connect):

```powershell
# PowerShell
$env:PARTITION_0_ADDRS="localhost:7000,localhost:7001,localhost:7002"
$env:PARTITION_1_ADDRS="localhost:7010,localhost:7011,localhost:7012"
$env:PARTITION_2_ADDRS="localhost:7020,localhost:7021,localhost:7022"
```

```bash
# Bash / zsh
export PARTITION_0_ADDRS="localhost:7000,localhost:7001,localhost:7002"
export PARTITION_1_ADDRS="localhost:7010,localhost:7011,localhost:7012"
export PARTITION_2_ADDRS="localhost:7020,localhost:7021,localhost:7022"
```

**3. Use the CLI client:**

```bash
go run ./cmd/client set foo bar
go run ./cmd/client get foo
go run ./cmd/client delete foo
```

The client automatically resolves the correct partition for each key and follows leader-redirect responses if it contacts a non-leader replica.

---

## REST API

An API gateway (port `9090`) routes requests across all partitions. Individual nodes are also reachable on ports `8000–8022` (HTTP port `8080` inside each container).

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `POST` | `/v1/keys` | `{"key":"foo","value":"bar"}` | Create a key (fails if already exists) |
| `PUT` | `/v1/keys/{key}` | `{"value":"bar"}` | Create or update a key |
| `GET` | `/v1/keys/{key}` | — | Get a single value |
| `GET` | `/v1/keys` | — | List all key-value pairs (fan-out across partitions) |
| `GET` | `/v1/health` | — | Cluster health for all 9 nodes |

Non-leader nodes return `307 Temporary Redirect` with an `X-Raft-Leader-Http-Addr` header; the gateway follows redirects automatically.

**Examples:**
```bash
curl -X POST localhost:9090/v1/keys \
     -H "Content-Type: application/json" \
     -d '{"key":"foo","value":"bar"}'

curl localhost:9090/v1/keys/foo
curl localhost:9090/v1/keys
curl localhost:9090/v1/health
```

---

## Failure Testing

Stop a node and verify the remaining two replicas in its partition elect a new leader and continue serving requests:

```bash
docker compose stop node-p0-r0

go run ./cmd/client set resilience test
go run ./cmd/client get resilience
```

Restart it to observe log catch-up and re-integration:

```bash
docker compose start node-p0-r0
```

---

## License

MIT
