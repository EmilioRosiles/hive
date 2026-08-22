# hive

[![CI](https://github.com/EmilioRosiles/hive/actions/workflows/ci.yml/badge.svg)](https://github.com/EmilioRosiles/hive/actions/workflows/ci.yml)

An embeddable, leaderless distributed cache for Go applications. Drop it into any Go service to share in-memory state across instances — no external infrastructure required.

```go
node, _ := hive.NewNode(hive.Config{
    Mode:  hive.ModeCluster,
    Seeds: []string{"node1:7946"},
})
defer node.Shutdown()

cluster := node.Cluster()
sessions := hive.NewValueStore[Session](cluster, "sessions")

sessions.Set(ctx, "user:123", Session{UserID: 123, Token: "abc"})
s, err := sessions.Get(ctx, "user:123")
```

## How it works

Each instance of your application runs a Hive node. Nodes discover each other through a seed list and form a self-organizing cluster using a gossip protocol. Keys are distributed across nodes using consistent hashing, replicated according to your replication factor, and automatically redistributed when nodes join or leave.

- **Leaderless** — every node is equal, no election needed
- **Self-healing** — nodes that go silent are detected and their keys redistributed
- **Embeddable** — no sidecar, no separate process, just a Go import
- **Minimal dependencies** — uses [`msgpack`](https://github.com/vmihailenco/msgpack) for serialization, nothing else added to your `go.mod`

## Installation

```bash
go get github.com/EmilioRosiles/hive
```

## Usage

### Standalone (single instance)

Good for development or single-node deployments. Data stays local, no networking.

```go
node, err := hive.NewNode(hive.Config{})
if err != nil {
    log.Fatal(err)
}
defer node.Shutdown()

cluster := node.Cluster()
counters := hive.NewValueStore[int](cluster, "counters")
counters.Set(ctx, "visits", 42)

v, err := counters.Get(ctx, "visits")
```

### Cluster mode

Each application instance joins the same cluster by pointing at one or more seed addresses. Seeds only need to be reachable at startup — once the node has joined, membership is maintained through gossip.

```go
node, err := hive.NewNode(hive.Config{
    Mode:     hive.ModeCluster,
    BindPort: 7946,
    Seeds:    []string{"10.0.0.1:7946", "10.0.0.2:7946"},
})
```

In a containerized environment, seeds are typically set via an environment variable:

```go
seeds := strings.Split(os.Getenv("HIVE_SEEDS"), ",")

node, err := hive.NewNode(hive.Config{
    Mode:  hive.ModeCluster,
    Seeds: seeds,
})
```

### TLS

Cluster-mode peer connections can be secured with mutual TLS — every node authenticates every peer it talks to, not just encrypts the wire. Peer addresses are dynamic (`IP:port`), so verification is chain-based (trust means "signed by our cluster CA") rather than hostname-based — the same pattern used by etcd and Consul.

```go
tlsConfig, err := hive.NewClusterTLSConfig(certPEM, keyPEM, caPEM)
if err != nil {
    log.Fatal(err)
}

node, err := hive.NewNode(hive.Config{
    Mode:      hive.ModeCluster,
    Seeds:     seeds,
    TLSConfig: tlsConfig,
})
```

`NewClusterTLSConfig` builds a ready-to-use `*tls.Config` from PEM-encoded cert/key/CA material; every node needs a certificate signed by the same CA. For full control — custom verification, hot cert rotation via `GetCertificate`/`GetClientCertificate` — set `Config.TLSConfig` to your own `*tls.Config` instead. `nil` disables TLS (default, plaintext).

### Checking cluster state

```go
cluster := node.Cluster()
fmt.Printf("node %s, cluster size %d\n", node.ID(), cluster.AliveCount())
for _, m := range cluster.Members() {
    fmt.Printf("  member %s addr=%s alive=%v mem=%d/%d\n", m.NodeID, m.Addr, m.Alive, m.MemUsed, m.MemLimit)
}
```

`Node` also exposes local-only facts about this process directly, with no gossip round-trip needed: `node.ID()`, `node.Addr()`, `node.MemUsed()`, `node.MemLimit()`, `node.KeyCount()`, `node.Uptime()`.

## Stores

Multiple stores can share the same node — they are namespaced views over the same underlying cluster. Obtain a `Cluster` handle from the node and pass it to each store constructor.

```go
cluster := node.Cluster()

sessions  := hive.NewValueStore[Session](cluster, "sessions")
online    := hive.NewSetStore(cluster, "online_users")
streams   := hive.NewHashStore[Stream](cluster, "streams")
queue     := hive.NewListStore[Task](cluster, "work_queue")
scores    := hive.NewZSetStore(cluster, "leaderboard")
```

Each store type maps to a Redis-style API.

### ValueStore[T]

A typed key/value store. Values are msgpack-encoded structs or scalars.

```go
type Session struct {
    UserID int
    Token  string
}

sessions := hive.NewValueStore[Session](cluster, "sessions")

// Set stores a value.
err := sessions.Set(ctx, "user:123", Session{UserID: 123, Token: "abc"})

// Get retrieves and decodes a value. Errors if missing or expired.
s, err := sessions.Get(ctx, "user:123")

// Del removes a key.
sessions.Del(ctx, "user:123")

// Expire sets a TTL. The key is deleted automatically after the duration elapses.
sessions.Expire(ctx, "user:123", 30*time.Minute)
```

### SetStore

A distributed string set, useful for tracking presence or membership.

```go
online := hive.NewSetStore(cluster, "online_users")

// SAdd adds a member to the set at key.
online.SAdd(ctx, "room:1", "user:123")

// SMembers returns all members.
members, err := online.SMembers(ctx, "room:1")

// SIsMember checks membership.
ok, err := online.SIsMember(ctx, "room:1", "user:123")

// SCard returns the number of members.
n, err := online.SCard(ctx, "room:1")

// SRem removes a single member.
online.SRem(ctx, "room:1", "user:123")

// Del removes the entire set. Expire sets a key-level TTL.
online.Del(ctx, "room:1")
online.Expire(ctx, "room:1", 5*time.Minute)
```

### HashStore[T]

A typed key/field/value store, well-suited for tracking per-entity state.

```go
type Stream struct {
    StartedAt time.Time
    BitRate   int
}

streams := hive.NewHashStore[Stream](cluster, "streams")

// HSet stores a value under key/field.
streams.HSet(ctx, "user:123", "stream:abc", Stream{StartedAt: time.Now()})

// HGet retrieves and decodes a single field.
s, err := streams.HGet(ctx, "user:123", "stream:abc")

// HGetAll retrieves all fields under a key.
all, err := streams.HGetAll(ctx, "user:123")

// HKeys returns the names of all fields.
fields, err := streams.HKeys(ctx, "user:123")

// HDel removes a single field.
streams.HDel(ctx, "user:123", "stream:abc")

// Del removes the entire hash. Expire sets a key-level TTL.
streams.Del(ctx, "user:123")
streams.Expire(ctx, "user:123", 1*time.Hour)
```

### ListStore[T]

A typed distributed ordered list. Elements are msgpack-encoded. Supports efficient push/pop from both ends, making it suitable for queues, stacks, and activity feeds.

```go
type Task struct {
    ID      string
    Payload []byte
}

queue := hive.NewListStore[Task](cluster, "work_queue")

// RPush appends to the tail. LPush prepends to the head.
queue.RPush(ctx, "jobs", Task{ID: "t1", Payload: data})
queue.LPush(ctx, "jobs", Task{ID: "t0", Payload: data})

// LPop removes and returns the head. RPop removes and returns the tail.
task, err := queue.LPop(ctx, "jobs")

// LLen returns the number of elements.
n, err := queue.LLen(ctx, "jobs")

// LIndex returns the element at index. Negative indices count from the tail.
last, err := queue.LIndex(ctx, "jobs", -1)

// LRange returns a slice from start to stop inclusive. Negative indices supported.
page, err := queue.LRange(ctx, "jobs", 0, 9)

// LSet overwrites the element at index.
queue.LSet(ctx, "jobs", 0, Task{ID: "t0-updated"})

// Del removes the entire list. Expire sets a key-level TTL.
queue.Del(ctx, "jobs")
queue.Expire(ctx, "jobs", 1*time.Hour)
```

### ZSetStore

A distributed sorted set. Each member is a unique string associated with a float64 score. Members are always kept in ascending score order, with ties broken lexicographically.

```go
scores := hive.NewZSetStore(cluster, "leaderboard")

// ZAdd inserts or updates member with score.
scores.ZAdd(ctx, "game:1", 9500.0, "alice")
scores.ZAdd(ctx, "game:1", 8200.0, "bob")

// ZScore returns the score for a member. Errors if member does not exist.
s, err := scores.ZScore(ctx, "game:1", "alice")

// ZRank returns the 0-based rank in ascending order (lowest score = 0).
// ZRevRank returns the rank in descending order (highest score = 0).
rank, err := scores.ZRank(ctx, "game:1", "bob")
rank, err  = scores.ZRevRank(ctx, "game:1", "alice")

// ZCard returns the number of members.
n, err := scores.ZCard(ctx, "game:1")

// ZRange returns members from rank start to stop inclusive.
// Negative indices count from the top (highest rank).
top3, err := scores.ZRange(ctx, "game:1", -3, -1)

// ZRangeByScore returns all members with min <= score <= max in ascending order.
mid, err := scores.ZRangeByScore(ctx, "game:1", 8000.0, 9000.0)

// ZRem removes a member.
scores.ZRem(ctx, "game:1", "bob")

// Del removes the entire sorted set. Expire sets a key-level TTL.
scores.Del(ctx, "game:1")
scores.Expire(ctx, "game:1", 24*time.Hour)
```

`ZRange` and `ZRangeByScore` return `[]ZSetEntry`, where each entry has `Member string` and `Score float64`.

## TTL behavior

`Expire` sets a key-level TTL; the entire key is deleted once it elapses.

## Locking

`Lock` acquires a non-blocking, cluster-wide distributed lock on a key, returning `ErrKeyLocked` immediately if it's already held. Every store type exposes it:

```go
lock, err := sessions.Lock(ctx, "user:123", 10*time.Second)
if err != nil {
    // ErrKeyLocked: someone else holds it
    return err
}
defer lock.Unlock(ctx)

// ... critical section ...

lock.Renew(ctx, 10*time.Second) // extend before it expires
```

A lock is **blanket enforcement**: while held, every operation against that key — `Set`, `Get`, `Del`, and so on — is rejected with `ErrKeyLocked` for everyone, including the holder. To perform the work the lock is protecting, authorize the call with the lock's own context:

```go
sessions.Set(lock.Context(), "user:123", updated) // authorized: same holder, passes
sessions.Set(ctx, "user:123", updated)             // rejected: ErrKeyLocked
```

`lock.Context()` is rooted on `context.Background()` and carries the lock's token, not any deadline — combine it with your own timeout via `context.WithTimeout` if you need one. `Unlock`/`Renew` verify the caller still holds the lock (via that same token) and return `ErrLockNotHeld` otherwise — either it was never held, or it expired and was re-acquired by someone else. Locks expire automatically if never renewed or unlocked, so a crashed holder can't strand a key forever.

## Configuration

```go
hive.Config{
    // Unique identifier for this node.
    // Auto-generated if empty.
    NodeID string

    // ModeStandalone (default) or ModeCluster.
    Mode Mode

    // Address to bind the peer communication port to.
    // Default: "0.0.0.0"
    BindAddr string

    // Port for peer communication.
    // Default: 7946
    BindPort int

    // Seed peer addresses (host:port) used to bootstrap cluster membership.
    // Required when Mode is ModeCluster.
    Seeds []string

    // Number of nodes that store a copy of each key.
    // Higher values improve fault tolerance but increase write overhead.
    // Must be <= cluster size. Default: 1
    // At the default of 1, replication is a no-op and each peer's
    // replication queue is never allocated, saving memory.
    ReplicationFactor int

    // How long this node waits before cancelling a routed operation
    // (forward-to-primary or replica fan-out).
    // Default: 1s
    RoutingTimeout time.Duration

    // ConnPoolSize is the number of pooled TCP connections maintained per
    // peer, round-robin shared across all traffic to that peer (forwarded
    // reads/writes, replication, gossip, rebalance). Higher values reduce
    // contention under concurrent load at the cost of more sockets.
    // Default: 4
    ConnPoolSize int

    // Maximum memory this node intends to use.
    // Controls two things: capacity enforcement (writes are rejected once the
    // limit is reached) and keyspace allocation (nodes with more memory receive
    // proportionally more vnodes on the hash ring, and therefore more keys).
    // nil (the zero value, i.e. left unset) means "use total system memory" —
    // the default. Set with hive.Bytes(n), e.g. hive.Bytes(4 * hive.GB), or
    // hive.Bytes(0) for a node that owns no keyspace at all — a pure
    // routing/relay worker that only forwards to the nodes that do.
    // Such a node also skips rebalancer bookkeeping entirely, since it can
    // never be a migration source or target.
    MemLimit MemLimit

    // How often this node sends heartbeats to peers.
    // Default: 5s
    GossipInterval time.Duration

    // Number of peers contacted per gossip round.
    // Default: 3
    GossipFanout int

    // How long this node waits before cancelling a heartbeat to a peer.
    // Default: 300ms
    GossipTimeout time.Duration

    // How long to wait after a topology change before rebalancing.
    // Prevents cascading migrations when multiple nodes join or leave at once.
    // Default: 500ms
    RebalanceDebounce time.Duration

    // Max number of migrated keys sent per rebalance frame.
    // Default: 128
    RebalanceBatchSize int

    // Max number of queued-but-unsent replication writes held per peer
    // before writes to that peer start blocking (backpressure).
    // Default: 4096
    ReplicationQueueSize int

    // Max number of queued replication writes sent to a peer in one batch.
    // Default: 256
    ReplicationBatchSize int

    // How often the cluster janitor runs to evict expired store entries
    // and remove dead peer tombstones from the membership table.
    // Default: 30s
    CleanupInterval time.Duration

    // Verbosity of internal log output written to stderr.
    // nil defaults to slog.LevelError (quiet).
    // Set to &slog.LevelInfo or &slog.LevelDebug for more detail.
    LogLevel *slog.Level

    // Enables TLS for cluster-mode peer connections when non-nil.
    // nil disables TLS (default, plaintext). Build one with
    // NewClusterTLSConfig, or construct your own *tls.Config.
    TLSConfig *tls.Config
}
```

## Consistency model

Hive is an **ephemeral, eventually consistent** cache.

- Reads and writes go to the key's primary owner as determined by consistent hashing
- Replication is asynchronous — replicas may be briefly behind the primary
- Replication to each replica is ordered and applies backpressure if that replica falls behind (tunable via `ReplicationQueueSize`/`ReplicationBatchSize`), bounding memory and goroutine growth under load at the cost of writes occasionally waiting on a struggling replica rather than silently drifting out of order
- When a network partition heals and keys are redistributed, Hive uses **last-write-wins (LWW)** conflict resolution: every stored entry carries a second-precision write timestamp (`mtime`), and rebalance only overwrites a local copy if the incoming entry is strictly newer. This prevents split-brain partitions from silently clobbering fresher data.
- There is no durability — a node restart loses its local data. Surviving replicas retain their copies

This makes Hive well-suited for session caches, rate-limit counters, presence tracking, leaderboards, job queues, and other short-lived shared state where occasional staleness is acceptable.

## Performance

Measured with a real multi-container cluster, not goroutines sharing one process: four Docker containers (three data-owning nodes plus a driver), each pinned to `cpus: 3` / `mem_limit: 1g` (12 CPUs / 4GB total budget). A snapshot of specific, narrow dimensions, not a general performance claim.

Standalone numbers come from the same driver container running a single unclustered node (see [Standalone](#standalone-single-instance)) — a same-process call, no network. Cluster numbers come from that driver joining a 3-node cluster (RF=2) as a node configured with `MemLimit: hive.Bytes(0)` (see [Configuration](#configuration)) — it owns no keyspace, so every operation is forwarded over the network to whichever of the other three nodes actually owns the key. Both modes run inside the same 3-CPU driver container, so the delta between them isolates network/forwarding cost rather than compute budget. Concurrent rows use 8 goroutines; throughput is aggregate ops/sec across all of them, not per-goroutine. LOCK rows measure a full `Lock`+`Unlock` round trip (two ops), each call using a fresh key so it never contends with another.

| Metric | Standalone | Cluster mode (cross-node) |
|---|---|---|
| SET, single-threaded | ~779 ns/op (~1.28M ops/sec) | ~52 μs/op (~19.2K ops/sec) |
| GET, single-threaded | ~737 ns/op (~1.36M ops/sec) | ~51 μs/op (~19.8K ops/sec) |
| LOCK+UNLOCK, single-threaded | ~930 ns/op (~1.08M ops/sec) | ~101 μs/op (~9.9K ops/sec) |
| SET, 8-way concurrent | ~2.99M ops/sec | ~87.5K ops/sec |
| GET, 8-way concurrent | ~3.40M ops/sec | ~112.0K ops/sec |
| LOCK+UNLOCK, 8-way concurrent | ~2.45M ops/sec | ~45.9K ops/sec |

LOCK's cost lines up with SET/GET as expected — a round trip of two ops costs almost exactly 2× one op, both standalone and cross-node.

Two different memory numbers, since they answer different questions:

- **Node construction cost** — incremental `HeapAlloc` added by `hive.NewNode()` itself (GC-settled heap immediately before vs. after), isolating just what Hive's own state costs, independent of Go runtime/binary baseline.
- **Process RSS** — the whole container's resident memory (`/proc/self/status` VmRSS): Go runtime, goroutine stacks, GC metadata, loaded binary, everything. This is what actually shows up in `docker stats`, but it's dominated by fixed Go-process overhead (several MB at rest for *any* Go binary), not something `NewNode` controls.

| Metric | Standalone | Cluster mode (relay/driver) |
|---|---|---|
| Node construction (`HeapAlloc` delta) | ~112 KB | ~859 KB |
| RSS, idle (no data) | ~9.3 MB | ~11.9 MB |
| RSS, after 1,000 keys | ~9.5 MB (+133 B/key logical) | ~14.1 MB |
| RSS, after full run above (~100K keys) | ~37.3 MB | ~13.7 MB |

The relay's own logical accounting (`node.MemUsed()`) stays at exactly 0 through every RSS row above — it never owns a key, so its RSS reflects only connection/gossip/forwarding state, not the data volume flowing through it; standalone's RSS instead grows with what it actually stores, from ~9.3 MB idle to ~37.3 MB holding ~100K small entries.

## Data types

Values must be serializable by [`msgpack`](https://github.com/vmihailenco/msgpack):

- All fields you want preserved must be **exported**
- Pointers, slices, maps, and structs are all supported

## Architecture notes

### Gossip and failure detection

Membership state is propagated using a gossip protocol. Every node periodically sends its view of the cluster to a random subset of peers (`GossipFanout`). Each outgoing heartbeat carries an **incarnation number** — a monotonically increasing counter seeded with the current Unix timestamp when the node starts. Seeding from wall time means a restarted node's first heartbeat carries a higher incarnation than any stale dead rumor about it, allowing it to rejoin without manual intervention.

A peer's state is updated only when the incoming incarnation is strictly higher than what is locally known. This prevents stale gossip from overwriting fresh state and avoids the clock-skew problems that arise from comparing wall-clock timestamps directly across machines.

Nodes that fail to respond to a heartbeat are marked dead immediately. Their keys are redistributed after `RebalanceDebounce` to allow the cluster to stabilize before migrating data.

### Virtual nodes and memory-proportional keyspace

The hash ring uses virtual nodes (vnodes) to distribute keyspace. Each node's vnode count is derived from its `MemLimit` relative to the rest of the cluster: a node with twice the memory of its peers owns roughly twice as much keyspace. This means data naturally flows toward nodes with more capacity without any manual weighting.

A node configured with `hive.Bytes(0)` gets exactly zero vnodes — it joins the cluster and participates in gossip like any other node, but never becomes a primary or replica for any key. Reads and writes routed through it are always forwarded to the nodes that actually own the data. Since it can never be a migration source or target, it also skips the rebalancer's bookkeeping (no ring-diffing on topology changes), a small additional memory saving on top of owning no keyspace. This is useful for a pure routing/relay worker, or for setting up benchmarks that pay the same network hop a client of a separate networked cache (e.g. Redis) always pays.

### Janitor

A background janitor runs every `CleanupInterval` and performs two tasks:

1. **Expired entry eviction** — scans the local store and removes entries whose TTL has elapsed
2. **Tombstone cleanup** — removes dead peer records from the membership table once they are no longer needed for gossip convergence

### Split-brain recovery

Each stored entry carries an `mtime` timestamp (Unix seconds, set at the time of the write). When rebalancing after a partition heals, incoming entries are written only if their `mtime` is strictly newer than the local copy. This last-write-wins strategy ensures the most recently written value survives without requiring coordination between nodes.

## Operational notes

**Ports** — each node needs its `BindPort` reachable by all other nodes. In Docker/Kubernetes, expose and map the port explicitly.

**Seeds** — at least one seed must be reachable when a node starts. Seeds do not need to be stable or permanent — any alive cluster member works.

**Replication factor** — keep it ≤ the minimum expected cluster size. A factor of 2 with a 2-node cluster means every node holds every key.

**Graceful shutdown** — calling `node.Shutdown()` announces the departure to peers so they can redistribute keys immediately.

**Memory limits** — `MemLimit` affects both write rejection and ring weight. Nodes that exceed their limit return an error on write; they do not evict existing entries to make room. Use TTLs on keys that should not accumulate indefinitely.

**Connection pool size** — each known peer gets `ConnPoolSize` connections (default 4), dialed lazily as traffic flows. Budget roughly `2 × peers × ConnPoolSize` sockets per node; for very large clusters, raise `ulimit -n` or lower `ConnPoolSize`.

## Development

Parts of this project were written with the assistance of AI tools (Claude Code), like the transport, some of the data structure implementations, testing, and documentation. All design decisions and code were carefully reviewed by the maintainer.

## License

MIT
