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
dau       := hive.NewBitmapStore(cluster, "dau")
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

### BitmapStore

A distributed packed bit array, one bit per offset. Useful for large sets of per-ID boolean flags like daily active users or feature rollouts, where a `SetStore` would cost a full entry per ID.

```go
dau := hive.NewBitmapStore(cluster, "dau")

// SetBit sets or clears the bit at offset.
dau.SetBit(ctx, "2026-08-22", 123456, true)

// GetBit reports whether the bit at offset is set. Unset and missing keys read as false.
on, err := dau.GetBit(ctx, "2026-08-22", 123456)

// Count returns the number of set bits.
n, err := dau.Count(ctx, "2026-08-22")

// Del removes the entire bitmap. Expire sets a key-level TTL.
dau.Del(ctx, "2026-08-22")
dau.Expire(ctx, "2026-08-22", 48*time.Hour)
```

Offsets are `uint32`. A bitmap grows to fit the highest bit set, so its memory is `offset/8` bytes regardless of how many bits are set — keep offsets dense (e.g. sequential user IDs, not hashes).

## TTL behavior

`Expire` sets a key-level TTL; the entire key is deleted once it elapses.

## Locking

`Lock` is the low-level primitive: a non-blocking, cluster-wide distributed lock on a key, returning `ErrKeyLocked` immediately if it's already held, or `ErrNotFound` if the key doesn't exist. Every store type exposes it:

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
sessions.Set(lock.Context(ctx), "user:123", updated) // authorized: same holder, passes
sessions.Set(ctx, "user:123", updated)               // rejected: ErrKeyLocked
```

`lock.Context(ctx)` carries the lock's token on top of whatever you pass in — pass your own in-flight `ctx` to preserve its values/deadline/cancellation, or `context.Background()` for a critical section that should run independently of any request already in flight. `Unlock`/`Renew` verify the caller still holds the lock (via that same token) and return `ErrLockNotHeld` otherwise — either it was never held, or it expired and was re-acquired by someone else. Locks expire automatically if never renewed or unlocked, so a crashed holder can't strand a key forever.

Most callers don't need to drive `Lock` directly — see `Atomic` below for the common case of just running a function under it.

### Atomic

`Atomic` wraps `Lock` for the common case: acquire, run a function, release. It waits for the lock — retrying with backoff instead of failing immediately on `ErrKeyLocked` — until it acquires the lock or `ctx` is done, then calls `fn` with the lock's authorized context already applied, and releases the lock when `fn` returns:

```go
err := sessions.Atomic(ctx, "user:123", 10*time.Second, func(ctx context.Context) error {
    s, err := sessions.Get(ctx, "user:123") // authorized: runs under the lock
    if err != nil {
        return err
    }
    s.Balance -= 10
    return sessions.Set(ctx, "user:123", s)
})
```

`ttl` bounds how long the lock is held once acquired, same as `Lock`; `ctx` bounds how long `Atomic` is willing to wait to acquire it — pass a `ctx` with no deadline to wait indefinitely. `fn`'s error is returned as-is; a nonexistent key still fails fast with `ErrNotFound` rather than retrying forever.

## Configuration

```go
hive.Config{
    // Unique identifier for this node.
    // Default: a random 16-character hex ID
    NodeID string

    // ModeStandalone (default) or ModeCluster.
    Mode Mode

    // Address to bind the peer communication port to.
    // Default: "0.0.0.0"
    BindAddr string

    // Port for peer communication.
    // Default: 7946
    BindPort int

    // host:port peers use to reach this node, when it differs from
    // BindAddr:BindPort (NAT, containers, proxies).
    // Default: BindAddr:BindPort
    AdvertiseAddr string

    // Seed peer addresses (host:port) used to bootstrap cluster membership.
    // Required when Mode is ModeCluster.
    Seeds []string

    // Number of nodes that store a copy of each key.
    // Higher values improve fault tolerance but increase write overhead.
    // Must be <= cluster size. Default: 1
    // At the default of 1, replication is a no-op and the replicator is
    // never started, saving memory.
    ReplicationFactor int

    // Most time a request may spend reaching its primary, including waiting
    // out a suspected one, and how long one replication batch may take.
    // A request that can't reach its primary in time returns ErrUnavailable.
    // Default: 1s
    RoutingTimeout time.Duration

    // How long a request waits between attempts while its primary is
    // suspected or after a retryable failure.
    // Default: 50ms
    RoutingRetryInterval time.Duration

    // Most TCP connections kept per peer. Each connection carries traffic in
    // both directions. A peer starts with one connection; another is opened
    // while every existing one is busy, and one left idle for a whole
    // CleanupInterval is closed (one is always kept).
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

    // How long a heartbeat may take before the peer is suspected and probed.
    // Default: 300ms
    GossipTimeout time.Duration

    // How long a direct ping to a suspected peer may take; pings relayed
    // through a helper get twice as long.
    // Default: 300ms
    ProbeTimeout time.Duration

    // How many other peers are asked to ping a suspected peer when a direct
    // ping fails.
    // Default: 3
    ProbeHelpers int

    // How long a probe waits before retrying when no helper answered.
    // Default: 1s
    ProbeInterval time.Duration

    // How long to wait after a topology change before rebalancing.
    // Prevents cascading migrations when multiple nodes join or leave at once.
    // Default: 500ms
    RebalanceDebounce time.Duration

    // Max number of migrated keys sent per rebalance frame.
    // Default: 128
    RebalanceBatchSize int

    // How long one rebalance frame may take before the migration is
    // retried on the next rebalance run.
    // Default: 10s
    RebalanceTimeout time.Duration

    // Max number of queued-but-unsent replication writes held per peer.
    // Further writes for that peer are dropped until it catches up.
    // Default: 4096
    ReplicationQueueSize int

    // Max number of queued replication writes sent to a peer in one batch.
    // Default: 256
    ReplicationBatchSize int

    // How often the janitor runs to delete expired entries, forget dead
    // peers after DeadRetention, and close idle peer connections.
    // Default: 30s
    CleanupInterval time.Duration

    // How long a dead peer is remembered, so stale gossip can't bring it back.
    // Default: 10 × GossipInterval
    DeadRetention time.Duration

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

- Reads and writes go to the key's primary owner as determined by consistent hashing. Replicas never serve reads or take writes; a replica only takes over a key once the old primary is declared dead and the ring makes it the new primary
- If the primary is suspected, a request waits (retrying every `RoutingRetryInterval`) until the suspicion is resolved, for at most `RoutingTimeout` or the caller's deadline. If it can't reach a primary in time it returns `ErrUnavailable`; a write that returns `ErrUnavailable` was not applied, so it is safe to retry
- Replication is asynchronous — replicas may be briefly behind the primary
- Replication to each replica is ordered. While a replica is suspected its writes are held and delivered once it is confirmed alive; a batch resent after a timeout is recognized and applied only once. If a replica stays unreachable long enough to fill its queue (`ReplicationQueueSize`), further writes for it are dropped rather than blocking callers, and it may stay stale for those keys until they are rewritten or expire
- When a network partition heals and keys are redistributed, Hive uses **last-write-wins (LWW)** conflict resolution: every stored entry carries a second-precision write timestamp (`mtime`), and rebalance only overwrites a local copy if the incoming entry is strictly newer. This prevents split-brain partitions from silently clobbering fresher data.
- There is no durability — a node restart loses its local data. Surviving replicas retain their copies

This makes Hive well-suited for session caches, rate-limit counters, presence tracking, leaderboards, job queues, and other short-lived shared state where occasional staleness is acceptable.

## Performance

Measured with a real multi-container cluster, not goroutines sharing one process: four Docker containers (three data-owning nodes plus a driver), each pinned to `cpus: 3` / `mem_limit: 1g` (12 CPUs / 4GB total budget). A snapshot of specific, narrow dimensions, not a general performance claim.

Standalone numbers come from the same driver container running a single unclustered node (see [Standalone](#standalone-single-instance)) — a same-process call, no network. Cluster numbers come from that driver joining a 3-node cluster (RF=2) as a node configured with `MemLimit: hive.Bytes(0)` (see [Configuration](#configuration)) — it owns no keyspace, so every operation is forwarded over the network to whichever of the other three nodes actually owns the key. Both modes run inside the same 3-CPU driver container, so the delta between them isolates network/forwarding cost rather than compute budget. Concurrent rows use 8 goroutines; throughput is aggregate ops/sec across all of them, not per-goroutine. LOCK rows measure a full `Lock`+`Unlock` round trip (two ops), each call using a fresh key so it never contends with another.

| Metric | Standalone | Cluster mode (cross-node) |
|---|---|---|
| SET, single-threaded | ~760 ns/op (~1.32M ops/sec) | ~47 μs/op (~21.4K ops/sec) |
| GET, single-threaded | ~748 ns/op (~1.34M ops/sec) | ~53 μs/op (~18.9K ops/sec) |
| LOCK+UNLOCK, single-threaded | ~774 ns/op (~1.29M ops/sec) | ~89 μs/op (~11.3K ops/sec) |
| SET, 8-way concurrent | ~3.76M ops/sec | ~104.4K ops/sec |
| GET, 8-way concurrent | ~4.22M ops/sec | ~134.3K ops/sec |
| LOCK+UNLOCK, 8-way concurrent | ~3.15M ops/sec | ~55.1K ops/sec |

Cross-node, LOCK's cost lines up with SET/GET as expected — a round trip of two ops costs almost exactly 2× one op. Standalone the gap is much narrower, since there's no per-op network round trip for a second op to double.

Two different memory numbers, since they answer different questions:

- **Node construction cost** — incremental `HeapAlloc` added by `hive.NewNode()` itself (GC-settled heap immediately before vs. after), isolating just what Hive's own state costs, independent of Go runtime/binary baseline.
- **Process RSS** — the whole container's resident memory (`/proc/self/status` VmRSS): Go runtime, goroutine stacks, GC metadata, loaded binary, everything. This is what actually shows up in `docker stats`, but it's dominated by fixed Go-process overhead (several MB at rest for *any* Go binary), not something `NewNode` controls.

| Metric | Standalone | Cluster mode (relay/driver) |
|---|---|---|
| Node construction (`HeapAlloc` delta) | ~57 KB | ~456 KB |
| RSS, idle (no data) | ~6.4 MB | ~8.9 MB |
| RSS, after 1,000 keys | ~6.8 MB (+133 B/key logical) | ~11.3 MB |
| RSS, after full run above (~100K keys) | ~48 MB | ~11.7 MB |

The relay's own logical accounting (`node.MemUsed()`) stays at exactly 0 through every RSS row above — it never owns a key, so its RSS reflects only connection/gossip/forwarding state, not the data volume flowing through it; standalone's RSS instead grows with what it actually stores, from ~6.4 MB idle to ~48 MB holding ~100K small entries. RSS rows vary by a couple of MB between runs; the `HeapAlloc` row is stable.

### Scaling with cluster size

Per-peer cost is what grows as a cluster gets bigger, so it's measured separately: an in-process cluster (every node in one test process, RF=2, 4,000 writes), counting what the whole cluster holds after a GC. Totals, not per node; each TCP connection counts two file descriptors here, one for each end.

| Nodes | Heap | Goroutines | File descriptors |
|---|---|---|---|
| 5 | ~4.6 MB | ~63 | ~48 |
| 10 | ~10.2 MB | ~168 | ~138 |
| 25 | ~44.5 MB | ~722 | ~645 |

Connections carry traffic in both directions and the pool only grows while connections are busy, so an idle or lightly loaded peer costs about one connection. Replication runs through a single worker per node whose queues only hold memory while writes are actually waiting. For comparison, the previous major version measured ~207 MB of heap, ~5,500 goroutines and ~4,900 file descriptors for the same 25-node cluster, mostly from a replication goroutine and a preallocated 4,096-slot queue per peer.

## Data types

Values must be serializable by [`msgpack`](https://github.com/vmihailenco/msgpack):

- All fields you want preserved must be **exported**
- Pointers, slices, maps, and structs are all supported

## Architecture notes

### Gossip and failure detection

Membership state is propagated using a gossip protocol. Every `GossipInterval` each node sends its view of the cluster to a random subset of peers (`GossipFanout`). Each entry carries an **incarnation number**, seeded with the current Unix timestamp when the node starts, so a restarted node's first heartbeat outranks any stale rumor about it.

Failure detection follows SWIM: a single failure never kills a node.

1. A failed heartbeat, forward, replication batch or rebalance send marks the peer **Suspect**. A Suspect peer keeps its place on the ring; requests for its keys wait for the outcome instead of being sent to a replica.
2. The suspecting node **probes** it: a direct ping (`ProbeTimeout`), then pings relayed through up to `ProbeHelpers` other peers (twice `ProbeTimeout`). Any acknowledgement marks it Alive again. If the helpers answered but none could reach it, it is marked **Dead**, removed from the ring, and its keys are redistributed after `RebalanceDebounce`. If no helper answered at all, the prober itself may be the one cut off, so it stays Suspect and retries every `ProbeInterval` instead of declaring anyone dead. In a two-node cluster there are no helpers, so a failed direct ping means Dead.
3. A newer incarnation always wins. At an equal incarnation the worse status wins (Dead over Suspect over Alive). A dead rumor about a peer is verified with the receiving node's own probe before it acts on it, and a node that hears it is rumored dead refutes the rumor by bumping its own incarnation.
4. A dead peer is remembered for `DeadRetention`, so stale gossip can't bring it back before every node has heard it is dead.

### Virtual nodes and memory-proportional keyspace

The hash ring uses virtual nodes (vnodes) to distribute keyspace. Each node's vnode count is derived from its `MemLimit` relative to the rest of the cluster: a node with twice the memory of its peers owns roughly twice as much keyspace. This means data naturally flows toward nodes with more capacity without any manual weighting.

A node configured with `hive.Bytes(0)` gets exactly zero vnodes — it joins the cluster and participates in gossip like any other node, but never becomes a primary or replica for any key. Reads and writes routed through it are always forwarded to the nodes that actually own the data. Since it can never be a migration source or target, it also skips the rebalancer's bookkeeping (no ring-diffing on topology changes), a small additional memory saving on top of owning no keyspace. This is useful for a pure routing/relay worker, or for setting up benchmarks that pay the same network hop a client of a separate networked cache (e.g. Redis) always pays.

### Janitor

A background janitor runs every `CleanupInterval` and performs three tasks:

1. **Expired entry eviction** — scans the local store and removes entries whose TTL has elapsed
2. **Tombstone cleanup** — forgets peers that have been dead for at least `DeadRetention`
3. **Idle connection reaping** — closes peer connections unused since the previous run, keeping one per peer

### Split-brain recovery

Each stored entry carries an `mtime` timestamp (Unix seconds, set at the time of the write). When rebalancing after a partition heals, incoming entries are written only if their `mtime` is strictly newer than the local copy. This last-write-wins strategy ensures the most recently written value survives without requiring coordination between nodes.

## Operational notes

**Ports** — every node must be reachable by all other nodes at the address it advertises: `AdvertiseAddr`, which defaults to `BindAddr:BindPort`. Behind NAT or in Docker/Kubernetes, where the address peers dial differs from the one the node binds, set `AdvertiseAddr` to the reachable `host:port` and map the port explicitly.

**Seeds** — at least one seed must be reachable when a node starts. Seeds do not need to be stable or permanent — any alive cluster member works.

**Replication factor** — keep it ≤ the minimum expected cluster size. A factor of 2 with a 2-node cluster means every node holds every key.

**Graceful shutdown** — calling `node.Shutdown()` announces the departure to peers so they can redistribute keys immediately.

**Memory limits** — `MemLimit` affects both write rejection and ring weight. Nodes that exceed their limit return an error on write; they do not evict existing entries to make room. Use TTLs on keys that should not accumulate indefinitely.

**Connection pool size** — connections carry traffic in both directions, so two peers share them. A peer pair starts with one connection and grows to at most `ConnPoolSize` (default 4) only while every connection is busy; idle ones are closed by the janitor. Budget at most `peers × ConnPoolSize` sockets per node; a lightly loaded cluster uses about one per peer.

**Upgrading** — the peer wire format changed in this major version, so nodes of different major versions can't form a cluster. Upgrade by replacing the whole cluster rather than rolling node by node; since Hive holds no durable state, a fresh cluster starts empty either way.

## Development

Parts of this project were written with the assistance of AI tools (Claude Code), like the transport, some of the data structure implementations, testing, and documentation. All design decisions and code were carefully reviewed by the maintainer.

## License

MIT
