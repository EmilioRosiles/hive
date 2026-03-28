# hive

An embeddable, leaderless distributed cache for Go applications. Drop it into any Go service to share in-memory state across instances — no external infrastructure required.

```go
node, _ := hive.NewNode(hive.Config{
    Mode:  hive.ModeCluster,
    Seeds: []string{"node1:7946"},
})
defer node.Shutdown()

sessions := hive.NewValueStore[Session](node, "sessions")

sessions.Set("user:123", Session{UserID: 123, Token: "abc"})
s, err := sessions.Get("user:123")
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

counters := hive.NewValueStore[int](node, "counters")
counters.Set("visits", 42)

v, err := counters.Get("visits")
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

### Checking cluster state

```go
status := node.Status()
fmt.Printf("node %s, cluster size %d\n", status.NodeID, status.Size)
for _, p := range status.Peers {
    fmt.Printf("  peer %s addr=%s alive=%v\n", p.NodeID, p.Addr, p.Alive)
}
```

## Stores

Multiple stores can share the same node — they are namespaced views over the same underlying cluster. Each store type maps to a Redis-style API.

### ValueStore[T]

A typed key/value store. Values are msgpack-encoded structs or scalars.

```go
type Session struct {
    UserID int
    Token  string
}

sessions := hive.NewValueStore[Session](node, "sessions")

// Set stores a value.
err := sessions.Set("user:123", Session{UserID: 123, Token: "abc"})

// Get retrieves and decodes a value. Errors if missing or expired.
s, err := sessions.Get("user:123")

// Del removes a key.
sessions.Del("user:123")

// Expire sets a TTL. The key is deleted automatically after the duration elapses.
sessions.Expire("user:123", 30*time.Minute)
```

### SetStore

A distributed string set. Members can carry independent per-member TTLs, making it useful for tracking presence or short-lived memberships.

```go
online := hive.NewSetStore(node, "online_users")

// SAdd adds a member to the set at key.
online.SAdd("room:1", "user:123")

// SExpireMember sets a per-member TTL. Other members and the key are unaffected.
online.SExpireMember("room:1", "user:123", 30*time.Second)

// SMembers returns all live members.
members, err := online.SMembers("room:1")

// SIsMember checks membership.
ok, err := online.SIsMember("room:1", "user:123")

// SCard returns the number of live members.
n, err := online.SCard("room:1")

// SRem removes a single member.
online.SRem("room:1", "user:123")

// Del removes the entire set. Expire sets a key-level TTL.
online.Del("room:1")
online.Expire("room:1", 5*time.Minute)
```

### HashStore[T]

A typed key/field/value store. Fields within a key carry independent TTLs, making it well-suited for tracking per-entity state with automatic eviction.

```go
type Stream struct {
    StartedAt time.Time
    BitRate   int
}

streams := hive.NewHashStore[Stream](node, "streams")

// HSet stores a value under key/field.
streams.HSet("user:123", "stream:abc", Stream{StartedAt: time.Now()})

// HExpireField sets a TTL on a single field. Other fields are unaffected.
streams.HExpireField("user:123", "stream:abc", 30*time.Minute)

// HGet retrieves and decodes a single field.
s, err := streams.HGet("user:123", "stream:abc")

// HGetAll retrieves all live fields under a key.
all, err := streams.HGetAll("user:123")

// HKeys returns the names of all live fields.
fields, err := streams.HKeys("user:123")

// HDel removes a single field.
streams.HDel("user:123", "stream:abc")

// Del removes the entire hash. Expire sets a key-level TTL.
streams.Del("user:123")
streams.Expire("user:123", 1*time.Hour)
```

## TTL behavior

All stores support two levels of TTL:

- **Key-level TTL** (`Expire`) — deletes the entire key when it elapses
- **Field-level TTL** (`SExpireMember`, `HExpireField`) — evicts a single member or field independently, without affecting other members or the key itself. If all members/fields expire, the key is cleaned up automatically.

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
    ReplicationFactor int

    // Virtual nodes per physical node on the consistent hash ring.
    // Higher values improve key distribution uniformity.
    // Default: 100
    VNodes int

    // How often this node sends heartbeats to peers.
    // Default: 3s
    GossipInterval time.Duration

    // Number of peers contacted per gossip round.
    // Default: 3
    GossipFanout int

    // How long to wait after a topology change before rebalancing.
    // Prevents cascading migrations when multiple nodes join or leave at once.
    // Default: 500ms
    RebalanceDebounce time.Duration

    // How long a peer can be unreachable before it is removed from the ring
    // and its keys redistributed.
    // Default: 10s
    DeadTimeout time.Duration
}
```

## Consistency model

Hive is an **ephemeral, eventually consistent** cache.

- Reads and writes go to the key's primary owner as determined by consistent hashing
- Replication is asynchronous — replicas may be briefly behind the primary
- If two partitioned sub-clusters both accept writes to the same key and later merge, the last rebalance wins (determined by ring ownership after the merge, not by write timestamp)
- There is no durability — a node restart loses its local data. Surviving replicas retain their copies

This makes Hive well-suited for session caches, rate-limit counters, presence tracking, stream limits, and other short-lived shared state where occasional staleness is acceptable.

## Data types

Values must be serializable by [`msgpack`](https://github.com/vmihailenco/msgpack):

- All fields you want preserved must be **exported**
- Pointers, slices, maps, and structs are all supported

## Operational notes

**Ports** — each node needs its `BindPort` reachable by all other nodes. In Docker/Kubernetes, expose and map the port explicitly.

**Seeds** — at least one seed must be reachable when a node starts. Seeds do not need to be stable or permanent — any alive cluster member works.

**Replication factor** — keep it ≤ the minimum expected cluster size. A factor of 2 with a 2-node cluster means every node holds every key.

**Graceful shutdown** — calling `node.Shutdown()` announces the departure to peers so they can redistribute keys immediately rather than waiting for `DeadTimeout`.

## License

MIT
