# hive

An embeddable, leaderless distributed cache for Go applications. Drop it into any Go service to share in-memory state across instances — no external infrastructure required.

```go
node, _ := hive.NewNode(hive.Config{
    Mode:  hive.ModeCluster,
    Seeds: []string{"node1:7946"},
})
defer node.Shutdown()

sessions := hive.NewCache[Session](node, "sessions")

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

counters := hive.NewCache[int](node, "counters")
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

### Working with caches

Create a typed `Cache` per logical data type. Multiple caches share the same node — they are just namespaced views over the same cluster.

```go
type Session struct {
    UserID int
    Token  string
}

type RateLimit struct {
    Count     int
    WindowEnd time.Time
}

node, _ := hive.NewNode(cfg)

sessions   := hive.NewCache[Session](node, "sessions")
rateLimits := hive.NewCache[RateLimit](node, "rate_limits")
```

**Set** stores a value. The struct is msgpack-encoded internally — exported fields only.

```go
err := sessions.Set("user:123", Session{UserID: 123, Token: "abc"})
```

**Get** retrieves and decodes a value. Returns an error if the key does not exist or has expired.

```go
s, err := sessions.Get("user:123")
if err != nil {
    // key missing or expired
}
```

**Del** removes a key.

```go
sessions.Del("user:123")
```

**Expire** sets a TTL. The key is deleted automatically after the duration elapses.

```go
sessions.Expire("user:123", 30*time.Minute)
```

### Checking cluster state

```go
status := node.Status()
fmt.Printf("node %s, cluster size %d\n", status.NodeID, status.Size)
for _, p := range status.Peers {
    fmt.Printf("  peer %s addr=%s alive=%v\n", p.NodeID, p.Addr, p.Alive)
}
```

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

This makes Hive well-suited for session caches, rate-limit counters, feature flags, and other short-lived shared state where occasional staleness is acceptable.

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
