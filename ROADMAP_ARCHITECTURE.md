# Hive Roadmap

Three areas to address before Hive is suitable for production workloads.
Each section describes the current state, the design decisions to make before coding,
and a concrete implementation path.

---

## 1. Memory cap + LRU eviction

### Problem

The store has no upper bound on memory. TTL-based cleanup (`janitor`) only removes
entries that have explicitly expired. Under sustained write load with no TTLs, the
node will grow without bound until the process OOMs.

### Design decisions

**Per-shard LRU vs global LRU**

A global LRU requires a cross-shard lock every time any key is read or written —
that would serialize all operations. Per-shard LRU (one LRU list per shard) fits
naturally into the existing sharded architecture and requires no new locks: the
shard's existing `sync.RWMutex` already protects its list.

**Memory accounting**

Two options:
- *Estimated*: track a byte counter per shard. Increment on write (estimated key+value size),
  decrement on eviction/delete. Fast, no syscalls, but approximate.
- *Sampled*: poll `runtime.MemStats.HeapInuse` on a goroutine. Simpler to implement but
  reacts to memory pressure from other parts of the program, not just the cache.

Estimated per-shard accounting is the right choice: it is deterministic, responds
immediately, and doesn't require tuning a polling interval.

**What counts as "memory"**

A simple estimator: `len(key) + len(encoded value) + fixed overhead per entry (~64 bytes
for map bucket + LRU node)`. Does not need to be exact — the goal is eviction
before OOM, not byte-perfect accounting.

### Implementation plan

1. **Add an LRU list to each shard** (`internal/store/store.go`)

   Replace `shard.data map[string]DataStructure` with a struct that holds both
   the map and a doubly-linked list. Use `container/list` from the standard library
   (no new dependency). Each list element stores the key so the map entry can be
   found on eviction.

   ```go
   type shard struct {
       mu       sync.RWMutex
       data     map[string]*lruEntry
       lru      list.List         // back = oldest, front = most recently used
       used     int64             // estimated bytes in use
       capacity int64             // 0 = unlimited
   }

   type lruEntry struct {
       value   DataStructure
       elem    *list.Element     // pointer back into lru list
       size    int64             // estimated bytes for this entry
   }
   ```

2. **Update `Set` and `Apply` to maintain LRU order and evict if over cap**

   On every write: move (or insert) the entry to the front of the list,
   update `used`. If `used > capacity`, evict from the back until under cap.

3. **Update `Get` and `Read` to promote on access**

   Move the accessed entry to the front of the LRU list under the write lock.
   `Read` currently takes an `RLock` — promoting on read requires upgrading to
   a write lock, which means `Read` must take a write lock when the LRU is enabled.
   Acceptable trade-off; document it.

4. **Wire `MemLimit` through to each shard**

   `DataStore` already receives `cleanupInterval`. Add a `memLimit uint64` parameter
   to `NewDataStore`. Divide it by shard count to get per-shard capacity.
   `MemLimit = 0` means unlimited (current behavior preserved).

5. **Size estimator helper**

   ```go
   func estimateSize(key string, e DataStructure) int64 {
       data, _ := e.Encode()
       return int64(len(key) + len(data) + 64)
   }
   ```

   Call this once on write. Rebalance-received entries are already encoded so
   the estimate is cheap.

6. **Eviction callback (optional, later)**

   For observability, add an `OnEvict func(key string)` field to `DataStore`.
   Nil by default. Useful when metrics are added later.

### Key files to touch

- `internal/store/store.go` — shard struct, Set, Apply, Get, Read, NewDataStore
- `internal/cluster/cluster.go` — pass MemLimit to NewDataStore
- `config.go` — MemLimit is already there; no change needed

---

## 2. Hardened gossip (no new dependencies)

### Problem

The current gossip implementation (`internal/cluster/gossip.go`) has three concrete bugs:

1. **Immediate death on a single missed heartbeat** — `heartbeat()` calls `markDead()`
   directly on the first TCP error. A GC pause, a momentary network blip, or a slow
   connection window triggers a ring change and rebalance unnecessarily.

2. **No suspicion or indirect probing** — SWIM-style failure detection requires asking
   other peers "can you reach this node?" before declaring it dead. The current code
   never does this, so a single-path network failure kills the node from the ring.

3. **Wall-clock `LastSeen` comparison in `mergeState`** — `rs.LastSeen.After(local.LastSeen)`
   is vulnerable to clock skew. A node with a clock slightly ahead can propagate state
   that looks fresh but is actually stale. This can cause a dead node to appear alive
   after restart.

### Design decisions

**Suspicion state (do not add a new boolean — extend `PeerInfo`)**

Add two fields to `PeerInfo` in `cluster.go`:
```go
type PeerInfo struct {
    NodeID            string
    Addr              string
    Alive             bool
    Suspected         bool      // true: failed direct ping, awaiting indirect confirmation
    SuspectedAt       time.Time // when suspicion started
    LastSeen          time.Time
    ReplicationFactor int
    MemLimit          uint64
}
```
A suspected peer stays in the ring and continues to receive traffic until it is
confirmed dead. This prevents routing failures during the probe window.

**Suspicion timeout: derive from existing config, no new field**

`suspicionTimeout = 3 × GossipInterval` (e.g., 9s with the default 3s interval).
This is a well-understood SWIM heuristic. No new config field needed — if the
operator tunes `GossipInterval`, the suspicion timeout scales with it.

**Indirect probes: `IndirectChecks = max(1, GossipFanout/2)`**

Same principle — derived from an existing config field. With the default `GossipFanout=3`,
this gives `IndirectChecks=1`. Increase `GossipFanout` to get more redundancy.

**Incarnation numbers replace `LastSeen` for state freshness**

Each node tracks a monotonic `selfIncarnation uint64` counter that it increments
on every heartbeat round. `PeerState` on the wire gains an `Incarnation uint64` field.
`mergeState` compares incarnations instead of wall-clock times:
- Higher incarnation → more recent, wins
- Same incarnation → tie, keep local state (idempotent)

A suspected node can refute its suspicion by incrementing its own incarnation and
gossiping `Alive=true` with the new value. Any peer seeing a higher incarnation
for a suspected node immediately cancels the suspicion.

**New message type: `MsgPingReq`**

One new message type. No new dependency. The probing node sends `MsgPingReq{TargetID, TargetAddr}`
to a random peer; that peer tries a direct heartbeat to the target and replies with
`PingReqResponse{Reachable bool}`.

### Implementation plan

**Step 1: Add suspicion state to `PeerInfo` and `cluster.go`** (`cluster.go`)

Add `Suspected bool` and `SuspectedAt time.Time` to `PeerInfo`. Add two new methods:

```go
// suspect transitions a peer from Alive to Suspected.
// No-op if already suspected or dead.
func (m *Cluster) suspect(nodeID string) {
    m.mu.Lock()
    defer m.mu.Unlock()
    p, ok := m.peers[nodeID]
    if !ok || !p.Alive || p.Suspected {
        return
    }
    p.Suspected = true
    p.SuspectedAt = time.Now()
    slog.Warn("cluster: peer suspected", "node", nodeID)
}

// cancelSuspicion clears the suspected flag when a probe succeeds or the peer refutes.
func (m *Cluster) cancelSuspicion(nodeID string) {
    m.mu.Lock()
    defer m.mu.Unlock()
    if p, ok := m.peers[nodeID]; ok {
        p.Suspected = false
    }
}
```

Add `checkSuspicions()` called from the gossip loop:
```go
func (m *Cluster) checkSuspicions() {
    timeout := m.cfg.GossipInterval * 3
    m.mu.RLock()
    var expired []string
    for nodeID, p := range m.peers {
        if p.Suspected && time.Since(p.SuspectedAt) > timeout {
            expired = append(expired, nodeID)
        }
    }
    m.mu.RUnlock()
    for _, nodeID := range expired {
        m.markDead(nodeID) // markDead already checks Alive, safe to call
    }
}
```

**Step 2: Replace immediate `markDead` with `suspect` + indirect probe** (`gossip.go`)

Change `heartbeat()`:
```go
// Before:
m.markDead(p.NodeID)

// After:
m.suspect(p.NodeID)
go m.indirectProbe(p.NodeID, p.Addr)
```

Add `indirectProbe`:
```go
func (m *Cluster) indirectProbe(targetID, targetAddr string) {
    n := max(1, m.cfg.GossipFanout/2)
    probers := m.randomAlivePeers(n)

    payload, _ := transport.Encode(transport.PingReqRequest{
        TargetID: targetID, TargetAddr: targetAddr,
    })
    frame := transport.Frame{Type: transport.MsgPingReq, Payload: payload}

    for _, p := range probers {
        client, ok := m.getClient(p.NodeID)
        if !ok {
            continue
        }
        resp, err := client.Send(frame)
        if err != nil {
            continue
        }
        var pr transport.PingReqResponse
        if transport.Decode(resp.Payload, &pr) == nil && pr.Reachable {
            m.cancelSuspicion(targetID)
            return
        }
    }
    // All indirect probes failed — leave in suspected state; checkSuspicions() handles timeout.
}
```

Add `checkSuspicions()` to the gossip loop after `evictDeadPeers()`.

**Step 3: Handle `MsgPingReq` on the receiving node** (`gossip.go` + `routing.go`)

```go
func (m *Cluster) handlePingReq(payload []byte) ([]byte, error) {
    var req transport.PingReqRequest
    if err := transport.Decode(payload, &req); err != nil {
        return nil, err
    }
    hbPayload, _ := transport.Encode(m.buildHeartbeatRequest())
    client := transport.NewClient(req.TargetAddr)
    _, err := client.Send(transport.Frame{Type: transport.MsgHeartbeat, Payload: hbPayload})
    return transport.Encode(transport.PingReqResponse{Reachable: err == nil})
}
```

Wire into `handleFrame` in `routing.go`:
```go
case transport.MsgPingReq:
    return m.handlePingReq(payload)
```

**Step 4: Incarnation numbers** (`gossip.go`, `cluster.go`, `transport/message.go`)

Add `selfIncarnation uint64` to `Cluster`. Increment it each gossip round in
`buildHeartbeatRequest`. Add `Incarnation uint64` to `transport.PeerState`.

Replace the `LastSeen` comparison in `mergeState`:
```go
// Before:
if rs.LastSeen.After(local.LastSeen) { ... }

// After:
if rs.Incarnation > local.Incarnation { ... }
```

Refutation: when a node sees itself listed as suspected in an incoming heartbeat
(rs.NodeID == m.cfg.NodeID && !rs.Alive), it increments its own incarnation and
will broadcast the higher value on the next gossip round, cancelling the suspicion
at all peers.

**Step 5: New wire types** (`transport/message.go`)

```go
MsgPingReq MsgType = 5  // add after MsgLeave

type PingReqRequest struct {
    TargetID   string
    TargetAddr string
}

type PingReqResponse struct {
    Reachable bool
}

// PeerState gains:
type PeerState struct {
    // ... existing fields ...
    Incarnation uint64
}
```

**Step 6: Tests**

- Test that a node paused for `2 × GossipInterval` is NOT evicted immediately (stays suspected)
- Test that a node paused for `4 × GossipInterval` IS eventually evicted (suspicion timeout fires)
- Test that indirect probe success cancels suspicion (mock the prober's response)
- Test refutation: a node marked suspected broadcasts a higher incarnation and peers cancel it

### Key files to touch

- `internal/cluster/cluster.go` — `PeerInfo` struct, `suspect`, `cancelSuspicion`, `checkSuspicions`
- `internal/cluster/gossip.go` — `heartbeat` (remove `markDead`, add `suspect`+`indirectProbe`), `startGossip` loop, `buildHeartbeatRequest` (incarnation), `mergeState` (incarnation comparison + refutation), `indirectProbe`, `handlePingReq`
- `internal/cluster/routing.go` — wire `MsgPingReq` in `handleFrame`
- `internal/transport/message.go` — `MsgPingReq`, `PingReqRequest`, `PingReqResponse`, `Incarnation` on `PeerState`

---

## 3. Split-brain recovery

### Problem

When a network partition splits the cluster and both halves accept writes to the same
key, the current rebalance logic (`handleRebalance`) does an unconditional `store.Set`.
Whichever node's rebalance batch arrives last wins. This is non-deterministic.

### Root cause in the code

`rebalance.go:handleRebalance` line 36:
```go
m.store.Set(re.Key, entry)   // always overwrites, no conflict check
```

There is no write timestamp on any stored value, so there is no basis for comparison.

### Design decisions

**Conflict resolution strategy: Last-Write-Wins (LWW) with wall-clock timestamps**

For a cache, LWW is appropriate. The goal is not strong consistency but predictability:
given two conflicting writes, always keep the newer one. Wall-clock timestamps are
acceptable here with a documented assumption (clocks must be within a small skew window,
e.g. 5 seconds — achievable with NTP in any real deployment).

Alternative (vector clocks / version vectors) would handle causality correctly but
adds significant complexity and is overkill for a cache where data is ephemeral.

**Where to store the timestamp**

On `DataStructure` itself, not in a separate metadata layer. Add a `WriteAt int64`
field (Unix nanoseconds) to `DataStore`'s shard entry wrapper. Encode it alongside
the existing `KeyExpiry` during rebalance.

**Epoch numbers for ring stability**

A secondary mechanism: each ring topology change increments a monotonic epoch counter
that is gossiped alongside node state. Writes carry the epoch at which they were made.
During rebalance, if both copies of a key have the same timestamp (clock skew or
simultaneous writes), the one written during the higher epoch wins.

### Implementation plan

1. **Add `WriteAt int64` to `DataStructure` interface** (`internal/store/store.go`)

   ```go
   type DataStructure interface {
       // ... existing methods ...
       WriteAt() int64      // unix nanoseconds of last write, set by DataStore
       SetWriteAt(t int64)
   }
   ```

   Implement on `ValueStructure`, `SetStructure`, `HashStructure`.

2. **Stamp `WriteAt` on every write in `DataStore.Set` and `DataStore.Apply`**

   ```go
   func (ds *DataStore) Set(key string, e DataStructure) {
       e.SetWriteAt(time.Now().UnixNano())
       // ... existing logic
   }
   ```

3. **Carry `WriteAt` through the rebalance wire format**

   Add `WriteAt int64` to `transport.RebalanceEntry`. Encode it in `rebalance.run()`
   alongside `TTL`.

4. **Conflict check in `handleRebalance`**

   Replace the unconditional `store.Set` with a compare-and-set:

   ```go
   m.store.ApplyIfNewer(re.Key, entry, re.WriteAt)
   ```

   Add `ApplyIfNewer` to `DataStore`:
   ```go
   func (ds *DataStore) ApplyIfNewer(key string, incoming DataStructure, writeAt int64) {
       s := ds.getShard(key)
       s.mu.Lock()
       defer s.mu.Unlock()
       existing, ok := s.data[key]
       if ok && existing.WriteAt() >= writeAt {
           return // existing copy is same age or newer — keep it
       }
       incoming.SetWriteAt(writeAt)
       s.data[key] = incoming
   }
   ```

5. **Add epoch to ring topology** (`internal/ring/ring.go`)

   Increment a `version` (already present as `GetVersion()`) on every topology change.
   Expose it as the epoch. Carry the epoch in the rebalance entry as a tiebreaker:

   ```go
   type RebalanceEntry struct {
       // ...
       WriteAt int64  `msgpack:"wa"`
       Epoch   uint64 `msgpack:"ep"`
   }
   ```

   In `ApplyIfNewer`, if `writeAt` timestamps are equal (within 1ms tolerance for
   clock skew), higher epoch wins.

6. **Update `Encode`/`Decode` on all three store types** to include `WriteAt` in
   the wire format so it survives rebalance migrations.

7. **Add a test** that writes the same key on two isolated nodes, reconnects them,
   forces a rebalance, and asserts the newer write survived.

### Key files to touch

- `internal/store/store.go` — DataStructure interface, DataStore.Set/Apply, new ApplyIfNewer
- `internal/store/value.go`, `set.go`, `hash.go` — WriteAt field + wire encoding
- `internal/transport/message.go` — RebalanceEntry gets WriteAt + Epoch
- `internal/cluster/rebalance.go` — handleRebalance uses ApplyIfNewer; run() encodes WriteAt
- `internal/ring/ring.go` — expose epoch (version already exists)

---

## Suggested order

1. **Memory cap + LRU** — highest operational risk today; an OOM kills the process.
   Self-contained change, no protocol changes, no new dependencies.

2. **Split-brain recovery** — touches the wire format (rebalance entries), so do
   this before adding new nodes to any existing deployment. Relatively small change,
   high correctness value.

3. **Hardened gossip** — biggest scope (new dependency, removes a whole subsystem).
   Do this last so the ring and rebalance logic are stable before changing how
   membership events are delivered.
