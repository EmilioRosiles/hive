# Hive Refactor Plan

## Goals

1. **Centralize dispatch** — replace per-op routing boilerplate with a single dispatcher driven by an `OpScope` enum
2. **Split `Node` and `Cache`** — `Node` owns cluster lifecycle; `Cache` is the data-access handle passed to stores
3. **Move scan to stores** — scatter-gather lives on individual stores, not on `Node` or `Cache`

---

## Phase 1 — OpScope + centralized dispatch

The core problem: every public method in `ops.go` (Set, Get, HSet, SAdd, …) repeats the same three-step routing pattern:

```
localIfResponsible → forwardIfNotPrimary → replicateToReplicas
```

This logic is duplicated ~15 times, making it easy to get subtly wrong and hard to audit.

### 1.1 — Define `OpScope`

Add to `internal/transport/message.go` (or a new `internal/cluster/opdefs.go`):

```go
type OpScope uint8

const (
    ScopeWrite  OpScope = iota // route to primary owner, replicate to replicas
    ScopeRead                  // route to primary owner, no replication
    ScopeLocal                 // always execute on the receiving node (e.g. rebalance delivery)
)
```

Three mutually exclusive states replace the `Replicate bool` + `ReadOnly bool` pair, eliminating the contradictory state (`Replicate: true, ReadOnly: true`).

### 1.2 — Extend opRegistry entries to carry scope

In `handler.go`, change `opFn` to `opDef`:

```go
type opDef struct {
    Exec  func(m *Manager, key string, payload []byte) ([]byte, error)
    Scope OpScope
}

var opRegistry = map[transport.Op]opDef{
    transport.OpDel:           {Exec: execDel,           Scope: ScopeWrite},
    transport.OpExpire:        {Exec: execExpire,        Scope: ScopeWrite},
    transport.OpValueSet:      {Exec: execValueSet,      Scope: ScopeWrite},
    transport.OpValueGet:      {Exec: execValueGet,      Scope: ScopeRead},
    transport.OpSAdd:          {Exec: execSAdd,          Scope: ScopeWrite},
    transport.OpSRem:          {Exec: execSRem,          Scope: ScopeWrite},
    transport.OpSIsMember:     {Exec: execSIsMember,     Scope: ScopeRead},
    transport.OpSMembers:      {Exec: execSMembers,      Scope: ScopeRead},
    transport.OpSCard:         {Exec: execSCard,         Scope: ScopeRead},
    transport.OpSExpireMember: {Exec: execSExpireMember, Scope: ScopeWrite},
    transport.OpHSet:          {Exec: execHSet,          Scope: ScopeWrite},
    transport.OpHGet:          {Exec: execHGet,          Scope: ScopeRead},
    transport.OpHDel:          {Exec: execHDel,          Scope: ScopeWrite},
    transport.OpHGetAll:       {Exec: execHGetAll,       Scope: ScopeRead},
    transport.OpHKeys:         {Exec: execHKeys,         Scope: ScopeRead},
    transport.OpHExpireField:  {Exec: execHExpireField,  Scope: ScopeWrite},
}
```

Update `handleForward` to use `def.Exec` instead of a bare function lookup — no behaviour change here, just a field dereference.

### 1.3 — Write a single `dispatch` method

Add to `ops.go`:

```go
// dispatch routes op to the correct node(s) based on its scope and executes it.
// For ScopeRead it forwards to the primary if not local and returns the raw response bytes.
// For ScopeWrite it runs locally and/or forwards to primary, then replicates asynchronously.
// For ScopeLocal it always executes here regardless of ring ownership.
func (m *Manager) dispatch(op transport.Op, key string, payload any) ([]byte, error) {
    def, ok := opRegistry[op]
    if !ok {
        return nil, fmt.Errorf("cluster: unknown op %d", op)
    }

    var encoded []byte
    if payload != nil {
        var err error
        encoded, err = transport.Encode(payload)
        if err != nil {
            return nil, err
        }
    }

    nodes := m.responsibleNodes(key)

    switch def.Scope {
    case ScopeRead:
        if m.localIsResponsible(nodes) {
            return def.Exec(m, key, encoded)
        }
        resp, err := m.forwardOpWithResponse(nodes[0], op, key, payload)
        if err != nil {
            return nil, err
        }
        return resp.Payload, nil

    case ScopeWrite:
        req, err := buildReq(op, key, payload)
        if err != nil {
            return nil, err
        }
        if m.localIsResponsible(nodes) {
            if _, err := def.Exec(m, key, encoded); err != nil {
                return nil, err
            }
        } else if len(nodes) > 0 {
            if _, err := m.sendReq(nodes[0], req); err != nil {
                return nil, err
            }
        }
        m.fanOutReplicas(req, nodes)
        return nil, nil

    case ScopeLocal:
        return def.Exec(m, key, encoded)
    }

    return nil, fmt.Errorf("cluster: unhandled scope for op %d", op)
}

// fanOutReplicas sends req to nodes[1:] asynchronously, skipping this node.
func (m *Manager) fanOutReplicas(req transport.ForwardRequest, nodes []string) {
    if len(nodes) <= 1 {
        return
    }
    for _, nodeID := range nodes[1:] {
        if nodeID == m.cfg.NodeID {
            continue
        }
        go func() {
            if _, err := m.sendReq(nodeID, req); err != nil {
                m.handlePeerError(nodeID)
            }
        }()
    }
}
```

### 1.4 — Reduce public op methods to payload-build + dispatch

Each public method in `ops.go` becomes a thin wrapper:

```go
func (m *Manager) Set(key string, data []byte) error {
    _, err := m.dispatch(transport.OpValueSet, key, transport.ValueSetPayload{Data: data})
    return err
}

func (m *Manager) Get(key string) ([]byte, error) {
    raw, err := m.dispatch(transport.OpValueGet, key, nil)
    if err != nil {
        return nil, err
    }
    var dr transport.DataResponse
    if err := transport.Decode(raw, &dr); err != nil {
        return nil, err
    }
    return dr.Data, nil
}

func (m *Manager) HSet(key, field string, data []byte, ttl time.Duration) error {
    _, err := m.dispatch(transport.OpHSet, key, transport.HSetPayload{Field: field, Data: data, TTLNs: ttl.Nanoseconds()})
    return err
}
// … etc for all ops
```

The `replicateOp`, `forwardOp`, and `forwardOpWithResponse` helpers can be removed or kept as internal helpers; `dispatch` replaces their call sites.

### TODO — Phase 1

- [ ] Add `OpScope` type and constants to `internal/transport/message.go`
- [ ] Define `opDef` struct in `internal/cluster/handler.go`
- [ ] Rebuild `opRegistry` as `map[transport.Op]opDef` with scope annotations
- [ ] Update `handleForward` to call `def.Exec`
- [ ] Write `dispatch` and `fanOutReplicas` in `internal/cluster/ops.go`
- [ ] Rewrite all public op methods as thin payload-build + dispatch wrappers
- [ ] Delete `replicateOp`, `forwardOp`, `forwardOpWithResponse` call sites (keep `sendReq` and `buildReq`)
- [ ] Run existing tests to confirm no regression

---

## Phase 2 — Node / Cache split

### The problem

`Node` currently carries two responsibilities:
- **Cluster participation**: gossip, membership, transport, lifecycle (`Shutdown`, `Status`)
- **Data access**: the thing you pass to store constructors

These are different roles. Calling `hive.NewValueStore[Session](node, "sessions")` is slightly wrong conceptually — you're not creating a store on *this node*, you're creating a namespaced view over the *cluster's data layer*.

### 2.1 — Introduce `Cache`

In `hive.go`, add:

```go
// Cache is a handle to the cluster's data layer. It is obtained from a Node
// and passed to store constructors. Multiple stores can share one Cache.
type Cache struct {
    cluster *cluster.Manager
}

// Cache returns a data-access handle for this node's cluster.
// Store constructors (NewValueStore, NewSetStore, NewHashStore) take a *Cache.
func (n *Node) Cache() *Cache {
    return &Cache{cluster: n.cluster}
}
```

`Node` keeps only what relates to cluster participation:

```go
func NewNode(cfg Config) (*Node, error) { … }
func (n *Node) Shutdown() error         { … }
func (n *Node) Status() ClusterStatus   { … }
func (n *Node) Cache() *Cache           { … }
```

### 2.2 — Update store constructors

Change the first parameter of all three constructors from `*Node` to `*Cache`:

```go
// Before
func NewValueStore[T any](n *Node, name string) *ValueStore[T]
func NewSetStore(n *Node, name string) *SetStore
func NewHashStore[T any](n *Node, name string) *HashStore[T]

// After
func NewValueStore[T any](c *Cache, name string) *ValueStore[T]
func NewSetStore(c *Cache, name string) *SetStore
func NewHashStore[T any](c *Cache, name string) *HashStore[T]
```

Inside `ValueStore`, `SetStore`, `HashStore` structs replace the `*Node` field with `*Cache`:

```go
type ValueStore[T any] struct {
    cache  *Cache
    name   string
}
```

All ops on those stores route through `cache.cluster.*` instead of `node.cluster.*`.

### Updated user-facing API

```go
node, _ := hive.NewNode(hive.Config{…})
defer node.Shutdown()

cache := node.Cache()

sessions := hive.NewValueStore[Session](cache, "sessions")
online   := hive.NewSetStore(cache, "online_users")
streams  := hive.NewHashStore[Stream](cache, "streams")
```

`Node` reads like what it is: a cluster participant you start and stop.
`Cache` reads like what it is: the data handle you pass around.

### TODO — Phase 2

- [ ] Add `Cache` struct and `node.Cache()` method to `hive.go`
- [ ] Update `value.go` — `NewValueStore` takes `*Cache`, stores `cache *Cache`
- [ ] Update `set.go`   — `NewSetStore`   takes `*Cache`, stores `cache *Cache`
- [ ] Update `hash.go`  — `NewHashStore`  takes `*Cache`, stores `cache *Cache`
- [ ] Update README examples to use `node.Cache()` pattern
- [ ] Confirm `cluster_test.go` compiles and passes

---

## Phase 3 — Scan on stores

### The problem

A cluster-wide key scan does not map cleanly to the `dispatch` model because it has no single owner — it needs to scatter to all nodes and aggregate. Putting it on `Node` or `Cache` as a generic method (`node.Scan()`, `cache.Keys()`) is awkward because the caller always wants keys scoped to a specific namespace.

### Solution — `Keys()` on each store

Each store type exposes a `Keys()` method scoped to its own namespace. The scatter-gather implementation lives on `Cache` as an unexported helper.

```go
// ValueStore
func (vs *ValueStore[T]) Keys() ([]string, error) {
    return vs.cache.scanPrefix(vs.name)
}

// SetStore
func (ss *SetStore) Keys() ([]string, error) {
    return ss.cache.scanPrefix(ss.name)
}

// HashStore
func (hs *HashStore[T]) Keys() ([]string, error) {
    return hs.cache.scanPrefix(hs.name)
}
```

`Cache.scanPrefix` fans out to all alive peers and the local store:

```go
// scanPrefix returns all keys with the given namespace prefix across the cluster.
func (c *Cache) scanPrefix(prefix string) ([]string, error) {
    // 1. local scan
    local := c.cluster.LocalKeys(prefix)

    // 2. fan out to all alive peers (parallel)
    peers := c.cluster.AlivePeerIDs()
    results := make(chan []string, len(peers))
    for _, id := range peers {
        go func() {
            keys, err := c.cluster.RemoteKeys(id, prefix)
            if err == nil {
                results <- keys
            } else {
                results <- nil
            }
        }()
    }

    all := local
    for range peers {
        all = append(all, <-results...)
    }
    return all, nil
}
```

This requires:
- A `LocalKeys(prefix string) []string` on `cluster.Manager` (scan the local `DataStore`)
- A `RemoteKeys(nodeID, prefix string) ([]string, error)` — a new transport op (`OpScanPrefix`) with `ScopeLocal` scope, so when forwarded it executes locally on the target node without further routing

### TODO — Phase 3

- [ ] Add `OpScanPrefix` to `internal/transport/message.go`
- [ ] Add `ScanPrefixPayload` and `StringsResponse` (already exists) to message types
- [ ] Implement `execScanPrefix` handler in `handler.go` — scans local `DataStore` by prefix, returns `StringsResponse`
- [ ] Register `OpScanPrefix` in `opRegistry` with `Scope: ScopeLocal`
- [ ] Add `LocalKeys(prefix string) []string` to `cluster.Manager`
- [ ] Add `RemoteKeys(nodeID, prefix string) ([]string, error)` to `cluster.Manager`
- [ ] Add unexported `scanPrefix(prefix string) ([]string, error)` to `Cache`
- [ ] Add `Keys() ([]string, error)` to `ValueStore`, `SetStore`, `HashStore`
- [ ] Add store-level scan to the internal `DataStore` (filter by key prefix)

---

## File change summary

| File | Change |
|------|--------|
| `internal/transport/message.go` | Add `OpScope` type + constants; add `OpScanPrefix`; add `ScanPrefixPayload` |
| `internal/cluster/handler.go` | `opFn` → `opDef{Exec, Scope}`; rebuild `opRegistry`; update `handleForward` |
| `internal/cluster/ops.go` | Add `dispatch`, `fanOutReplicas`; slim all public methods to payload+dispatch; delete old routing helpers |
| `internal/cluster/cluster.go` | Add `LocalKeys`, `RemoteKeys` |
| `hive.go` | Add `Cache` struct; add `Node.Cache()`; keep `Node` for lifecycle only |
| `value.go` | `NewValueStore` takes `*Cache`; add `Keys()` |
| `set.go` | `NewSetStore` takes `*Cache`; add `Keys()` |
| `hash.go` | `NewHashStore` takes `*Cache`; add `Keys()` |
| `internal/store/store.go` | Add prefix scan to `DataStore` |
