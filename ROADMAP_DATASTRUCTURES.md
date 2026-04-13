# Data Structures Roadmap

Plan for adding four new data structure types to Hive: List, ZSet, Geo, and VSet.

Each section covers the internal representation, the public API, the full list of
operations, and every file that needs to change. The pattern to follow is the same
for all four — study `SetStructure` + `HashStore` for the definitive reference.

---

## How to add a new data structure (the pattern)

Every data structure requires changes in exactly six places:

1. **`internal/store/<type>.go`** — the `DataStructure` implementation
   - Unexported struct with the actual data
   - `Kind()`, `KeyExpiry()`, `SetKeyExpiry()`, `Cleanup()`, `Encode()` methods
   - `Decode<Type>Structure([]byte)` constructor for rebalance
   - Wire struct(s) for msgpack serialization

2. **`internal/store/store.go`** — register the decoder
   - Add `Kind<Type>` constant
   - Add decoder entry in `NewDataStore`'s `decoders` map

3. **`internal/transport/message.go`** — define the ops
   - Add op constants in the reserved range for this type
   - Add any new request/response structs if needed (most ops use raw `[][]byte` slots)

4. **`internal/cluster/exec.go`** — local execution functions
   - One `exec<Op>` function per op
   - Arg index constants at the top of the file

5. **`internal/cluster/routing.go`** — register ops in `opRegistry`
   - `ScopeWrite` for mutations, `ScopeRead` for queries

6. **Root package `<type>.go`** — the typed public API
   - `<Type>Store[T]` struct with `*Cache` and `prefix string`
   - `New<Type>Store[T](cache *Cache, name string)` constructor
   - Namespace pattern: `name + ":<letter>:"`

---

## 1. List

### What it is

An ordered sequence of byte slices. Supports efficient push/pop from both ends,
range reads, and index access. The natural fit is a slice — linked lists add
pointer overhead and hurt cache locality for the sequential access patterns common
in a cache (range reads, encode for rebalance).

### Internal representation

```go
// internal/store/list.go
type ListStructure struct {
    items     [][]byte
    expiresAt int64 // unix seconds, key-level TTL
}
```

`items[0]` is the left end (head). `LPush` prepends; `RPush` appends.
No per-element TTL — list elements don't independently expire (unlike SetStructure members).

### Operations

| Op | Scope | Args | Returns | Notes |
|---|---|---|---|---|
| `LPush` | Write | data `[]byte` | — | prepend to head |
| `RPush` | Write | data `[]byte` | — | append to tail |
| `LPop` | Write | — | data `[]byte` | remove+return head; ErrNotFound if empty |
| `RPop` | Write | — | data `[]byte` | remove+return tail; ErrNotFound if empty |
| `LLen` | Read | — | count uint64 | number of elements |
| `LIndex` | Read | index int64 | data `[]byte` | negative index counts from tail; ErrNotFound if OOB |
| `LRange` | Read | start, stop int64 | `[][]byte` | inclusive; negative indices supported; clipped silently |
| `LSet` | Write | index int64, data `[]byte` | — | overwrite element; ErrNotFound if OOB |

### Op range

Reserve `Op = 150–169` for List ops.

```go
OpLPush  Op = 150
OpRPush  Op = 151
OpLPop   Op = 152
OpRPop   Op = 153
OpLLen   Op = 154
OpLIndex Op = 155
OpLRange Op = 156
OpLSet   Op = 157
```

### Public API (`list.go`)

```go
type ListStore[T any] struct {
    cache  *Cache
    prefix string // name + ":l:"
}

func (l *ListStore[T]) LPush(key string, value T) error
func (l *ListStore[T]) RPush(key string, value T) error
func (l *ListStore[T]) LPop(key string) (T, error)
func (l *ListStore[T]) RPop(key string) (T, error)
func (l *ListStore[T]) LLen(key string) (int, error)
func (l *ListStore[T]) LIndex(key string, index int) (T, error)
func (l *ListStore[T]) LRange(key string, start, stop int) ([]T, error)
func (l *ListStore[T]) LSet(key string, index int, value T) error
func (l *ListStore[T]) Del(key string) error
func (l *ListStore[T]) Expire(key string, ttl time.Duration) error
```

### Implementation notes

- `LPop`/`RPop` are write ops (`ScopeWrite`) because they mutate the list.
  They return data — unlike most write ops, they return `[][]byte{element}`.
  The routing layer already supports this: `ScopeWrite` callers receive the
  return value when the primary executes locally; for forwarded writes, the
  primary's return value comes back in `ForwardResponse.Results`.
  **Check that `dispatch` in `routing.go` actually returns the result for
  `ScopeWrite` when executing locally — it currently returns `nil, nil`.
  This needs to be fixed before implementing any write-with-return op.**

- Negative index normalization: `resolveIndex(i int64, length int) (int, bool)` helper
  in `exec.go` — converts negative indices (Python-style) to absolute positions and
  returns `false` if out of bounds.

- `LRange` clips silently: `start < 0` → 0, `stop >= len` → `len-1`. Never errors on
  out-of-range bounds, consistent with Redis behavior.

### Files to touch

- `internal/store/list.go` — new
- `internal/store/store.go` — `KindList`, decoder entry
- `internal/transport/message.go` — Op constants 150–157
- `internal/cluster/exec.go` — `execLPush`, `execRPush`, `execLPop`, `execRPop`, `execLLen`, `execLIndex`, `execLRange`, `execLSet`; arg index constants
- `internal/cluster/routing.go` — 8 entries in `opRegistry`
- `list.go` — new (root package)

---

## 2. ZSet (Sorted Set)

### What it is

A set of unique string members each associated with a `float64` score. The set is
always kept in score order. Supports rank-based and score-range-based access.
Classic use cases: leaderboards, priority queues, time-ordered event logs.

### Internal representation

```go
// internal/store/zset.go
type ZSetStructure struct {
    scores  map[string]float64 // O(1) score lookup by member
    sorted  []zsetEntry        // score-ordered slice for rank/range ops
    expiresAt int64
}

type zsetEntry struct {
    Member string
    Score  float64
}
```

`sorted` is maintained in ascending score order. On `ZAdd`, binary search to find
the insertion point, splice in the new entry. On `ZRem`, binary search + remove.
This is O(n) for mutations but O(log n) for lookups, which is acceptable for
cache-sized sorted sets (typically < 10K entries).

For very large sorted sets a skip list would be better, but it is significantly
more complex to implement and serialize correctly. Start with the sorted slice and
revisit if benchmarks show it is a bottleneck.

Tiebreaking: when two members have the same score, order lexicographically by member
name. This matches Redis behavior and makes range results deterministic.

### Operations

| Op | Scope | Args | Returns | Notes |
|---|---|---|---|---|
| `ZAdd` | Write | score float64 (8 bytes BE), member string | — | upsert; updates score if member exists |
| `ZRem` | Write | member string | — | no-op if missing |
| `ZScore` | Read | member string | score float64 (8 bytes BE) | ErrNotFound if missing |
| `ZRank` | Read | member string | rank uint64 | 0 = lowest score; ErrNotFound if missing |
| `ZCard` | Read | — | count uint64 | |
| `ZRange` | Read | start, stop int64 | `[][]byte` alternating member+score | rank range, inclusive, negative ok |
| `ZRangeByScore` | Read | min, max float64 (8 bytes each) | `[][]byte` alternating member+score | score range, inclusive |
| `ZRevRank` | Read | member string | rank uint64 | 0 = highest score |

### Op range

Reserve `Op = 170–189` for ZSet ops.

```go
OpZAdd          Op = 170
OpZRem          Op = 171
OpZScore        Op = 172
OpZRank         Op = 173
OpZCard         Op = 174
OpZRange        Op = 175
OpZRangeByScore Op = 176
OpZRevRank      Op = 177
```

### Encoding float64 on the wire

Encode `float64` as 8-byte IEEE 754 big-endian (same pattern as `encodeUint64`).
Add helpers in `exec.go`:
```go
func encodeFloat64(f float64) []byte {
    return binary.BigEndian.AppendUint64(nil, math.Float64bits(f))
}
func decodeFloat64(b []byte) float64 {
    return math.Float64frombits(binary.BigEndian.Uint64(b))
}
```

### Public API (`zset.go`)

```go
type ZSetStore struct {
    cache  *Cache
    prefix string // name + ":z:"
}

func (z *ZSetStore) ZAdd(key string, score float64, member string) error
func (z *ZSetStore) ZRem(key, member string) error
func (z *ZSetStore) ZScore(key, member string) (float64, error)
func (z *ZSetStore) ZRank(key, member string) (int, error)
func (z *ZSetStore) ZRevRank(key, member string) (int, error)
func (z *ZSetStore) ZCard(key string) (int, error)
func (z *ZSetStore) ZRange(key string, start, stop int) ([]ZSetEntry, error)
func (z *ZSetStore) ZRangeByScore(key string, min, max float64) ([]ZSetEntry, error)
func (z *ZSetStore) Del(key string) error
func (z *ZSetStore) Expire(key string, ttl time.Duration) error

type ZSetEntry struct {
    Member string
    Score  float64
}
```

`ZSetStore` is not generic — members are always strings (scores are `float64`).

### Implementation notes

- The `sorted` slice + `scores` map must be kept in sync on every mutation.
  Wrap all mutations in a helper that updates both atomically:
  ```go
  func (z *ZSetStructure) add(member string, score float64) { ... }
  func (z *ZSetStructure) remove(member string) { ... }
  ```
- Binary search for insertion/removal: use `slices.SortedIndexFunc` or implement
  a simple `lowerBound` over `sorted`.
- `ZRangeByScore` needs two binary searches (lower bound of min, upper bound of max),
  then a slice of the result.
- For rebalance encoding, encode `sorted` directly (member+score pairs in order).
  Reconstruct `scores` map from it on decode — no need to encode both.

### Files to touch

- `internal/store/zset.go` — new
- `internal/store/store.go` — `KindZSet`, decoder entry
- `internal/transport/message.go` — Op constants 170–177
- `internal/cluster/exec.go` — 8 exec functions, float64 encode/decode helpers
- `internal/cluster/routing.go` — 8 entries in `opRegistry`
- `zset.go` — new (root package)

---

## 3. Geo

### What it is

A geospatial index mapping string member names to (longitude, latitude) coordinates.
Supports distance calculation between members and radius queries (find all members
within X meters/km of a point).

Internally implemented as a ZSet where the score is a GeoHash integer — a 52-bit
integer that encodes lat/lon by interleaving their binary representations. Because
GeoHash values are spatially local (nearby coordinates produce nearby integers),
a ZSet range query over GeoHash values approximates a radius query, with a secondary
distance filter to exclude false positives at the cell boundary.

### Internal representation

```go
// internal/store/geo.go
type GeoStructure struct {
    members   map[string]uint64  // member → geohash (52-bit)
    sorted    []geoEntry         // geohash-ordered for range queries
    expiresAt int64
}

type geoEntry struct {
    Member  string
    GeoHash uint64
}
```

This is identical in shape to `ZSetStructure` with `float64` replaced by `uint64`.
Consider whether to share the sorted-slice logic or duplicate it.

### GeoHash implementation (no external dependency)

A 52-bit GeoHash interleaves 26 bits of longitude precision and 26 bits of latitude
precision. Implementation is ~60 lines of pure bit manipulation:

```go
// Encode (lon, lat) → 52-bit geohash integer
func geoEncode(lon, lat float64) uint64 { ... }

// Decode 52-bit geohash → (lon, lat)
func geoDecode(hash uint64) (lon, lat float64) { ... }

// Neighbors returns the 8 adjacent geohash cells at the same precision.
// Needed for radius queries that span cell boundaries.
func geoNeighbors(hash uint64, bits int) [8]uint64 { ... }
```

Haversine distance formula for `GeoDist` (great-circle distance):
```go
func haversine(lon1, lat1, lon2, lat2 float64) float64 { ... }
```

Both are pure math — no imports beyond `math`.

### Radius query algorithm

1. Determine the GeoHash precision (number of bits) that gives cells slightly larger
   than the query radius. A lookup table maps radius → bit precision.
2. Compute the center cell hash and its 8 neighbors.
3. For each of the 9 cells, do a ZSet-style range query over `sorted` to find members
   whose hashes fall within that cell's range.
4. For each candidate, decode the geohash to (lon, lat) and apply Haversine to filter
   out false positives.

### Operations

| Op | Scope | Args | Returns | Notes |
|---|---|---|---|---|
| `GeoAdd` | Write | lon float64, lat float64, member string | — | upsert |
| `GeoRem` | Write | member string | — | no-op if missing |
| `GeoPos` | Read | member string | lon, lat float64 | ErrNotFound if missing |
| `GeoDist` | Read | member1, member2 string, unit string | dist float64 | units: m, km, mi, ft |
| `GeoRadius` | Read | lon, lat, radius float64, unit string, count int | `[][]byte` member names | sorted by distance; count=0 means no limit |
| `GeoSearch` | Read | member string, radius float64, unit string, count int | `[][]byte` member names | like GeoRadius but center is a member's own position |

### Op range

Reserve `Op = 190–209` for Geo ops.

```go
OpGeoAdd    Op = 190
OpGeoRem    Op = 191
OpGeoPos    Op = 192
OpGeoDist   Op = 193
OpGeoRadius Op = 194
OpGeoSearch Op = 195
```

### Public API (`geo.go`)

```go
type GeoStore struct {
    cache  *Cache
    prefix string // name + ":g:"
}

type GeoUnit string
const (
    GeoMeters     GeoUnit = "m"
    GeoKilometers GeoUnit = "km"
    GeoMiles      GeoUnit = "mi"
    GeoFeet       GeoUnit = "ft"
)

func (g *GeoStore) GeoAdd(key string, lon, lat float64, member string) error
func (g *GeoStore) GeoRem(key, member string) error
func (g *GeoStore) GeoPos(key, member string) (lon, lat float64, err error)
func (g *GeoStore) GeoDist(key, member1, member2 string, unit GeoUnit) (float64, error)
func (g *GeoStore) GeoRadius(key string, lon, lat, radius float64, unit GeoUnit, count int) ([]string, error)
func (g *GeoStore) GeoSearch(key, member string, radius float64, unit GeoUnit, count int) ([]string, error)
func (g *GeoStore) Del(key string) error
func (g *GeoStore) Expire(key string, ttl time.Duration) error
```

### Wire encoding

`GeoRadius` and `GeoSearch` have several arguments. Use a fixed positional layout:
```
args[0] = lon (8 bytes IEEE 754 BE)   // or member name for GeoSearch
args[1] = lat (8 bytes)               // absent for GeoSearch
args[2] = radius (8 bytes)
args[3] = unit (1 byte: 0=m,1=km,2=mi,3=ft)
args[4] = count (8 bytes uint64; 0 = no limit)
```

### Files to touch

- `internal/store/geo.go` — new (includes GeoHash math: `geoEncode`, `geoDecode`, `geoNeighbors`, `haversine`)
- `internal/store/store.go` — `KindGeo`, decoder entry
- `internal/transport/message.go` — Op constants 190–195
- `internal/cluster/exec.go` — 6 exec functions
- `internal/cluster/routing.go` — 6 entries in `opRegistry`
- `geo.go` — new (root package)

---

## 4. VSet (Vector Set)

### What it is

A set of named vectors (float32 slices) supporting approximate nearest-neighbor (ANN)
search. Enables embedding-based similarity search directly in the cache — useful for
semantic caching, recommendation, deduplication by embedding.

### Design decision: brute force first, HNSW later

For the initial implementation, use **brute-force cosine similarity search**:
compute the dot product between the query vector and every stored vector.

Complexity: O(n × d) per query, where n = number of vectors and d = dimensions.
For n < 10K and d < 1K (realistic cache use cases), this is well under 1ms per query.

When n grows beyond ~50K, upgrade to **HNSW** (Hierarchical Navigable Small World
graph). HNSW gives O(log n) ANN search with excellent recall. It is complex to
implement correctly — plan it as a separate phase, not part of the initial VSet work.

The store interface does not change between phases. HNSW is an internal
implementation detail of `VSetStructure`.

### Internal representation (brute force phase)

```go
// internal/store/vset.go
type VSetStructure struct {
    dims    int                // vector dimension; validated on first insert
    vectors map[string][]float32
    expiresAt int64
}
```

### Vector similarity

Cosine similarity is the standard for embedding vectors:
```go
func cosineSim(a, b []float32) float32 {
    var dot, normA, normB float32
    for i := range a {
        dot  += a[i] * b[i]
        normA += a[i] * a[i]
        normB += b[i] * b[i]
    }
    if normA == 0 || normB == 0 {
        return 0
    }
    return dot / (sqrt32(normA) * sqrt32(normB))
}
```

For pre-normalized vectors (unit norm, common with OpenAI/sentence-transformer
embeddings), cosine similarity reduces to a dot product — skip the normalization
divisions and it becomes faster.

### Operations

| Op | Scope | Args | Returns | Notes |
|---|---|---|---|---|
| `VAdd` | Write | member string, vector `[]float32` | — | upsert; errors if dims mismatch |
| `VRem` | Write | member string | — | no-op if missing |
| `VGet` | Read | member string | vector `[]float32` | ErrNotFound if missing |
| `VDims` | Read | — | dims uint64 | dimension count for this key |
| `VCard` | Read | — | count uint64 | number of stored vectors |
| `VSearch` | Read | query `[]float32`, k uint64 | `[][]byte` alternating member+score | top-k by cosine similarity; score as float32 (4 bytes) |

### Op range

Reserve `Op = 210–229` for VSet ops.

```go
OpVAdd    Op = 210
OpVRem    Op = 211
OpVGet    Op = 212
OpVDims   Op = 213
OpVCard   Op = 214
OpVSearch Op = 215
```

### Wire encoding for vectors

Encode `[]float32` as raw little-endian IEEE 754 bytes (4 bytes per element).
Add helpers in `exec.go`:
```go
func encodeVector(v []float32) []byte {
    b := make([]byte, len(v)*4)
    for i, f := range v {
        binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
    }
    return b
}
func decodeVector(b []byte) []float32 {
    v := make([]float32, len(b)/4)
    for i := range v {
        v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
    }
    return v
}
```

Little-endian matches the native layout of most vector libraries (numpy, PyTorch).

### VSearch result format

`VSearch` returns alternating `[member, score, member, score, ...]` byte slices.
Each score is 4 bytes (float32 LE). The results are sorted descending by score
(most similar first).

### Public API (`vset.go`)

```go
type VSetStore struct {
    cache  *Cache
    prefix string // name + ":vs:"
}

type VSearchResult struct {
    Member string
    Score  float32
}

func (v *VSetStore) VAdd(key, member string, vector []float32) error
func (v *VSetStore) VRem(key, member string) error
func (v *VSetStore) VGet(key, member string) ([]float32, error)
func (v *VSetStore) VDims(key string) (int, error)
func (v *VSetStore) VCard(key string) (int, error)
func (v *VSetStore) VSearch(key string, query []float32, k int) ([]VSearchResult, error)
func (v *VSetStore) Del(key string) error
func (v *VSetStore) Expire(key string, ttl time.Duration) error
```

`VSetStore` is not generic — vectors are always `[]float32`, members are strings.

### Rebalance encoding note

`vectors` is a `map[string][]float32`. The map is small in practice but can be large
if vectors have high dimensions. For rebalance, encode as a flat binary blob:

```
[4 bytes: dims uint32][for each entry: 2-byte member len, member bytes, dims×4 float32 bytes]
```

This avoids the msgpack overhead of encoding each vector as a list of floats.

### Future HNSW phase

When upgrading from brute force to HNSW:
- The `VSetStructure` struct gains an HNSW graph alongside `vectors`
- `VAdd` inserts into both the map and the graph
- `VSearch` queries the graph instead of scanning the map
- The graph is serialized in `Encode()` for rebalance
- The public API and Op constants do not change

### Files to touch

- `internal/store/vset.go` — new (includes `cosineSim`, vector encode/decode helpers)
- `internal/store/store.go` — `KindVSet`, decoder entry
- `internal/transport/message.go` — Op constants 210–215
- `internal/cluster/exec.go` — 6 exec functions, vector encode/decode helpers
- `internal/cluster/routing.go` — 6 entries in `opRegistry`
- `vset.go` — new (root package)

---

## Suggested order

| # | Type | Complexity | Why this order |
|---|---|---|---|
| 1 | **List** | Low | Teaches the write-with-return fix needed by LPop/RPop — that fix benefits ZSet and Geo too |
| 2 | **ZSet** | Medium | Sorted-slice pattern is reused by Geo; establishes float64 wire encoding |
| 3 | **Geo** | Medium | Builds directly on ZSet's sorted-slice internals; adds self-contained GeoHash math |
| 4 | **VSet** | Medium-High | Most independent; large vector payloads need care in rebalance encoding |

### Cross-cutting prerequisite: write ops that return values

`LPop`, `RPop`, and any future "pop" pattern return data from a write op.
The current `dispatch` in `routing.go` (`ScopeWrite` case) always returns `nil, nil`:

```go
case ScopeWrite:
    // ...
    m.fanOutReplicas(req, nodes[1:])
    return nil, nil   // ← result is discarded
```

Before implementing List, change this to return the primary's result:

```go
case ScopeWrite:
    var result [][]byte
    if len(nodes) == 0 || m.cfg.NodeID == nodes[0] {
        result, err = def.Exec(m, key, args)
        if err != nil { return nil, err }
    } else {
        resp, err := m.sendReq(nodes[0], req)
        if err != nil { return nil, err }
        result = resp.Results
    }
    m.fanOutReplicas(req, nodes[1:])
    return result, nil
```

This is backward-compatible — all existing write ops return `nil` from their exec
function, so `result` will be `nil` and callers that ignore the return value are unaffected.
