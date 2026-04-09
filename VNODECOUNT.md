Summary
Remove the static VNodes config field and derive each node's virtual node count from its declared MemLimit, so nodes with more memory naturally own a wider slice of the hash ring.

Background
Today all nodes get the same number of vNodes regardless of available resources, giving every node an equal share of the keyspace. A node with 4× the memory should receive proportionally more keys. Weighted consistent hashing achieves this by assigning more virtual nodes to higher-capacity peers.

Depends on: Ticket 2 (MemLimit)

Proposed design

vNode calculation

Introduce an internal constant vNodesPerUnit (e.g. 100 vNodes per 256 MB). Each node computes its own count at startup:

vNodes = max(minVNodes, MemLimit / unitSize * vNodesPerUnit)

Nodes with no MemLimit set use a default (e.g. 100), preserving current behavior.

Gossiping vNode count

Add VNodes int to transport.PeerState so each node advertises its count. Update buildHeartbeatRequest to include the local count, and mergeState to store it on the peer.

Ring changes

Update ring.Add to accept a count int parameter instead of reading r.vNodeCount. The ring no longer has a single global count; each node is added with its own. Update all call sites in cluster.go (NewManager, addPeer, mergeState recovery path) to pass the appropriate count.

Config changes

Remove VNodes from hive.Config and cluster.Config. Add MemLimit (from Ticket 1) — that is the only input needed.

Acceptance criteria

    A node with twice the MemLimit of another receives approximately twice the keyspace
    Nodes without MemLimit set behave as they do today (fixed default vNode count)
    VNodes is no longer a user-facing config field
    Rebalance triggers correctly when a new weighted node joins
