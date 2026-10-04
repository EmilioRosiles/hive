package cluster

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// faultNet puts a TCP proxy in front of every node it starts, so tests can cut
// or stall links between nodes. A proxy learns who dialed from the
// connection's first frame, MsgHello.
type faultNet struct {
	mu      sync.Mutex
	cuts    map[[2]string]bool
	stalled map[string]bool
	links   map[*faultLink]bool
}

// faultLink is one proxied connection from node `from` to node `to`.
type faultLink struct {
	from, to string
	a, b     net.Conn
}

func newFaultNet() *faultNet {
	return &faultNet{cuts: map[[2]string]bool{}, stalled: map[string]bool{}, links: map[*faultLink]bool{}}
}

func pair(x, y string) [2]string {
	if x > y {
		x, y = y, x
	}
	return [2]string{x, y}
}

// node starts an RF=1 clustered node behind a proxy and advertises the proxy's address.
func (f *faultNet) node(t *testing.T, seeds []string) *Cluster {
	t.Helper()
	cfg := clusteredTestConfig(t, seeds, 1, 1)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go f.serve(ln, cfg.NodeID, fmt.Sprintf("127.0.0.1:%d", cfg.BindPort))
	cfg.AdvertiseAddr = ln.Addr().String()
	m, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(func() { m.Shutdown() })
	return m
}

// serve accepts connections for node `to` until ln closes.
func (f *faultNet) serve(ln net.Listener, to, backend string) {
	for {
		a, err := ln.Accept()
		if err != nil {
			return
		}
		go f.forward(a, to, backend)
	}
}

// forward relays one connection to backend unless its link is cut.
func (f *faultNet) forward(a net.Conn, to, backend string) {
	hello, err := transport.ReadFrame(a)
	if err != nil || hello.Type != transport.MsgHello {
		a.Close()
		return
	}
	b, err := net.Dial("tcp", backend)
	if err != nil {
		a.Close()
		return
	}
	l := &faultLink{from: string(hello.Payload), to: to, a: a, b: b}
	f.mu.Lock()
	if f.cuts[pair(l.from, l.to)] {
		f.mu.Unlock()
		a.Close()
		b.Close()
		return
	}
	f.links[l] = true
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.links, l)
		f.mu.Unlock()
	}()

	if transport.WriteFrame(b, hello) != nil {
		a.Close()
		b.Close()
		return
	}
	go f.pipe(l, a, b)
	f.pipe(l, b, a)
}

// pipe copies src to dst, holding bytes while either end of l is stalled.
func (f *faultNet) pipe(l *faultLink, src, dst net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		for f.isStalled(l) {
			time.Sleep(time.Millisecond)
		}
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	l.a.Close()
	l.b.Close()
}

func (f *faultNet) isStalled(l *faultLink) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stalled[l.from] || f.stalled[l.to]
}

// cut drops every connection between x and y and refuses new ones.
func (f *faultNet) cut(x, y *Cluster) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cuts[pair(x.cfg.NodeID, y.cfg.NodeID)] = true
	for l := range f.links {
		if pair(l.from, l.to) == pair(x.cfg.NodeID, y.cfg.NodeID) {
			l.a.Close()
			l.b.Close()
		}
	}
}

// pause holds every byte to and from n for d, like a stalled process.
func (f *faultNet) pause(n *Cluster, d time.Duration) {
	f.mu.Lock()
	f.stalled[n.cfg.NodeID] = true
	f.mu.Unlock()
	time.AfterFunc(d, func() {
		f.mu.Lock()
		delete(f.stalled, n.cfg.NodeID)
		f.mu.Unlock()
	})
}

// formFaultCluster starts three nodes on a faultNet and waits until each knows the others.
func formFaultCluster(t *testing.T) (*faultNet, []*Cluster) {
	t.Helper()
	f := newFaultNet()
	n1 := f.node(t, nil)
	n2 := f.node(t, []string{clusteredAddr(n1)})
	n3 := f.node(t, []string{clusteredAddr(n1)})
	nodes := []*Cluster{n1, n2, n3}
	waitForCond(t, 3*time.Second, "cluster formed", func() bool {
		for _, n := range nodes {
			if len(n.Peers()) != 2 {
				return false
			}
		}
		return true
	})
	return f, nodes
}

// watch samples observers for d and returns every "observer -> peer" pair seen
// Suspect and every pair seen Dead.
func watch(observers []*Cluster, d time.Duration) (suspected, dead []string) {
	seen := map[string]bool{}
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		for _, o := range observers {
			for _, p := range o.Peers() {
				key := fmt.Sprintf("%d %s -> %s", p.Status, o.cfg.NodeID, p.NodeID)
				if seen[key] {
					continue
				}
				seen[key] = true
				switch p.Status {
				case NodeSuspect:
					suspected = append(suspected, key)
				case NodeDead:
					dead = append(dead, key)
				}
			}
		}
	}
	return suspected, dead
}

// TestFault_NonTransitivePartition_NoDeathOrRingChange cuts n1 from n3 while
// both still reach n2, which vouches for each through indirect probes.
func TestFault_NonTransitivePartition_NoDeathOrRingChange(t *testing.T) {
	f, nodes := formFaultCluster(t)
	n1, n3 := nodes[0], nodes[2]
	versions := make([]uint64, len(nodes))
	for i, n := range nodes {
		versions[i] = n.ring.GetVersion()
	}

	f.cut(n1, n3)

	suspected, dead := watch(nodes, 3*time.Second)
	if len(suspected) == 0 {
		t.Error("nobody suspected across the cut link: the partition had no effect")
	}
	if len(dead) > 0 {
		t.Errorf("peers marked Dead during a non-transitive partition: %v", dead)
	}
	for i, n := range nodes {
		if n.ring.GetVersion() != versions[i] {
			t.Errorf("%s: ring changed during a non-transitive partition", n.cfg.NodeID)
		}
	}
}

// TestFault_IsolatedNode_DeclaredDead cuts n3 off from everyone.
func TestFault_IsolatedNode_DeclaredDead(t *testing.T) {
	f, nodes := formFaultCluster(t)
	n1, n2, n3 := nodes[0], nodes[1], nodes[2]

	f.cut(n1, n3)
	f.cut(n2, n3)

	waitForCond(t, 3*time.Second, "both survivors mark n3 Dead", func() bool {
		s1, _ := n1.peerStatus(n3.cfg.NodeID)
		s2, _ := n2.peerStatus(n3.cfg.NodeID)
		return s1 == NodeDead && s2 == NodeDead
	})
}

// TestFault_IsolatedProber_KillsNobody cuts n1 off from everyone: its probes
// get no answer from any helper, so it must keep its peers Suspect, not Dead.
func TestFault_IsolatedProber_KillsNobody(t *testing.T) {
	f, nodes := formFaultCluster(t)
	n1, n2, n3 := nodes[0], nodes[1], nodes[2]

	f.cut(n1, n2)
	f.cut(n1, n3)

	suspected, dead := watch([]*Cluster{n1}, 3*time.Second)
	if len(suspected) == 0 {
		t.Error("the isolated node suspected nobody: the cut had no effect")
	}
	if len(dead) > 0 {
		t.Errorf("an isolated node marked its peers Dead: %v", dead)
	}
}

// TestFault_ShortPause_SuspectedNotKilled stalls n3 longer than a heartbeat
// timeout but shorter than a probe, so it is suspected and then cleared.
func TestFault_ShortPause_SuspectedNotKilled(t *testing.T) {
	f, nodes := formFaultCluster(t)
	n3 := nodes[2]

	f.pause(n3, 400*time.Millisecond)

	suspected, dead := watch(nodes, 2*time.Second)
	if len(suspected) == 0 {
		t.Error("nobody suspected n3 during the pause: it was shorter than a heartbeat timeout")
	}
	if len(dead) > 0 {
		t.Errorf("peers marked Dead after a short pause: %v", dead)
	}
}
