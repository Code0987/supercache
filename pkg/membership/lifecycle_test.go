package membership

import (
	"log"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/Code0987/supercache/internal/ring"
)

// freePort returns an unused TCP port (New maps BindPort 0 → 7946, so we cannot pass 0).
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func TestNewValidationAndLifecycle(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("NodeID required")
	}
	if _, err := New(Config{NodeID: "n1"}); err == nil {
		t.Fatal("PeerGRPCAddr required")
	}

	port := freePort(t)
	m, err := New(Config{
		NodeID:        "a",
		PeerGRPCAddr:  "127.0.0.1:19001",
		BindAddr:      "127.0.0.1",
		AdvertiseAddr: "127.0.0.1",
		BindPort:      port,
		LocalGossip:   true,
		Logger:        log.New(log.Writer(), "ml-a ", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if m.Ring() == nil || m.Ring().Len() < 1 {
		t.Fatalf("ring len=%d", m.Ring().Len())
	}
	if m.Self().ID != "a" {
		t.Fatal(m.Self())
	}
	_ = m.LocalAddr()
	_ = m.Events()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTwoNodeJoin(t *testing.T) {
	pa, pb := freePort(t), freePort(t)
	a, err := New(Config{
		NodeID: "a", PeerGRPCAddr: "127.0.0.1:19101",
		BindAddr: "127.0.0.1", AdvertiseAddr: "127.0.0.1", BindPort: pa, LocalGossip: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	b, err := New(Config{
		NodeID: "b", PeerGRPCAddr: "127.0.0.1:19102",
		BindAddr: "127.0.0.1", AdvertiseAddr: "127.0.0.1", BindPort: pb, LocalGossip: true,
		Seeds: []string{a.LocalAddr()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if a.Ring().Len() >= 2 && b.Ring().Len() >= 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Join is best-effort; at least each has self.
	if a.Ring().Len() < 1 || b.Ring().Len() < 1 {
		t.Fatalf("rings a=%d b=%d", a.Ring().Len(), b.Ring().Len())
	}
}

func TestMetaHelpersEmitAndEventDelegate(t *testing.T) {
	if isLoopbackHost("") || isLoopbackHost("10.0.0.1") {
		t.Fatal("non-loopback")
	}
	if !isLoopbackHost("localhost") || !isLoopbackHost("127.0.0.1:7946") {
		t.Fatal("loopback")
	}
	if !preferLocalGossip(Config{LocalGossip: true, AdvertiseAddr: "10.0.0.1"}) {
		t.Fatal("LocalGossip force")
	}
	if looksLikeHostPort("") || looksLikeHostPort("{x") || !looksLikeHostPort("127.0.0.1:9") {
		t.Fatal("looksLikeHostPort")
	}
	if looksLikeHostPort(string(make([]byte, 300))) {
		t.Fatal("too long")
	}

	p := peerFromNode(&memberlist.Node{Name: "x"})
	if p.ID != "x" || p.Addr != "" {
		t.Fatalf("%+v", p)
	}
	p = peerFromNode(&memberlist.Node{Name: "x", Meta: []byte("1.2.3.4:9")})
	if p.Addr != "1.2.3.4:9" {
		t.Fatal(p.Addr)
	}

	m := &Membership{
		cfg:    Config{NodeID: "n", PeerGRPCAddr: "127.0.0.1:1"},
		ring:   ring.New(8),
		events: make(chan Event, 1),
	}
	m.emit(EventJoin, ring.Peer{ID: "p1"})
	m.emit(EventLeave, ring.Peer{ID: "p2"})
	m.emit(EventUpdate, ring.Peer{ID: "p3"})
	// rebuild with nil list → self only
	m.rebuildRing()
	if m.ring.Len() != 1 {
		t.Fatalf("self-only ring %d", m.ring.Len())
	}

	d := &delegate{m: &Membership{cfg: Config{PeerGRPCAddr: "host.example.com:12345"}}}
	if meta := d.NodeMeta(2); meta != nil {
		t.Fatalf("want nil, got %q", meta)
	}
	d.NotifyMsg(nil)
	_ = d.GetBroadcasts(0, 0)
	_ = d.LocalState(true)
	d.MergeRemoteState(nil, false)

	m2, err := New(Config{
		NodeID: "e", PeerGRPCAddr: "127.0.0.1:19201",
		BindAddr: "127.0.0.1", AdvertiseAddr: "127.0.0.1", BindPort: freePort(t), LocalGossip: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	ed := &eventDelegate{m: m2}
	ed.NotifyJoin(&memberlist.Node{Name: "z", Meta: []byte("127.0.0.1:1")})
	ed.NotifyLeave(&memberlist.Node{Name: "z"})
	ed.NotifyUpdate(&memberlist.Node{Name: "z", Meta: []byte("127.0.0.1:1")})
	time.Sleep(80 * time.Millisecond)
}

func TestBaseMemberlistConfigBindPort(t *testing.T) {
	mlc := baseMemberlistConfig(Config{
		NodeID: "x", PeerGRPCAddr: "127.0.0.1:1",
		BindAddr: "127.0.0.1", BindPort: 17946,
	})
	if mlc.BindPort != 17946 {
		t.Fatal(mlc.BindPort)
	}
}
