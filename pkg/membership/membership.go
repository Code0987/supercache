package membership

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/Code0987/supercache/internal/ring"
)

// Config for gossip membership.
type Config struct {
	NodeID        string
	BindAddr      string // host for gossip bind
	BindPort      int
	AdvertiseAddr string
	AdvertisePort int
	PeerGRPCAddr  string // host:port for peer RPCs (meta)
	Seeds         []string
	GossipSecret  []byte // optional symmetric key
	Logger        *log.Logger
	// LocalGossip forces memberlist.DefaultLocalConfig (loopback-friendly).
	// When false (default), loopback bind/advertise still auto-selects Local;
	// non-loopback addresses use DefaultLANConfig for production multi-host.
	LocalGossip bool
}

// EventType for membership changes.
type EventType int

const (
	EventJoin EventType = iota
	EventLeave
	EventUpdate
)

// Event is a membership change notification.
type Event struct {
	Type EventType
	Peer ring.Peer
}

// Membership manages memberlist and a consistent-hash ring.
type Membership struct {
	cfg    Config
	list   *memberlist.Memberlist
	ring   *ring.Ring
	events chan Event

	mu     sync.RWMutex
	closed bool
}

type metaPayload struct {
	PeerGRPC string `json:"peer_grpc"`
}

// New starts memberlist and joins seeds.
func New(cfg Config) (*Membership, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("membership: NodeID required")
	}
	if cfg.PeerGRPCAddr == "" {
		return nil, fmt.Errorf("membership: PeerGRPCAddr required")
	}
	if cfg.BindPort == 0 {
		cfg.BindPort = 7946
	}
	if cfg.AdvertisePort == 0 {
		cfg.AdvertisePort = cfg.BindPort
	}
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = cfg.BindAddr
	}
	if cfg.BindAddr == "" {
		cfg.BindAddr = "0.0.0.0"
	}

	m := &Membership{
		cfg:    cfg,
		ring:   ring.New(64),
		events: make(chan Event, 64),
	}

	mlc := baseMemberlistConfig(cfg)
	mlc.Delegate = &delegate{m: m}
	mlc.Events = &eventDelegate{m: m}
	if len(cfg.GossipSecret) > 0 {
		mlc.SecretKey = cfg.GossipSecret
	}
	if cfg.Logger != nil {
		mlc.Logger = cfg.Logger
	} else {
		mlc.LogOutput = io.Discard
	}

	list, err := memberlist.Create(mlc)
	if err != nil {
		return nil, fmt.Errorf("membership create: %w", err)
	}
	m.mu.Lock()
	m.list = list
	m.mu.Unlock()

	if len(cfg.Seeds) > 0 {
		if _, err := list.Join(cfg.Seeds); err != nil {
			_ = list.Shutdown()
			return nil, fmt.Errorf("membership join: %w", err)
		}
	}
	m.rebuildRing()
	return m, nil
}

// preferLocalGossip chooses DefaultLocalConfig for loopback demos/tests.
// Multi-host (non-loopback advertise/bind) uses DefaultLANConfig.
func preferLocalGossip(cfg Config) bool {
	if cfg.LocalGossip {
		return true
	}
	return isLoopbackHost(cfg.AdvertiseAddr) || isLoopbackHost(cfg.BindAddr)
}

func isLoopbackHost(host string) bool {
	if host == "" {
		return false
	}
	// Strip port if present.
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// baseMemberlistConfig builds the memberlist Config (timings + identity/bind).
// Exported-to-test via same package.
func baseMemberlistConfig(cfg Config) *memberlist.Config {
	var mlc *memberlist.Config
	if preferLocalGossip(cfg) {
		// Reliable on 127.0.0.1; LANConfig often fails to join loopback quickly.
		mlc = memberlist.DefaultLocalConfig()
	} else {
		mlc = memberlist.DefaultLANConfig()
	}
	mlc.Name = cfg.NodeID
	mlc.BindAddr = cfg.BindAddr
	mlc.BindPort = cfg.BindPort
	mlc.AdvertiseAddr = cfg.AdvertiseAddr
	mlc.AdvertisePort = cfg.AdvertisePort
	return mlc
}

// Ring returns the consistent hash ring (shared, concurrency-safe).
func (m *Membership) Ring() *ring.Ring { return m.ring }

// Events returns membership events (drop-oldest on slow consumer via non-blocking send).
func (m *Membership) Events() <-chan Event { return m.events }

// LocalAddr returns gossip bind host:port.
func (m *Membership) LocalAddr() string {
	return net.JoinHostPort(m.cfg.AdvertiseAddr, strconv.Itoa(m.cfg.AdvertisePort))
}

// Self returns this node as a ring peer.
func (m *Membership) Self() ring.Peer {
	return ring.Peer{ID: m.cfg.NodeID, Addr: m.cfg.PeerGRPCAddr}
}

// Close leaves the cluster.
func (m *Membership) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	if m.list != nil {
		_ = m.list.Leave(time.Second)
		return m.list.Shutdown()
	}
	return nil
}

func (m *Membership) rebuildRing() {
	// NotifyJoin can fire during memberlist.Create before m.list is assigned.
	m.mu.Lock()
	list := m.list
	m.mu.Unlock()
	if list == nil {
		m.ring.SetPeers([]ring.Peer{m.Self()})
		return
	}

	members := list.Members()
	peers := make([]ring.Peer, 0, len(members))
	for _, mem := range members {
		p := peerFromNode(mem)
		if p.ID == "" || p.Addr == "" {
			continue
		}
		peers = append(peers, p)
	}
	// Ensure self is present even if meta lag.
	self := m.Self()
	found := false
	for _, p := range peers {
		if p.ID == self.ID {
			found = true
			break
		}
	}
	if !found {
		peers = append(peers, self)
	}
	m.ring.SetPeers(peers)
}

func (m *Membership) emit(t EventType, p ring.Peer) {
	ev := Event{Type: t, Peer: p}
	select {
	case m.events <- ev:
	default:
		// drop oldest
		select {
		case <-m.events:
		default:
		}
		select {
		case m.events <- ev:
		default:
		}
	}
}

func peerFromNode(n *memberlist.Node) ring.Peer {
	p := ring.Peer{ID: n.Name}
	if len(n.Meta) == 0 {
		return p
	}
	var meta metaPayload
	if json.Unmarshal(n.Meta, &meta) == nil && meta.PeerGRPC != "" {
		p.Addr = meta.PeerGRPC
		return p
	}
	// Compact form: raw host:port (used when JSON does not fit Meta limit).
	if s := string(n.Meta); looksLikeHostPort(s) {
		p.Addr = s
	}
	return p
}

func looksLikeHostPort(s string) bool {
	if s == "" || len(s) > 253 {
		return false
	}
	// Reject obvious truncated JSON.
	if s[0] == '{' || s[0] == '[' {
		return false
	}
	_, _, err := net.SplitHostPort(s)
	return err == nil
}

type delegate struct{ m *Membership }

// NodeMeta returns peer gRPC address metadata for memberlist.
// Never returns truncated JSON (that would parse as empty Addr and drop the peer
// from the ring). Falls back to a compact host:port encoding when JSON exceeds limit.
func (d *delegate) NodeMeta(limit int) []byte {
	addr := d.m.cfg.PeerGRPCAddr
	b, err := json.Marshal(metaPayload{PeerGRPC: addr})
	if err == nil && len(b) <= limit {
		return b
	}
	// Compact fallback.
	raw := []byte(addr)
	if len(raw) <= limit {
		return raw
	}
	// Cannot fit a usable address — omit meta rather than send garbage.
	return nil
}
func (d *delegate) NotifyMsg([]byte)                           {}
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *delegate) LocalState(join bool) []byte                { return nil }
func (d *delegate) MergeRemoteState(buf []byte, join bool)     {}

type eventDelegate struct{ m *Membership }

// Event callbacks run under memberlist locks — never call Members()/Join here
// synchronously (deadlock). Rebuild the ring asynchronously.
func (e *eventDelegate) NotifyJoin(n *memberlist.Node) {
	p := peerFromNode(n)
	go func() {
		e.m.rebuildRing()
		e.m.emit(EventJoin, p)
	}()
}
func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	p := peerFromNode(n)
	go func() {
		e.m.rebuildRing()
		e.m.emit(EventLeave, p)
	}()
}
func (e *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	p := peerFromNode(n)
	go func() {
		e.m.rebuildRing()
		e.m.emit(EventUpdate, p)
	}()
}
