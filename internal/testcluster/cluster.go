// Package testcluster starts an in-process SuperCache mesh without gossip.
// Listen order: cache :0 + peer :0 → SetNodeInfo → ring.SetPeers → AttachCluster.
package testcluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/telemetry"
)

// Config starts N engines with Cache + Peer gRPC on 127.0.0.1:0.
type Config struct {
	Nodes          int
	Keyspaces      []keyspace.Config
	Fanout         peer.FanoutConfig
	VNodes         int
	MaxVersionKeys int
}

// Node is one in-process cache node.
type Node struct {
	ID        string
	Engine    *engine.Engine
	CacheAddr string
	PeerAddr  string
}

// FanoutCounters sums transport queue and hint counters owned by the cluster.
type FanoutCounters struct {
	Errors       uint64
	Dropped      uint64
	HintsFlushed uint64
	HintsDropped uint64
}

type runtimeNode struct {
	Node
	cacheSrv *grpc.Server
	peerSrv  *grpc.Server
	tr       *peer.Transport
	fo       *peer.FanoutPool
}

// Cluster is a process-local mesh.
type Cluster struct {
	rt []runtimeNode
}

// CacheOnlyBench is the default CacheOnly keyspace for embed benches.
func CacheOnlyBench() keyspace.Config {
	return keyspace.Config{
		Name: "bench", Mode: keyspace.ModeCacheOnly,
		MaxBytes: 64 << 20, TTL: time.Hour,
	}
}

// LoadThroughBench is LoadThrough with MaxBytes=1MiB so lastVer prune stays cheap.
func LoadThroughBench(src datasource.DataSource) keyspace.Config {
	return keyspace.Config{
		Name: "benchlt", Mode: keyspace.ModeLoadThrough,
		MaxBytes: 1 << 20, TTL: time.Hour, DataSource: src,
	}
}

// Start listens, builds a shared ring, and attaches peer transport.
// Nodes must be 1, 3, or 10.
func Start(cfg Config) (*Cluster, error) {
	if cfg.Nodes != 1 && cfg.Nodes != 3 && cfg.Nodes != 10 {
		return nil, fmt.Errorf("testcluster: Nodes must be 1, 3, or 10, got %d", cfg.Nodes)
	}
	if cfg.VNodes <= 0 {
		cfg.VNodes = 32
	}
	if cfg.MaxVersionKeys <= 0 {
		cfg.MaxVersionKeys = 65536
	}
	keyspaces := cfg.Keyspaces
	if len(keyspaces) == 0 {
		keyspaces = []keyspace.Config{{
			Name: "bench", Mode: keyspace.ModeCacheOnly,
			MaxBytes: 64 << 20, TTL: time.Hour,
		}}
	}

	c := &Cluster{rt: make([]runtimeNode, cfg.Nodes)}
	for i := 0; i < cfg.Nodes; i++ {
		id := fmt.Sprintf("n%d", i)
		eng := engine.New(engine.WithMaxVersionKeys(cfg.MaxVersionKeys))
		for _, ks := range keyspaces {
			if err := eng.UpdateKeySpace(ks); err != nil {
				c.Close()
				return nil, fmt.Errorf("testcluster: keyspace %s: %w", ks.Name, err)
			}
		}
		cgs, clis, err := listenCache(eng)
		if err != nil {
			eng.Close()
			c.Close()
			return nil, fmt.Errorf("testcluster: cache listen %s: %w", id, err)
		}
		pgs, plis, err := listenPeer(eng)
		if err != nil {
			cgs.Stop()
			eng.Close()
			c.Close()
			return nil, fmt.Errorf("testcluster: peer listen %s: %w", id, err)
		}
		peerAddr := plis.Addr().String()
		eng.SetNodeInfo(id, peerAddr)
		c.rt[i] = runtimeNode{
			Node: Node{
				ID:        id,
				Engine:    eng,
				CacheAddr: clis.Addr().String(),
				PeerAddr:  peerAddr,
			},
			cacheSrv: cgs,
			peerSrv:  pgs,
		}
	}

	peers := make([]ring.Peer, 0, cfg.Nodes)
	for _, n := range c.rt {
		peers = append(peers, ring.Peer{ID: n.ID, Addr: n.PeerAddr})
	}
	for i := range c.rt {
		rn := &c.rt[i]
		r := ring.New(cfg.VNodes)
		r.SetPeers(peers)
		tr := peer.NewTransport(time.Second)
		fo := peer.NewFanoutPool(tr, cfg.Fanout)
		rn.tr = tr
		rn.fo = fo
		rn.Engine.AttachCluster(&engine.Cluster{
			SelfID:    rn.ID,
			Ring:      r,
			Transport: tr,
			Fanout:    fo,
		})
	}

	if err := c.ready(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func listenCache(eng *engine.Engine) (*grpc.Server, net.Listener, error) {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		gs, lis, err := cacheserver.ListenAndServe("127.0.0.1:0", eng)
		if err == nil {
			return gs, lis, nil
		}
		last = err
	}
	return nil, nil, last
}

func listenPeer(eng *engine.Engine) (*grpc.Server, net.Listener, error) {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		gs, lis, err := peerserver.ListenAndServe("127.0.0.1:0", eng)
		if err == nil {
			return gs, lis, nil
		}
		last = err
	}
	return nil, nil, last
}

func (c *Cluster) ready() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, n := range c.rt {
		cli, err := client.Dial(ctx, n.CacheAddr)
		if err != nil {
			return fmt.Errorf("testcluster: dial %s: %w", n.CacheAddr, err)
		}
		ks := firstKS(n.Engine)
		_, err = cli.Get(ctx, ks, "__testcluster_ready__")
		if err == nil || errors.Is(err, client.ErrNotFound) {
			_ = cli.Close()
			continue
		}
		_, berr := cli.BloomTest(ctx, ks, "__testcluster_ready__", []byte("x"))
		if berr == nil {
			_ = cli.Close()
			continue
		}
		_, serr := cli.SetContains(ctx, ks, "__testcluster_ready__", []byte("x"))
		if serr == nil {
			_ = cli.Close()
			continue
		}
		_, _, zerr := cli.ZScore(ctx, ks, "__testcluster_ready__", []byte("x"))
		_ = cli.Close()
		if zerr == nil {
			continue
		}
		return fmt.Errorf("testcluster: ready get %s: %w", n.ID, err)
	}
	return nil
}

func firstKS(e *engine.Engine) string {
	snaps := e.KeySpaceSnapshots()
	if len(snaps) == 0 {
		return "bench"
	}
	return snaps[0].Name
}

// Close stops fan-out, transports, gRPC servers, then engines.
func (c *Cluster) Close() {
	if c == nil {
		return
	}
	for i := range c.rt {
		if c.rt[i].fo != nil {
			c.rt[i].fo.Close()
			c.rt[i].fo = nil
		}
	}
	for i := range c.rt {
		if c.rt[i].tr != nil {
			_ = c.rt[i].tr.Close()
			c.rt[i].tr = nil
		}
	}
	for i := range c.rt {
		if c.rt[i].cacheSrv != nil {
			c.rt[i].cacheSrv.Stop()
			c.rt[i].cacheSrv = nil
		}
		if c.rt[i].peerSrv != nil {
			c.rt[i].peerSrv.Stop()
			c.rt[i].peerSrv = nil
		}
	}
	for i := range c.rt {
		if c.rt[i].Engine != nil {
			c.rt[i].Engine.Close()
			c.rt[i].Engine = nil
		}
	}
	c.rt = nil
}

// Nodes returns public node views.
func (c *Cluster) Nodes() []Node {
	if c == nil {
		return nil
	}
	out := make([]Node, len(c.rt))
	for i := range c.rt {
		out[i] = c.rt[i].Node
	}
	return out
}

// CacheAddrs returns cache gRPC addresses in node order.
func (c *Cluster) CacheAddrs() []string {
	if c == nil {
		return nil
	}
	out := make([]string, len(c.rt))
	for i := range c.rt {
		out[i] = c.rt[i].CacheAddr
	}
	return out
}

// PrefillAll writes the same versioned entry to every node's store (bypasses fan-out).
func (c *Cluster) PrefillAll(_ context.Context, ks, prefix string, n int, value []byte) error {
	if c == nil || n < 0 {
		return fmt.Errorf("testcluster: prefill: invalid cluster or n")
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		ent := store.Entry{Value: append([]byte(nil), value...), Version: 1}
		for _, rn := range c.rt {
			if rn.Engine == nil {
				return fmt.Errorf("testcluster: prefill: engine %s closed", rn.ID)
			}
			ok, err := rn.Engine.ApplyPut(ks, key, ent)
			if err != nil {
				return fmt.Errorf("testcluster: ApplyPut %s %s: %w", rn.ID, key, err)
			}
			if !ok {
				return fmt.Errorf("testcluster: ApplyPut %s %s rejected", rn.ID, key)
			}
		}
	}
	return nil
}

// VerifyLocalHits checks Engine.Get on each node. sample<=0 checks all n keys.
func (c *Cluster) VerifyLocalHits(ctx context.Context, ks, prefix string, n, sample int) error {
	if sample <= 0 || sample > n {
		sample = n
	}
	step := 1
	if sample < n {
		step = n / sample
		if step < 1 {
			step = 1
		}
	}
	for _, rn := range c.rt {
		checked := 0
		for i := 0; i < n && checked < sample; i += step {
			key := fmt.Sprintf("%s%d", prefix, i)
			_, err := rn.Engine.Get(ctx, ks, key)
			if err != nil {
				return fmt.Errorf("testcluster: verify %s %s: %w", rn.ID, key, err)
			}
			checked++
		}
	}
	return nil
}

// FanoutStats sums transport queue stats and hint counters.
func (c *Cluster) FanoutStats() FanoutCounters {
	var out FanoutCounters
	if c == nil {
		return out
	}
	for _, rn := range c.rt {
		if rn.Engine != nil {
			e, d := rn.Engine.FanoutStats()
			out.Errors += e
			out.Dropped += d
		}
		if rn.fo != nil {
			out.HintsFlushed += rn.fo.HintsFlushed.Load()
			out.HintsDropped += rn.fo.HintsDropped.Load()
		}
	}
	return out
}

// Metrics returns a telemetry snapshot per node.
func (c *Cluster) Metrics() []telemetry.Snapshot {
	if c == nil {
		return nil
	}
	out := make([]telemetry.Snapshot, len(c.rt))
	for i, rn := range c.rt {
		if rn.Engine != nil {
			out[i] = rn.Engine.Metrics()
		}
	}
	return out
}
