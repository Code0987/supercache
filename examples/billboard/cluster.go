package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/pkg/admin"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/membership"
	"github.com/Code0987/supercache/pkg/protect"
	"github.com/Code0987/supercache/pkg/telemetry"
	"github.com/Code0987/supercache/pkg/warmup"
)

// nodeSpec is one SuperCache node in the billboard cluster.
type nodeSpec struct {
	ID        string
	CacheAddr string
	PeerAddr  string
	AdminAddr string
	GossipPort int
	Seeds     []string
}

// runningNode is a live node with handles for shutdown.
type runningNode struct {
	Spec   nodeSpec
	Engine *engine.Engine
	Log    *log.Logger
	src    *ChartSource

	stopAdmin func()
	stopCache func()
	stopPeer  func()
	stopMem   func()
	stopWarm  func()
}

func defaultClusterSpecs() []nodeSpec {
	return []nodeSpec{
		{ID: "billboard-1", CacheAddr: "127.0.0.1:9101", PeerAddr: "127.0.0.1:9201", AdminAddr: "127.0.0.1:8081", GossipPort: 7941},
		{ID: "billboard-2", CacheAddr: "127.0.0.1:9102", PeerAddr: "127.0.0.1:9202", AdminAddr: "127.0.0.1:8082", GossipPort: 7942, Seeds: []string{"127.0.0.1:7941"}},
		{ID: "billboard-3", CacheAddr: "127.0.0.1:9103", PeerAddr: "127.0.0.1:9203", AdminAddr: "127.0.0.1:8083", GossipPort: 7943, Seeds: []string{"127.0.0.1:7941"}},
	}
}

func startNode(spec nodeSpec, shared *ChartSource, logger *log.Logger) (*runningNode, error) {
	nlog := log.New(logger.Writer(), fmt.Sprintf("[%s] ", spec.ID), log.LstdFlags|log.Lmsgprefix)
	nlog.Printf("starting node cache=%s peer=%s admin=%s gossip=:%d seeds=%v",
		spec.CacheAddr, spec.PeerAddr, spec.AdminAddr, spec.GossipPort, spec.Seeds)

	metrics := telemetry.New()
	eng := engine.New(
		engine.WithMetrics(metrics),
		engine.WithGlobalProtect(protect.New(protect.Config{
			RateLimitRPS:     50,
			Burst:            20,
			FailureThreshold: 8,
			OpenTimeout:      5 * time.Second,
		})),
	)
	eng.SetNodeInfo(spec.ID, spec.PeerAddr)

	// charts: LoadThrough from expensive SoT
	chartTTL := 45 * time.Second
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name:            "charts",
		Mode:            keyspace.ModeLoadThrough,
		MaxBytes:        32 << 20,
		TTL:             chartTTL,
		NegativeTTL:     10 * time.Second,
		LoadTimeout:     3 * time.Second,
		PeerTimeout:     500 * time.Millisecond,
		DataSource:      shared,
		WarmKeys:        []string{"chart:global", "chart:genre:pop", "chart:genre:hiphop", "chart:genre:electronic"},
		RefreshInterval: 20 * time.Second,
		RateLimitRPS:    30,
		CircuitBreaker: protect.Config{
			FailureThreshold: 5,
			OpenTimeout:      8 * time.Second,
		},
	}); err != nil {
		eng.Close()
		return nil, fmt.Errorf("keyspace charts: %w", err)
	}
	nlog.Printf("keyspace charts mode=LoadThrough ttl=%s warm=%v refresh=%s",
		chartTTL, []string{"chart:global", "chart:genre:*"}, 20*time.Second)

	// meta: CacheOnly editorial pins / blurbs (no SoT)
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name:     "meta",
		Mode:     keyspace.ModeCacheOnly,
		MaxBytes: 8 << 20,
		TTL:      10 * time.Minute,
	}); err != nil {
		eng.Close()
		return nil, fmt.Errorf("keyspace meta: %w", err)
	}
	nlog.Printf("keyspace meta mode=CacheOnly ttl=10m")

	wm := warmup.NewManager(eng, warmup.Config{Workers: 4, TopN: 32})
	eng.AttachWarmup(wm, wm)
	wm.Start(context.Background())
	nlog.Printf("warmup manager started workers=4 topN=32")

	// Cache gRPC
	cgs, clis, err := cacheserver.ListenAndServe(spec.CacheAddr, eng)
	if err != nil {
		wm.Stop()
		eng.Close()
		return nil, fmt.Errorf("cache listen: %w", err)
	}
	nlog.Printf("cache gRPC listening on %s", clis.Addr())

	// Peer gRPC
	pgs, plis, err := peerserver.ListenAndServe(spec.PeerAddr, eng)
	if err != nil {
		cgs.GracefulStop()
		wm.Stop()
		eng.Close()
		return nil, fmt.Errorf("peer listen: %w", err)
	}
	nlog.Printf("peer gRPC listening on %s", plis.Addr())

	// Membership + cluster
	mem, err := membership.New(membership.Config{
		NodeID:        spec.ID,
		BindAddr:      "127.0.0.1",
		BindPort:      spec.GossipPort,
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: spec.GossipPort,
		PeerGRPCAddr:  spec.PeerAddr,
		Seeds:         spec.Seeds,
		LocalGossip:   true,
		Logger:        nlog,
	})
	if err != nil {
		pgs.GracefulStop()
		cgs.GracefulStop()
		wm.Stop()
		eng.Close()
		return nil, fmt.Errorf("membership: %w", err)
	}
	nlog.Printf("membership joined ring_gen=%d peers=%d", mem.Ring().Generation(), mem.Ring().Len())

	tr := peer.NewTransport(500 * time.Millisecond)
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 16, QueueSize: 2048})
	eng.AttachCluster(&engine.Cluster{
		SelfID:    spec.ID,
		Ring:      mem.Ring(),
		Transport: tr,
		Fanout:    fo,
	})
	nlog.Printf("cluster attached self=%s", spec.ID)

	go func() {
		for ev := range mem.Events() {
			var t engine.EventType
			switch ev.Type {
			case membership.EventJoin:
				t = engine.PeerJoined
			case membership.EventLeave:
				t = engine.PeerLeft
			default:
				t = engine.PeerUpdated
			}
			nlog.Printf("membership event type=%v peer=%s addr=%s", ev.Type, ev.Peer.ID, ev.Peer.Addr)
			select {
			case eng.EventsSink() <- engine.ClusterEvent{
				Type: t,
				Peer: engine.PeerInfo{ID: ev.Peer.ID, Address: ev.Peer.Addr},
			}:
			default:
			}
			eng.NotifyTopologyChange()
			nlog.Printf("topology notify → warmup prefetch ring_gen=%d peers=%d",
				mem.Ring().Generation(), mem.Ring().Len())
		}
	}()
	eng.NotifyTopologyChange()

	// Admin
	adm := admin.New(eng)
	adm.SetReady(true)
	asrv := &http.Server{Addr: spec.AdminAddr, Handler: adm.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		nlog.Printf("admin HTTP http://%s  (/healthz /readyz /peers /keyspaces /metrics)", spec.AdminAddr)
		if err := asrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			nlog.Printf("admin error: %v", err)
		}
	}()

	rn := &runningNode{
		Spec:   spec,
		Engine: eng,
		Log:    nlog,
		src:    shared,
		stopAdmin: func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = asrv.Shutdown(ctx)
		},
		stopCache: func() { cgs.GracefulStop() },
		stopPeer:  func() { pgs.GracefulStop() },
		stopMem: func() {
			fo.Close()
			_ = tr.Close()
			_ = mem.Close()
		},
		stopWarm: func() { wm.Stop() },
	}
	return rn, nil
}

func (n *runningNode) Close() {
	n.Log.Printf("shutting down…")
	n.stopAdmin()
	n.stopWarm()
	n.stopMem()
	n.stopPeer()
	n.stopCache()
	n.Engine.Close()
	n.Log.Printf("stopped")
}
