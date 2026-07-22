// Command supercache-node runs SuperCache with admin HTTP and optional clustering.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"


	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/pkg/admin"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/membership"
	"github.com/Code0987/supercache/pkg/protect"
	"github.com/Code0987/supercache/pkg/telemetry"
	"github.com/Code0987/supercache/pkg/warmup"
)

func main() {
	var (
		adminAddr   = flag.String("admin", "127.0.0.1:8080", "admin HTTP listen address")
		peerAddr    = flag.String("peer", "127.0.0.1:9001", "peer gRPC listen address (mesh, internal)")
		cacheAddr   = flag.String("cache", "127.0.0.1:9000", "cache gRPC listen address (apps)")
		nodeID      = flag.String("node-id", "node-1", "node identity")
		gossipBind  = flag.String("gossip-bind", "0.0.0.0", "gossip bind address")
		gossipPort  = flag.Int("gossip-port", 7946, "gossip bind port")
		gossipAdv   = flag.String("gossip-advertise", "127.0.0.1", "gossip advertise address")
		seeds       = flag.String("seeds", "", "comma-separated gossip seeds host:port")
		gossipSecret = flag.String("gossip-secret", "", "optional gossip shared secret")
		demoKS      = flag.Bool("demo-keyspace", true, "register demo CacheOnly keyspace")
		globalRPS   = flag.Float64("global-rps", 0, "global DataSource rate limit (0=off)")
		cluster     = flag.Bool("cluster", false, "enable gossip membership + peer fan-out")
	)
	flag.Parse()

	metrics := telemetry.New()
	var opts []engine.Option
	opts = append(opts, engine.WithMetrics(metrics))
	if *globalRPS > 0 {
		opts = append(opts, engine.WithGlobalProtect(protect.New(protect.Config{
			RateLimitRPS: *globalRPS,
			Burst:        int(*globalRPS),
		})))
	}
	eng := engine.New(opts...)
	eng.SetNodeInfo(*nodeID, *peerAddr)
	defer eng.Close()

	wm := warmup.NewManager(eng, warmup.Config{Workers: 8, TopN: 64})
	eng.AttachWarmup(wm, wm)
	wm.Start(context.Background())
	defer wm.Stop()

	if *demoKS {
		if err := eng.UpdateKeySpace(keyspace.Config{
			Name:     "demo",
			Mode:     keyspace.ModeCacheOnly,
			MaxBytes: 64 << 20,
			TTL:      5 * time.Minute,
		}); err != nil {
			log.Fatal(err)
		}
	}

	// Application Cache gRPC (clients) — separate from peer mesh.
	cgs, clis, err := cacheserver.ListenAndServe(*cacheAddr, eng)
	if err != nil {
		log.Fatalf("cache listen: %v", err)
	}
	defer cgs.GracefulStop()
	log.Printf("cache gRPC on %s", clis.Addr())

	// Peer gRPC (mesh, internal).
	gs, lis, err := peerserver.ListenAndServe(*peerAddr, eng)
	if err != nil {
		log.Fatalf("peer listen: %v", err)
	}
	defer gs.GracefulStop()
	log.Printf("peer gRPC on %s", lis.Addr())

	var mem *membership.Membership
	var transport *peer.Transport
	var fanout *peer.FanoutPool

	if *cluster {
		var seedList []string
		if *seeds != "" {
			for _, s := range strings.Split(*seeds, ",") {
				s = strings.TrimSpace(s)
				if s != "" {
					seedList = append(seedList, s)
				}
			}
		}
		var secret []byte
		if *gossipSecret != "" {
			secret = []byte(*gossipSecret)
		}
		mem, err = membership.New(membership.Config{
			NodeID:        *nodeID,
			BindAddr:      *gossipBind,
			BindPort:      *gossipPort,
			AdvertiseAddr: *gossipAdv,
			AdvertisePort: *gossipPort,
			PeerGRPCAddr:  *peerAddr,
			Seeds:         seedList,
			GossipSecret:  secret,
		})
		if err != nil {
			log.Fatalf("membership: %v", err)
		}
		defer mem.Close()

		transport = peer.NewTransport(500 * time.Millisecond)
		defer transport.Close()
		fanout = peer.NewFanoutPool(transport, peer.FanoutConfig{Workers: 32, QueueSize: 10_000})
		defer fanout.Close()

		eng.AttachCluster(&engine.Cluster{
			SelfID:    *nodeID,
			Ring:      mem.Ring(),
			Transport: transport,
			Fanout:    fanout,
		})

		// Bridge membership events to engine.Events (best-effort).
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
				select {
				case eng.EventsSink() <- engine.ClusterEvent{
					Type: t,
					Peer: engine.PeerInfo{ID: ev.Peer.ID, Address: ev.Peer.Addr},
				}:
				default:
				}
				eng.NotifyTopologyChange()
			}
		}()
		// Initial warmup after join
		eng.NotifyTopologyChange()
		log.Printf("cluster enabled seeds=%v gossip=:%d", seedList, *gossipPort)
	}

	adm := admin.New(eng)
	adm.SetReady(true)
	srv := &http.Server{
		Addr:              *adminAddr,
		Handler:           adm.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("supercache-node %s admin http://%s", *nodeID, *adminAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	fmt.Println("bye")
}

