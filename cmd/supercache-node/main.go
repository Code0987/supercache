// Command supercache-node runs SuperCache with admin HTTP and optional clustering.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/pkg/admin"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/membership"
	"github.com/Code0987/supercache/pkg/protect"
	"github.com/Code0987/supercache/pkg/telemetry"
	"github.com/Code0987/supercache/pkg/tlsconfig"
	"github.com/Code0987/supercache/pkg/warmup"
)

// version is set at link time for releases: -ldflags "-X main.version=vX.Y.Z"
var version = "dev"

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
		showVersion = flag.Bool("version", false, "print version and exit")

		// TLS (optional). Empty paths keep plaintext for local/dev.
		tlsCert     = flag.String("tls-cert", "", "PEM certificate for Cache and Peer gRPC servers")
		tlsKey      = flag.String("tls-key", "", "PEM private key for Cache and Peer gRPC servers")
		tlsClientCA = flag.String("tls-client-ca", "", "PEM CA for verifying clients (Peer mTLS when set with -peer-mtls)")
		peerMTLS    = flag.Bool("peer-mtls", false, "require client certs on Peer port (needs -tls-client-ca)")
		// Peer dial identity (mTLS outbound). Defaults to server cert/key when peer-mtls is on.
		peerClientCert = flag.String("peer-client-cert", "", "PEM client cert for outbound peer RPCs (default: -tls-cert)")
		peerClientKey  = flag.String("peer-client-key", "", "PEM client key for outbound peer RPCs (default: -tls-key)")
		peerServerName = flag.String("peer-server-name", "", "TLS ServerName for peer dials (optional; else host from peer addr)")
		// Cache-only client CA (optional separate from peer). If empty, Cache uses no client auth.
		cacheClientCA = flag.String("cache-client-ca", "", "PEM CA for optional Cache client cert verification")
		cacheMTLS     = flag.Bool("cache-mtls", false, "require client certs on Cache port (needs -cache-client-ca or -tls-client-ca)")
	)
	flag.Parse()
	if *showVersion {
		fmt.Printf("supercache-node %s\n", version)
		return
	}

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
		// ModeSet for exact membership demos (feature tags, allow-lists, …).
		if err := eng.UpdateKeySpace(keyspace.Config{
			Name:     "tags",
			Mode:     keyspace.ModeSet,
			MaxBytes: 16 << 20,
			TTL:      30 * time.Minute,
		}); err != nil {
			log.Fatal(err)
		}
		// ModeZSet for scored rankings (leaderboards, time-ordered feeds, …).
		if err := eng.UpdateKeySpace(keyspace.Config{
			Name:     "board",
			Mode:     keyspace.ModeZSet,
			MaxBytes: 16 << 20,
			TTL:      30 * time.Minute,
		}); err != nil {
			log.Fatal(err)
		}
		log.Printf("demo keyspaces: demo=CacheOnly tags=ModeSet board=ModeZSet")
	}

	cacheSrvOpts, peerSrvOpts, peerDialTLS, err := buildTLS(
		*tlsCert, *tlsKey, *tlsClientCA, *peerMTLS,
		*cacheClientCA, *cacheMTLS,
		*peerClientCert, *peerClientKey, *peerServerName,
	)
	if err != nil {
		log.Fatalf("tls: %v", err)
	}

	// Application Cache gRPC (clients) — separate from peer mesh.
	cgs, clis, err := cacheserver.ListenAndServe(*cacheAddr, eng, cacheSrvOpts...)
	if err != nil {
		log.Fatalf("cache listen: %v", err)
	}
	defer cgs.GracefulStop()
	log.Printf("cache gRPC on %s tls=%v", clis.Addr(), len(cacheSrvOpts) > 0)

	// Peer gRPC (mesh, internal).
	gs, lis, err := peerserver.ListenAndServe(*peerAddr, eng, peerSrvOpts...)
	if err != nil {
		log.Fatalf("peer listen: %v", err)
	}
	defer gs.GracefulStop()
	log.Printf("peer gRPC on %s tls=%v mtls=%v", lis.Addr(), len(peerSrvOpts) > 0, *peerMTLS)

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

		var trOpts []peer.TransportOption
		if peerDialTLS != nil {
			trOpts = append(trOpts, peer.WithTLS(peerDialTLS))
		}
		transport = peer.NewTransport(500*time.Millisecond, trOpts...)
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

// buildTLS returns gRPC server options for cache/peer and optional peer client TLS.
// Plaintext when cert/key are empty.
func buildTLS(
	certFile, keyFile, peerClientCA string, peerMTLS bool,
	cacheClientCA string, cacheMTLS bool,
	peerClientCert, peerClientKey, peerServerName string,
) (cacheOpts, peerOpts []grpc.ServerOption, peerDial *tls.Config, err error) {
	if certFile == "" && keyFile == "" {
		if peerMTLS || cacheMTLS || peerClientCA != "" || cacheClientCA != "" {
			return nil, nil, nil, fmt.Errorf("tls flags set but -tls-cert/-tls-key missing")
		}
		return nil, nil, nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, nil, nil, fmt.Errorf("both -tls-cert and -tls-key are required")
	}

	// Cache server: optional client auth via -cache-client-ca (fallback -tls-client-ca).
	cacheCA := cacheClientCA
	if cacheCA == "" && cacheMTLS {
		cacheCA = peerClientCA
	}
	cacheTLS, err := tlsconfig.ServerFiles(certFile, keyFile, cacheCA, cacheMTLS && cacheCA != "")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cache server tls: %w", err)
	}
	cacheOpts = []grpc.ServerOption{grpc.Creds(credentials.NewTLS(cacheTLS))}

	// Peer server: mTLS when -peer-mtls and CA provided.
	peerCA := peerClientCA
	if peerMTLS && peerCA == "" {
		return nil, nil, nil, fmt.Errorf("-peer-mtls requires -tls-client-ca")
	}
	peerTLS, err := tlsconfig.ServerFiles(certFile, keyFile, peerCA, peerMTLS)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("peer server tls: %w", err)
	}
	peerOpts = []grpc.ServerOption{grpc.Creds(credentials.NewTLS(peerTLS))}

	// Outbound peer dials: present client cert when mTLS is enabled.
	if peerCA != "" || peerMTLS {
		cCert, cKey := peerClientCert, peerClientKey
		if cCert == "" {
			cCert = certFile
		}
		if cKey == "" {
			cKey = keyFile
		}
		peerDial, err = tlsconfig.ClientFiles(peerCA, peerServerName, cCert, cKey)
		if err != nil {
			// TLS without client CA still verifies server if we have a CA; if only server TLS
			// without peer CA, dial with system-less RootCAs from server cert file is wrong.
			// Require peer CA for dial when using TLS clustering.
			return nil, nil, nil, fmt.Errorf("peer client tls: %w (set -tls-client-ca for cluster TLS)", err)
		}
	} else {
		// Server TLS only: peers need a CA to verify. Use the server cert file as trust if it is a full chain;
		// otherwise require -tls-client-ca. For simplicity, demand -tls-client-ca whenever TLS is on and cluster is used.
		// Dial config without mTLS still needs RootCAs — use cert file as PEM pool when it includes CA,
		// or the same -tls-cert if self-signed leaf is presented (clients verify leaf as CA via custom pool).
		peerDial, err = tlsconfig.ClientFiles(certFile, peerServerName, "", "")
		if err != nil {
			return nil, nil, nil, fmt.Errorf("peer client tls (server-only): %w; provide -tls-client-ca with CA PEM", err)
		}
	}
	return cacheOpts, peerOpts, peerDial, nil
}

