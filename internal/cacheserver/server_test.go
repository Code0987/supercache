package cacheserver_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

// DeleteMany gRPC must return structured peer_failures per key, not only message text.
func TestDeleteManyResponsePeerFailures(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})

	// Cluster with missing peer so Delete returns MultiError.
	r := ring.New(16)
	r.SetPeers([]ring.Peer{
		{ID: "a", Addr: "127.0.0.1:19101"},
		{ID: "b", Addr: "127.0.0.1:19102"}, // down
	})
	tr := peer.NewTransport(100 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 2, QueueSize: 8})
	defer fo.Close()
	eng.AttachCluster(&engine.Cluster{SelfID: "a", Ring: r, Transport: tr, Fanout: fo})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	cachev1.RegisterCacheServer(gs, cacheserver.New(eng))
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cli := cachev1.NewCacheClient(conn)

	ctx := context.Background()
	_, _ = cli.Put(ctx, &cachev1.PutRequest{Keyspace: "demo", Key: "k", Value: []byte("v")})
	resp, err := cli.DeleteMany(ctx, &cachev1.DeleteManyRequest{Keyspace: "demo", Keys: []string{"k"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Errors) == 0 {
		t.Fatal("expected per-key errors")
	}
	ke := resp.Errors[0]
	if ke.Key != "k" {
		t.Fatalf("key=%q", ke.Key)
	}
	if len(ke.PeerFailures) == 0 {
		t.Fatalf("want structured peer_failures, got only message=%q", ke.Message)
	}
	found := false
	for _, pf := range ke.PeerFailures {
		if pf.PeerId == "b" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing peer b in failures: %+v", ke.PeerFailures)
	}
}
