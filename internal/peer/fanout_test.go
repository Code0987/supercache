package peer_test

import (
	"net"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/store"
)

// Fan-out must contact peers concurrently; serial ApplyPut multiplies RTT.
func TestFanoutPeersInParallel(t *testing.T) {
	const n = 4
	const rpcTimeout = 120 * time.Millisecond

	var lns []net.Listener
	var peers []ring.Peer
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns = append(lns, ln)
		// Accept and hold the connection so gRPC RPCs block until client timeout.
		go func(ln net.Listener) {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go func(c net.Conn) {
					time.Sleep(5 * time.Second)
					_ = c.Close()
				}(c)
			}
		}(ln)
		peers = append(peers, ring.Peer{ID: string(rune('a' + i)), Addr: ln.Addr().String()})
	}
	defer func() {
		for _, ln := range lns {
			_ = ln.Close()
		}
	}()

	tr := peer.NewTransport(rpcTimeout)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 1, QueueSize: 10})
	// One worker so parallelism must be within the job, not across workers.
	defer fo.Close()

	start := time.Now()
	fo.Submit(peers, "ks", "k", store.Entry{Value: []byte("v"), Version: 1}, 1)

	// Wait until all peer RPCs have failed (timeout or hang).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if tr.FanoutErrors.Load() >= uint64(n) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(start)
	errs := tr.FanoutErrors.Load()
	if errs < uint64(n) {
		t.Fatalf("expected %d fanout errors, got %d (elapsed %v)", n, errs, elapsed)
	}
	// Serial: ~n * rpcTimeout. Parallel: ~1 * rpcTimeout (+ slack).
	serialFloor := time.Duration(n-1) * rpcTimeout
	if elapsed >= serialFloor {
		t.Fatalf("fan-out looks serial: elapsed=%v (serial floor ~%v for %d peers)", elapsed, serialFloor, n)
	}
}
