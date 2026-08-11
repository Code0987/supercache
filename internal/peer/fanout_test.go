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
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 1, QueueSize: 10, DisableHints: true})
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

func TestFanoutApplyEmptyAddr(t *testing.T) {
	tr := peer.NewTransport(20 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 1, QueueSize: 4, DisableHints: true})
	defer fo.Close()

	fails := fo.Apply(nil, []ring.Peer{{ID: "x"}}, "ks", "k", store.Entry{Version: 1, Flags: store.FlagTombstone}, 1)
	if len(fails) != 1 || fails[0].Err == nil {
		t.Fatalf("want empty-address failure, got %+v", fails)
	}
}

func TestSubmitDropHintsPeers(t *testing.T) {
	tr := peer.NewTransport(200 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{
		Workers: 1, QueueSize: 1, HintMaxPerPeer: 8,
	})
	defer fo.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				time.Sleep(time.Second)
				_ = c.Close()
			}(c)
		}
	}()
	p := ring.Peer{ID: "b", Addr: ln.Addr().String()}
	// Worker takes hang1; hang2 fills the one-slot queue.
	fo.Submit([]ring.Peer{p}, "ks", "hang1", store.Entry{Value: []byte("v"), Version: 1}, 1)
	fo.Submit([]ring.Peer{p}, "ks", "hang2", store.Entry{Value: []byte("v"), Version: 1}, 1)
	// Queue full: this submit must hint instead of dropping on the floor.
	fo.Submit([]ring.Peer{p}, "ks", "drop", store.Entry{Version: 3, Flags: store.FlagTombstone}, 1)
	if fo.HintPending() < 1 {
		t.Fatalf("dropped submit should enqueue a hint, pending=%d", fo.HintPending())
	}
}
