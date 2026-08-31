package main

import (
	"bytes"
	"log"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// lockedBuf is safe for startNode's per-node loggers, which each wrap
// logger.Writer() and would otherwise race on bytes.Buffer.
type lockedBuf struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *lockedBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func TestExampleBillboardModeTopK(t *testing.T) {
	var buf lockedBuf
	logger := log.New(&buf, "", 0)
	src := NewChartSource(logger, 20*time.Millisecond)
	specs := []nodeSpec{
		{ID: "billboard-1", CacheAddr: "127.0.0.1:19101", PeerAddr: "127.0.0.1:19201", AdminAddr: "127.0.0.1:18081", GossipPort: 17941},
		{ID: "billboard-2", CacheAddr: "127.0.0.1:19102", PeerAddr: "127.0.0.1:19202", AdminAddr: "127.0.0.1:18082", GossipPort: 17942, Seeds: []string{"127.0.0.1:17941"}},
		{ID: "billboard-3", CacheAddr: "127.0.0.1:19103", PeerAddr: "127.0.0.1:19203", AdminAddr: "127.0.0.1:18083", GossipPort: 17943, Seeds: []string{"127.0.0.1:17941"}},
	}
	var nodes []*runningNode
	for i, spec := range specs {
		if i > 0 {
			time.Sleep(150 * time.Millisecond)
		}
		n, err := startNode(spec, src, logger)
		if err != nil {
			t.Skipf("bind cluster: %v", err)
		}
		nodes = append(nodes, n)
	}
	defer func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			nodes[i].Close()
		}
	}()
	time.Sleep(600 * time.Millisecond)

	app, err := newAppServer(logger, specs, src)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	const appAddr = "127.0.0.1:18180"
	hs := &http.Server{Addr: appAddr, Handler: app.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = hs.ListenAndServe() }()
	defer hs.Close()
	time.Sleep(80 * time.Millisecond)

	if err := runDemo("http://"+appAddr, logger, src, app); err != nil {
		t.Fatalf("%v\n%s", err, buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "OK: ModeTopK") {
		t.Fatalf("missing OK line:\n%s", out)
	}
	if !strings.Contains(out, "t001 200") || !strings.Contains(out, "t002 150") {
		t.Fatalf("missing locked head:\n%s", out)
	}
	if !strings.Contains(out, "evicted t003") {
		t.Fatalf("missing evicted head:\n%s", out)
	}
	if strings.Contains(out, "ModeZSet keyspace (board") || strings.Contains(out, "/v1/board/") {
		t.Fatalf("ZSet board path still present:\n%s", out)
	}
	if !strings.Contains(out, "LoadThrough keyspace (charts)") {
		t.Fatalf("official charts walkthrough missing:\n%s", out)
	}
}
