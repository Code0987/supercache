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
		{ID: "billboard-1", CacheAddr: "127.0.0.1:19401", PeerAddr: "127.0.0.1:19411", AdminAddr: "127.0.0.1:18091", GossipPort: 17951},
		{ID: "billboard-2", CacheAddr: "127.0.0.1:19402", PeerAddr: "127.0.0.1:19412", AdminAddr: "127.0.0.1:18092", GossipPort: 17952, Seeds: []string{"127.0.0.1:17951"}},
		{ID: "billboard-3", CacheAddr: "127.0.0.1:19403", PeerAddr: "127.0.0.1:19413", AdminAddr: "127.0.0.1:18093", GossipPort: 17953, Seeds: []string{"127.0.0.1:17951"}},
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
	if !strings.Contains(out, "OK: ModeCMS") {
		t.Fatalf("missing ModeCMS OK:\n%s", out)
	}
	if !strings.Contains(out, "cms t003") || !strings.Contains(out, "cms t001") || !strings.Contains(out, "cmsincr t003") {
		t.Fatalf("missing cms count lines:\n%s", out)
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
