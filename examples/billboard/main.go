// Billboard demo: music trending charts on a 3-node SuperCache cluster.
//
//	go run ./examples/billboard
//	go run ./examples/billboard -demo=false   # serve only, skip scripted walkthrough
//	go run ./examples/billboard -hold=false   # run demo then exit (CI-friendly)
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var (
		appAddr    = flag.String("http", "127.0.0.1:18080", "billboard app HTTP listen address")
		sotLatency = flag.Duration("sot-latency", 120*time.Millisecond, "mock chart aggregator latency per load")
		runDemoFlg = flag.Bool("demo", true, "run scripted feature walkthrough after cluster is up")
		hold       = flag.Bool("hold", true, "keep serving after demo until Ctrl+C")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	logger.Println("══════════════════════════════════════════════════════════════════")
	logger.Println(" SuperCache example · Music Trending Billboard (cluster mode)")
	logger.Println("══════════════════════════════════════════════════════════════════")
	logger.Printf("config: app_http=%s sot_latency=%s demo=%v hold=%v", *appAddr, *sotLatency, *runDemoFlg, *hold)

	src := NewChartSource(logger, *sotLatency)
	logger.Printf("[main] mock chart SoT ready (artificial latency=%s)", *sotLatency)

	specs := defaultClusterSpecs()
	logger.Printf("[main] starting %d SuperCache nodes…", len(specs))
	var nodes []*runningNode
	for i, spec := range specs {
		if i > 0 {
			time.Sleep(200 * time.Millisecond)
		}
		n, err := startNode(spec, src, logger)
		if err != nil {
			logger.Fatalf("start node %s: %v", spec.ID, err)
		}
		nodes = append(nodes, n)
	}
	logger.Printf("[main] all nodes up — waiting for ring to settle…")
	time.Sleep(800 * time.Millisecond)
	for _, n := range nodes {
		peers := n.Engine.Peers()
		logger.Printf("[main] ring snapshot node=%s peers=%d gen=%d", n.Spec.ID, len(peers), n.Engine.RingGeneration())
		for _, p := range peers {
			logger.Printf("[main]   peer id=%s addr=%s", p.ID, p.Address)
		}
	}

	app, err := newAppServer(logger, specs, src)
	if err != nil {
		logger.Fatalf("app server: %v", err)
	}
	defer app.Close()

	srv := &http.Server{Addr: *appAddr, Handler: app.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Printf("[main] billboard HTTP listening on http://%s", *appAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("http: %v", err)
		}
	}()
	time.Sleep(150 * time.Millisecond)

	if *runDemoFlg {
		if err := runDemo("http://"+*appAddr, logger, src); err != nil {
			logger.Printf("[main] demo error: %v", err)
		}
	} else {
		logger.Printf("[main] demo skipped — try: curl -s http://%s/v1/charts/global", *appAddr)
	}

	if !*hold {
		logger.Println("[main] -hold=false → shutting down")
		shutdownAll(logger, srv, nodes)
		return
	}

	logger.Println("[main] serving until Ctrl+C …")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	logger.Println("[main] signal received")
	shutdownAll(logger, srv, nodes)
	fmt.Println("billboard bye")
}

func shutdownAll(logger *log.Logger, srv *http.Server, nodes []*runningNode) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	for i := len(nodes) - 1; i >= 0; i-- {
		nodes[i].Close()
	}
	logger.Println("[main] shutdown complete")
}
