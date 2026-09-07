// SuperCache Lab: interactive cluster + mode explorer.
//
//	go run ./examples/lab
//	go run ./examples/lab -hold=false   # walkthrough then exit (CI)
//	cd examples/lab/ui && npm run dev   # Vite :5173 proxies /v1
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var (
		httpAddr   = flag.String("http", "127.0.0.1:19080", "lab HTTP listen address")
		sotLatency = flag.Duration("sot-latency", 200*time.Millisecond, "mock LoadThrough SoT latency")
		inProcess  = flag.Bool("cluster", false, "start in-process 3-node demo mesh")
		addrs      = flag.String("addr", "", "comma-separated cache gRPC addrs (no in-process mesh)")
		runDemo    = flag.Bool("demo", true, "run scripted walkthrough after cluster is up")
		hold       = flag.Bool("hold", true, "keep serving after demo until Ctrl+C")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	logger.Println("SuperCache Lab")
	logger.Printf("config: http=%s cluster=%v addr=%q sot_latency=%s demo=%v hold=%v",
		*httpAddr, *inProcess, *addrs, *sotLatency, *runDemo, *hold)

	if !*hold && *httpAddr == "127.0.0.1:19080" && !*inProcess && *addrs == "" {
		if err := runWalkthrough(os.Stdout); err != nil {
			logger.Fatalf("walkthrough: %v", err)
		}
		return
	}

	lab, err := startLab(labConfig{
		HTTPAddr: *httpAddr, SoTLatency: *sotLatency,
		InProcess: *inProcess, Addrs: parseAddrs(*addrs),
	})
	if err != nil {
		logger.Fatalf("start: %v", err)
	}
	defer lab.Close()
	logger.Printf("lab HTTP http://%s  (Vite dev: examples/lab/ui npm run dev)", lab.Addr)
	info := lab.clusterJSON()
	logger.Printf("  backend mode=%v connected=%v", info["mode"], info["connected"])
	if !*inProcess && *addrs == "" {
		logger.Printf("  no mesh started — connect cache gRPC addrs in the UI or pass -addr / -cluster")
	}

	if *runDemo && !*hold {
		if err := runWalkthrough(os.Stdout); err != nil {
			logger.Fatalf("walkthrough: %v", err)
		}
	}

	if !*hold {
		return
	}
	logger.Printf("open http://%s/", lab.Addr)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	logger.Println("shutting down")
}
