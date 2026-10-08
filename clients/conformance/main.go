package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "cache listen address (host:0 binds a free port)")
	useTLS := flag.Bool("tls", false, "serve TLS and print PEM paths")
	mtls := flag.Bool("mtls", false, "require client certificates (implies -tls)")
	flag.Parse()
	log.SetOutput(os.Stderr)

	srv, err := Listen(*addr, Options{TLS: *useTLS, MTLS: *mtls})
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	// Write is enough. Sync fails on a pipe (the language tests capture stdout)
	// with EINVAL, and the bytes are already in the pipe buffer.
	if _, err := fmt.Fprint(os.Stdout, ReadyLines(srv.Addr, srv.TLS)); err != nil {
		log.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
