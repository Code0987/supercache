// Command conformance is a single-node Cache gRPC server for client SDK tests.
//
//	go run ./clients/conformance
//	addr=127.0.0.1:12345
//
// -tls prints PEM paths. -mtls also requires a client certificate.
// The process serves until SIGINT or SIGTERM. Stdout is only the ready lines.
package main

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/tlsconfig"
)

// Options selects plaintext or TLS for Listen.
type Options struct {
	TLS  bool
	MTLS bool
}

// TLSMaterial is the PEM set written for a TLS server.
type TLSMaterial struct {
	Dir        string
	CA         string
	Cert       string
	Key        string
	ClientCert string
	ClientKey  string
}

// Server is a running conformance cache.
type Server struct {
	Engine *engine.Engine
	Addr   string
	TLS    *TLSMaterial

	gs   *grpc.Server
	once sync.Once
}

// Listen registers one keyspace per mode and serves Cache gRPC on addr.
// addr may be host:0. The bound address is Server.Addr.
func Listen(addr string, opt Options) (*Server, error) {
	if opt.MTLS {
		opt.TLS = true
	}
	eng := engine.New()
	if err := register(eng); err != nil {
		eng.Close()
		return nil, err
	}
	var material *TLSMaterial
	var srvOpts []grpc.ServerOption
	if opt.TLS {
		files, err := writeTLS()
		if err != nil {
			eng.Close()
			return nil, err
		}
		material = files
		cfg, err := tlsconfig.ServerFiles(files.Cert, files.Key, files.CA, opt.MTLS)
		if err != nil {
			eng.Close()
			return nil, err
		}
		srvOpts = append(srvOpts, grpc.Creds(credentials.NewTLS(cfg)))
	}
	gs, lis, err := cacheserver.ListenAndServe(addr, eng, srvOpts...)
	if err != nil {
		eng.Close()
		return nil, err
	}
	return &Server{Engine: eng, Addr: lis.Addr().String(), TLS: material, gs: gs}, nil
}

// Close stops gRPC and the engine. It is safe to call more than once.
func (s *Server) Close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		if s.gs != nil {
			s.gs.Stop()
		}
		if s.Engine != nil {
			s.Engine.Close()
		}
	})
}

// ReadyLines is the stdout block the process prints, addr first.
func ReadyLines(addr string, files *TLSMaterial) string {
	out := fmt.Sprintf("addr=%s\n", addr)
	if files == nil {
		return out
	}
	out += fmt.Sprintf("ca=%s\ncert=%s\nkey=%s\nclient_cert=%s\nclient_key=%s\n",
		files.CA, files.Cert, files.Key, files.ClientCert, files.ClientKey)
	return out
}

func register(eng *engine.Engine) error {
	const mb int64 = 32 << 20
	ttl := time.Minute
	cfgs := []keyspace.Config{
		{Name: "cacheonly", Mode: keyspace.ModeCacheOnly, MaxBytes: mb, TTL: ttl},
		{Name: "loadthrough", Mode: keyspace.ModeLoadThrough, MaxBytes: mb, TTL: ttl,
			DataSource: datasource.Map{"seeded": []byte("from-source")}},
		{Name: "bloom", Mode: keyspace.ModeBloom, MaxBytes: mb, TTL: ttl, BloomBits: 1 << 16, BloomHashes: 3},
		{Name: "set", Mode: keyspace.ModeSet, MaxBytes: mb, TTL: ttl},
		{Name: "zset", Mode: keyspace.ModeZSet, MaxBytes: mb, TTL: ttl},
		{Name: "geo", Mode: keyspace.ModeGeo, MaxBytes: mb, TTL: ttl},
		{Name: "list", Mode: keyspace.ModeList, MaxBytes: mb, TTL: ttl},
		{Name: "hash", Mode: keyspace.ModeHash, MaxBytes: mb, TTL: ttl},
		{Name: "counter", Mode: keyspace.ModeCounter, MaxBytes: mb, TTL: ttl},
		{Name: "json", Mode: keyspace.ModeJSON, MaxBytes: mb, TTL: ttl},
		{Name: "bitmap", Mode: keyspace.ModeBitmap, MaxBytes: mb, TTL: ttl},
		{Name: "hll", Mode: keyspace.ModeHLL, MaxBytes: mb, TTL: ttl},
		{Name: "topk", Mode: keyspace.ModeTopK, MaxBytes: mb, TTL: ttl, TopKSize: 10},
		{Name: "cms", Mode: keyspace.ModeCMS, MaxBytes: mb, TTL: ttl},
		{Name: "vectorset", Mode: keyspace.ModeVectorSet, MaxBytes: mb, TTL: ttl, VectorDim: 4},
		{Name: "stream", Mode: keyspace.ModeStream, MaxBytes: mb, TTL: ttl},
	}
	for _, cfg := range cfgs {
		if err := eng.UpdateKeySpace(cfg); err != nil {
			return fmt.Errorf("keyspace %s: %w", cfg.Name, err)
		}
	}
	return nil
}
