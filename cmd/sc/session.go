package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/tlsconfig"
)

// session holds sticky multi-seed cache client state for one-shot cmds and the REPL.
type session struct {
	cfg *config

	mu      sync.Mutex
	cli     *client.Client
	addr    string // currently connected seed
	seedIdx int    // index of last successful / next-start seed
}

func newSession(cfg *config) *session {
	return &session{cfg: cfg}
}

// Close releases the cache connection.
func (s *session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cli != nil {
		_ = s.cli.Close()
		s.cli = nil
	}
}

// ConnectedAddr returns the active seed (empty if not dialed).
func (s *session) ConnectedAddr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// Client returns a live cache client, dialing seeds if needed.
func (s *session) Client() (*client.Client, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cli != nil {
		return s.cli, s.addr, nil
	}
	return s.dialLocked()
}

// Invalidate drops the current connection so the next Client() redials
// (starting at the next seed).
func (s *session) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cli != nil {
		_ = s.cli.Close()
		s.cli = nil
	}
	if n := len(s.cfg.addrs); n > 0 {
		s.seedIdx = (s.seedIdx + 1) % n
	}
	s.addr = ""
}

// dialLocked tries seeds starting at seedIdx (sticky). Caller holds s.mu.
func (s *session) dialLocked() (*client.Client, string, error) {
	addrs := s.cfg.addrs
	if len(addrs) == 0 {
		return nil, "", fmt.Errorf("no cache seeds configured")
	}
	var errs []string
	n := len(addrs)
	start := s.seedIdx % n
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		a := addrs[idx]
		c, err := dialOne(s.cfg, a)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", a, err))
			continue
		}
		if s.cli != nil {
			_ = s.cli.Close()
		}
		s.cli = c
		s.addr = a
		s.seedIdx = idx
		return s.cli, s.addr, nil
	}
	return nil, "", fmt.Errorf("all %d cache seed(s) failed:\n  %s", n, strings.Join(errs, "\n  "))
}

func dialOne(cfg *config, addr string) (*client.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	if cfg.tlsCA != "" {
		tlsCfg, err := tlsconfig.ClientFiles(cfg.tlsCA, cfg.tlsServer, cfg.tlsCert, cfg.tlsKey)
		if err != nil {
			return nil, err
		}
		return client.DialTLS(ctx, addr, tlsCfg)
	}
	if cfg.tlsCert != "" || cfg.tlsKey != "" {
		return nil, fmt.Errorf("-tls-ca is required when using client certs")
	}
	return client.Dial(ctx, addr)
}

// withClient runs fn with a client; on transport-ish failure, rotates seed and retries once.
func (s *session) withClient(fn func(cli *client.Client, addr string) error) error {
	cli, addr, err := s.Client()
	if err != nil {
		return err
	}
	err = fn(cli, addr)
	if err == nil || !isDialRetryable(err) {
		return err
	}
	// Fail over to next seed.
	s.Invalidate()
	cli, addr, err2 := s.Client()
	if err2 != nil {
		return fmt.Errorf("%w (re-dial: %v)", err, err2)
	}
	return fn(cli, addr)
}

func isDialRetryable(err error) bool {
	if err == nil {
		return false
	}
	// gRPC connection errors typically contain these; avoid retrying app-level errors.
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"transport is closing",
		"unavailable",
		"no servers available",
		"error reading from server",
		"connection error",
		"i/o timeout",
		"deadline exceeded",
		"name resolver error",
		"failed to exit idle mode",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}
