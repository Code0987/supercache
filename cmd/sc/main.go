// Command sc is a SuperCache CLI for cache ops, multi-seed failover, and a REPL.
//
//	go run ./cmd/supercache-node
//	sc put greeting "hello"
//	sc get greeting
//	sc -addr 127.0.0.1:9000,127.0.0.1:9010 ping
//	sc                        # interactive REPL
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// version is set at link time for releases: -ldflags "-X main.version=vX.Y.Z"
var version = "dev"

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	cmd, flagArgs, posArgs, err := splitArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	fs := flag.NewFlagSet("sc", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var (
		addr      = fs.String("addr", envOr("SC_ADDR", "127.0.0.1:9000"), "cache gRPC seed(s), comma-separated (app port, not peer)")
		admin     = fs.String("admin", envOr("SC_ADMIN", "127.0.0.1:8080"), "admin HTTP seed(s), comma-separated")
		keyspace  = fs.String("keyspace", envOr("SC_KEYSPACE", "cacheonly"), "default keyspace")
		timeout   = fs.Duration("timeout", 5*time.Second, "per-request timeout")
		ttl       = fs.Duration("ttl", 0, "TTL for put (0 + -no-expiry = never expire)")
		noExpiry  = fs.Bool("no-expiry", false, "put with TTL=0 (no expiry)")
		filePath  = fs.String("file", "", "put: read value from file (`-` = stdin)")
		base64IO  = fs.Bool("base64", false, "get: always base64; put: decode arg as base64")
		jsonOut   = fs.Bool("json", false, "JSON output where applicable")
		raw       = fs.Bool("raw", false, "get: value bytes only")
		quiet     = fs.Bool("q", false, "quiet success on put/del")
		tlsCA     = fs.String("tls-ca", envOr("SC_TLS_CA", ""), "PEM CA to verify Cache server")
		tlsCert   = fs.String("tls-cert", envOr("SC_TLS_CERT", ""), "client cert PEM (mTLS)")
		tlsKey    = fs.String("tls-key", envOr("SC_TLS_KEY", ""), "client key PEM (mTLS)")
		tlsServer = fs.String("tls-server-name", envOr("SC_TLS_SERVER_NAME", ""), "TLS ServerName / SNI")
	)

	wantHelp := cmd == "help" || cmd == "-h" || cmd == "--help" || hasHelp(args)
	if cmd == "version" || cmd == "--version" {
		fmt.Printf("sc %s\n", version)
		return 0
	}

	// No command: REPL on TTY, else help.
	if cmd == "" {
		_ = fs.Parse(flagArgs)
		if wantHelp || !stdinIsTTY() {
			printUsage()
			if wantHelp {
				return 0
			}
			return 2
		}
		cfg, err := configFromFlags(fs, addr, admin, keyspace, timeout, ttl, noExpiry, filePath, base64IO, jsonOut, raw, quiet, tlsCA, tlsCert, tlsKey, tlsServer)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		return runREPL(cfg)
	}

	if err := fs.Parse(flagArgs); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printUsage()
			return 0
		}
		fmt.Fprintln(os.Stderr, err)
		printNote("type help for commands")
		return 2
	}
	if wantHelp && (cmd == "help" || cmd == "-h" || cmd == "--help") {
		printUsage()
		return 0
	}

	cfg, err := configFromFlags(fs, addr, admin, keyspace, timeout, ttl, noExpiry, filePath, base64IO, jsonOut, raw, quiet, tlsCA, tlsCert, tlsKey, tlsServer)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	switch strings.ToLower(cmd) {
	case "repl", "shell", "i":
		return runREPL(cfg)
	case "get", "put", "set", "del", "delete", "rm", "ping",
		"peers", "keyspaces", "ks", "metrics", "health", "healthz", "ready", "readyz",
		"bloom", "sadd", "srem", "sismember", "scard", "smembers",
		"zadd", "zrem", "zscore", "zcard", "zrange", "zrangebyscore",
		"geoadd", "georem", "geopos", "geocard", "geodist", "georadius",
		"lpush", "rpush", "lpop", "rpop", "llen", "lindex", "lrange",
		"hset", "hget", "hdel", "hexists", "hlen", "hgetall",
		"incr", "cget",
		"jsonset", "jsonget", "jsondel",
		"bitset", "bitget", "bitcount", "bitpos",
		"hlladd", "hllcount",
		"topkadd", "topklist",
		"cmsincr", "cmsquery",
		"vadd", "vrem", "vsim", "vcard", "vdim", "vemb":
		sess := newSession(cfg)
		defer sess.Close()
		ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
		defer cancel()
		return dispatch(ctx, sess, cmd, posArgs)
	default:
		printUnknown(cmd)
		printNote("type help for commands")
		return 2
	}
}

func configFromFlags(
	_ *flag.FlagSet,
	addr, admin, keyspace *string,
	timeout, ttl *time.Duration,
	noExpiry *bool,
	filePath *string,
	base64IO, jsonOut, raw, quiet *bool,
	tlsCA, tlsCert, tlsKey, tlsServer *string,
) (*config, error) {
	addrs := parseList(*addr)
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no cache addresses in -addr")
	}
	admins := parseList(*admin)
	cfg := &config{
		addrs:     addrs,
		admins:    admins,
		keyspace:  *keyspace,
		timeout:   *timeout,
		ttl:       *ttl,
		ttlSet:    *noExpiry || *ttl != 0,
		filePath:  *filePath,
		base64:    *base64IO,
		jsonOut:   *jsonOut,
		raw:       *raw,
		quiet:     *quiet,
		tlsCA:     *tlsCA,
		tlsCert:   *tlsCert,
		tlsKey:    *tlsKey,
		tlsServer: *tlsServer,
	}
	if *noExpiry {
		cfg.ttl = 0
		cfg.ttlSet = true
	}
	applyJSONColor(cfg.jsonOut)
	return cfg, nil
}

type config struct {
	addrs, admins                     []string
	keyspace                          string
	timeout                           time.Duration
	ttl                               time.Duration
	ttlSet                            bool
	filePath                          string
	base64, jsonOut, raw, quiet       bool
	tlsCA, tlsCert, tlsKey, tlsServer string
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func hasHelp(args []string) bool {
	for _, a := range args {
		if a == "-h" || a == "--help" {
			return true
		}
	}
	return false
}

func stdinIsTTY() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// parseList splits a comma-separated list, trims space, drops empties.
func parseList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	return out
}

// splitArgs finds the subcommand and separates flag tokens from positional args.
func splitArgs(args []string) (cmd string, flagArgs, posArgs []string, err error) {
	takesValue := map[string]bool{
		"-addr": true, "-admin": true, "-keyspace": true, "-timeout": true,
		"-ttl": true, "-file": true, "-tls-ca": true, "-tls-cert": true,
		"-tls-key": true, "-tls-server-name": true,
	}

	tokens := args
	i := 0
	for i < len(tokens) {
		a := tokens[i]
		if a == "--" {
			i++
			break
		}
		if strings.HasPrefix(a, "-") {
			name := a
			if eq := strings.IndexByte(a, '='); eq >= 0 {
				flagArgs = append(flagArgs, a)
				i++
				continue
			}
			flagArgs = append(flagArgs, a)
			if takesValue[name] {
				if i+1 >= len(tokens) {
					return "", nil, nil, fmt.Errorf("flag %s needs a value", name)
				}
				flagArgs = append(flagArgs, tokens[i+1])
				i += 2
				continue
			}
			i++
			continue
		}
		cmd = a
		i++
		break
	}

	for i < len(tokens) {
		a := tokens[i]
		if a == "--" {
			posArgs = append(posArgs, tokens[i+1:]...)
			break
		}
		if strings.HasPrefix(a, "-") {
			if a == "-" {
				posArgs = append(posArgs, a)
				i++
				continue
			}
			name := a
			if eq := strings.IndexByte(a, '='); eq >= 0 {
				flagArgs = append(flagArgs, a)
				i++
				continue
			}
			flagArgs = append(flagArgs, a)
			if takesValue[name] {
				if i+1 >= len(tokens) {
					return "", nil, nil, fmt.Errorf("flag %s needs a value", name)
				}
				flagArgs = append(flagArgs, tokens[i+1])
				i += 2
				continue
			}
			i++
			continue
		}
		posArgs = append(posArgs, a)
		i++
	}
	return cmd, flagArgs, posArgs, nil
}
