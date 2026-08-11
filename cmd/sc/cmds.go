package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/Code0987/supercache/pkg/client"
)

func dispatch(ctx context.Context, sess *session, cmd string, args []string) int {
	switch strings.ToLower(cmd) {
	case "get":
		return cmdGet(ctx, sess, args)
	case "put", "set":
		return cmdPut(ctx, sess, args)
	case "del", "delete", "rm":
		return cmdDel(ctx, sess, args)
	case "ping":
		return cmdPing(ctx, sess)
	case "peers":
		return cmdAdmin(ctx, sess.cfg, "/peers")
	case "keyspaces", "ks":
		return cmdAdmin(ctx, sess.cfg, "/keyspaces")
	case "metrics":
		return cmdAdmin(ctx, sess.cfg, "/metrics")
	case "health", "healthz":
		return cmdAdmin(ctx, sess.cfg, "/healthz")
	case "ready", "readyz":
		return cmdAdmin(ctx, sess.cfg, "/readyz")
	case "bloom":
		return cmdBloom(ctx, sess, args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		return 2
	}
}

func cmdGet(ctx context.Context, sess *session, keys []string) int {
	if len(keys) == 0 {
		fmt.Fprintln(os.Stderr, "usage: get <key> [key...]")
		return 2
	}
	cfg := sess.cfg

	type item struct {
		Key    string `json:"key"`
		Found  bool   `json:"found"`
		Value  string `json:"value,omitempty"`
		Base64 bool   `json:"base64,omitempty"`
		Error  string `json:"error,omitempty"`
	}
	var items []item
	missing := 0

	err := sess.withClient(func(cli *client.Client, _ string) error {
		items = items[:0]
		missing = 0
		for _, k := range keys {
			v, err := cli.Get(ctx, cfg.keyspace, k)
			it := item{Key: k}
			switch {
			case errors.Is(err, client.ErrNotFound):
				it.Found = false
				missing++
			case err != nil:
				// transport? bubble for failover
				if isDialRetryable(err) {
					return err
				}
				it.Error = err.Error()
				missing++
			default:
				it.Found = true
				if cfg.base64 {
					it.Value = base64.StdEncoding.EncodeToString(v)
					it.Base64 = true
				} else if utf8.Valid(v) && isMostlyPrintable(v) {
					it.Value = string(v)
				} else {
					it.Value = base64.StdEncoding.EncodeToString(v)
					it.Base64 = true
				}
			}
			items = append(items, it)
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
		return 1
	}

	if cfg.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(map[string]any{
			"keyspace": cfg.keyspace,
			"seed":     sess.ConnectedAddr(),
			"items":    items,
		})
		if missing > 0 {
			return 1
		}
		return 0
	}

	for _, it := range items {
		if it.Error != "" {
			fmt.Fprintf(os.Stderr, "%s: %s\n", it.Key, it.Error)
			continue
		}
		if !it.Found {
			fmt.Fprintf(os.Stderr, "%s: (not found)\n", it.Key)
			continue
		}
		if len(keys) == 1 {
			if cfg.raw {
				_, _ = os.Stdout.WriteString(it.Value)
			} else {
				fmt.Println(it.Value)
				if it.Base64 && !cfg.base64 {
					fmt.Fprintln(os.Stderr, "# note: binary value shown as base64; pass -base64 to make it explicit")
				}
			}
			continue
		}
		if it.Base64 {
			fmt.Printf("%s\t(base64)\t%s\n", it.Key, it.Value)
		} else {
			fmt.Printf("%s\t%s\n", it.Key, it.Value)
		}
	}
	if missing > 0 {
		return 1
	}
	return 0
}

func cmdPut(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: put <key> <value> | put <key> -file <path>")
		return 2
	}
	cfg := sess.cfg
	key := args[0]
	var value []byte
	var err error

	switch {
	case cfg.filePath != "":
		value, err = readFileOrStdin(cfg.filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read value: %v\n", err)
			return 1
		}
	case len(args) >= 2:
		raw := strings.Join(args[1:], " ")
		if cfg.base64 {
			value, err = base64.StdEncoding.DecodeString(raw)
			if err != nil {
				fmt.Fprintf(os.Stderr, "base64 decode: %v\n", err)
				return 1
			}
		} else {
			value = []byte(raw)
		}
	default:
		fmt.Fprintln(os.Stderr, "usage: put <key> <value> | put <key> -file <path>")
		return 2
	}

	var opts []client.PutOption
	if cfg.ttlSet {
		opts = append(opts, client.WithTTL(cfg.ttl))
	}

	err = sess.withClient(func(cli *client.Client, _ string) error {
		return cli.Put(ctx, cfg.keyspace, key, value, opts...)
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "put: %v\n", err)
		return 1
	}
	if cfg.jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"ok": true, "keyspace": cfg.keyspace, "key": key,
			"bytes": len(value), "ttl_set": cfg.ttlSet, "ttl": cfg.ttl.String(),
			"seed": sess.ConnectedAddr(),
		})
	} else if !cfg.quiet {
		fmt.Printf("OK %s/%s (%d bytes) via %s\n", cfg.keyspace, key, len(value), sess.ConnectedAddr())
	}
	return 0
}

func cmdDel(ctx context.Context, sess *session, keys []string) int {
	if len(keys) == 0 {
		fmt.Fprintln(os.Stderr, "usage: del <key> [key...]")
		return 2
	}
	cfg := sess.cfg

	var delErr error
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		if len(keys) == 1 {
			e = cli.Delete(ctx, cfg.keyspace, keys[0])
		} else {
			e = cli.DeleteMany(ctx, cfg.keyspace, keys)
		}
		// Peer partial failures are not transport failures.
		var pf client.PeerFailures
		var ke client.KeyErrors
		if e != nil && (errors.As(e, &pf) || errors.As(e, &ke)) {
			delErr = e
			return nil
		}
		if e != nil && isDialRetryable(e) {
			return e
		}
		delErr = e
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "delete: %v\n", err)
		return 1
	}
	if delErr != nil {
		var pf client.PeerFailures
		var ke client.KeyErrors
		if errors.As(delErr, &pf) || errors.As(delErr, &ke) {
			fmt.Fprintf(os.Stderr, "warning: %v\n", delErr)
			if cfg.jsonOut {
				_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
					"ok": true, "partial": true, "keyspace": cfg.keyspace,
					"keys": keys, "warning": delErr.Error(), "seed": sess.ConnectedAddr(),
				})
			} else if !cfg.quiet {
				fmt.Printf("OK %s (with peer warnings) via %s\n", strings.Join(keys, ", "), sess.ConnectedAddr())
			}
			return 0
		}
		fmt.Fprintf(os.Stderr, "delete: %v\n", delErr)
		return 1
	}
	if cfg.jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"ok": true, "keyspace": cfg.keyspace, "keys": keys, "seed": sess.ConnectedAddr(),
		})
	} else if !cfg.quiet {
		fmt.Printf("OK deleted %s via %s\n", strings.Join(keys, ", "), sess.ConnectedAddr())
	}
	return 0
}

func cmdBloom(ctx context.Context, sess *session, args []string) int {
	if len(args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: bloom add|test <name> <item>")
		return 2
	}
	op, name, item := strings.ToLower(args[0]), args[1], args[2]
	var (
		maybe bool
		opErr error
	)
	err := sess.withClient(func(cli *client.Client, _ string) error {
		switch op {
		case "add":
			opErr = cli.BloomAdd(ctx, sess.cfg.keyspace, name, []byte(item))
			return opErr
		case "test":
			maybe, opErr = cli.BloomTest(ctx, sess.cfg.keyspace, name, []byte(item))
			return opErr
		default:
			opErr = fmt.Errorf("usage: bloom add|test <name> <item>")
			return nil
		}
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "bloom: %v\n", err)
		return 1
	}
	if opErr != nil && op != "add" && op != "test" {
		fmt.Fprintf(os.Stderr, "%v\n", opErr)
		return 2
	}
	if opErr != nil {
		fmt.Fprintf(os.Stderr, "bloom: %v\n", opErr)
		return 1
	}
	if op == "test" {
		fmt.Printf("maybe=%v\n", maybe)
	} else if !sess.cfg.quiet {
		fmt.Printf("OK bloom add %s %s\n", name, item)
	}
	return 0
}

func cmdPing(ctx context.Context, sess *session) int {
	err := sess.withClient(func(cli *client.Client, addr string) error {
		_, err := cli.Get(ctx, sess.cfg.keyspace, "__sc_ping__")
		if err != nil && !errors.Is(err, client.ErrNotFound) {
			return err
		}
		fmt.Printf("cache %s: ok\n", addr)
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cache: %v\n", err)
		return 1
	}

	code, body, aerr, used := adminGET(ctx, sess.cfg, "/healthz")
	if aerr != nil {
		fmt.Printf("admin: unreachable (%v)\n", aerr)
		return 0
	}
	fmt.Printf("admin %s: HTTP %d\n", used, code)
	if sess.cfg.jsonOut && len(body) > 0 {
		os.Stdout.Write(body)
	}
	return 0
}

func cmdAdmin(ctx context.Context, cfg *config, path string) int {
	code, body, err, used := adminGET(ctx, cfg, path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "admin %s: %v\n", path, err)
		return 1
	}
	if code >= 400 {
		fmt.Fprintf(os.Stderr, "admin %s HTTP %d\n", used, code)
		writeBody(body)
		return 1
	}
	if cfg.jsonOut {
		writeBody(body)
		return 0
	}
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		writeBody(body)
		return 0
	}
	if m, ok := v.(map[string]any); ok {
		m["_admin"] = used
		v = m
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	return 0
}

func writeBody(body []byte) {
	os.Stdout.Write(body)
	if len(body) == 0 || body[len(body)-1] != '\n' {
		fmt.Println()
	}
}

// adminGET tries each admin seed until one responds.
func adminGET(ctx context.Context, cfg *config, path string) (code int, body []byte, err error, used string) {
	admins := cfg.admins
	if len(admins) == 0 {
		return 0, nil, fmt.Errorf("no admin seeds configured"), ""
	}
	var errs []string
	for _, a := range admins {
		code, body, err = adminGETOne(ctx, a, path)
		if err == nil {
			return code, body, nil, a
		}
		errs = append(errs, fmt.Sprintf("%s: %v", a, err))
	}
	return 0, nil, fmt.Errorf("all admin seed(s) failed:\n  %s", strings.Join(errs, "\n  ")), ""
}

func adminGETOne(ctx context.Context, admin, path string) (int, []byte, error) {
	base := admin
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	base = strings.TrimRight(base, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

func readFileOrStdin(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(io.LimitReader(os.Stdin, 32<<20))
	}
	return os.ReadFile(path)
}

func isMostlyPrintable(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	bad := 0
	for _, c := range b {
		if c == '\n' || c == '\r' || c == '\t' {
			continue
		}
		if c < 32 || c == 127 {
			bad++
		}
	}
	return bad*10 <= len(b)
}
