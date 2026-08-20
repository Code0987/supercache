package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/keyspace"
)

const ks = "rl"

func runDemo(out io.Writer) error {
	window := 60 * time.Second
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: ks, Mode: keyspace.ModeCounter,
			MaxBytes: 4 << 20, TTL: 2 * window, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		return err
	}
	defer c.Close()

	ctx := context.Background()
	nodes := c.Nodes()
	cli, err := client.Dial(ctx, nodes[0].CacheAddr)
	if err != nil {
		return err
	}
	defer cli.Close()

	p := func(format string, args ...any) { fmt.Fprintf(out, format+"\n", args...) }
	p("Fixed-window rate limiter on ModeCounter (Incr returns n; n > limit denies)")
	p("Why not KV/Hash: blob RMW loses incrs; HINCRBY was excluded (hint coalesce).")
	p("")

	const limit int64 = 3
	var name string
	for i := 1; i <= 4; i++ {
		allowed, remaining, n, gotName, err := Allow(ctx, cli, ks, "alice", limit, window)
		if err != nil {
			return err
		}
		if i == 1 {
			name = gotName
		} else if gotName != name {
			return fmt.Errorf("window rolled mid-burst: %s → %s", name, gotName)
		}
		wantAllow := i <= 3
		if allowed != wantAllow || n != int64(i) {
			return fmt.Errorf("allow #%d: allowed=%v n=%d remaining=%d", i, allowed, n, remaining)
		}
		p("    Allow alice #%d → allowed=%v n=%d remaining=%d name=%s", i, allowed, n, remaining, gotName)
	}

	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range nodes {
			if n.Engine.HasLocal(ks, name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		return fmt.Errorf("RF locals=%d want 2 for %s", locals, name)
	}
	p("    RF=2 local copies of %s", name)

	if err := cli.Delete(ctx, ks, name); err != nil {
		return err
	}
	allowed, _, n, _, err := Allow(ctx, cli, ks, "alice", limit, window)
	if err != nil || !allowed || n != 1 {
		return fmt.Errorf("after Delete: allowed=%v n=%d err=%v", allowed, n, err)
	}
	p("    Delete(window) reset; next Allow n=%d", n)

	_, _, _, _, err = Allow(ctx, cli, ks, "alice", limit, time.Millisecond)
	if err == nil {
		return fmt.Errorf("want window<1s error")
	}
	p("    window<1s → %v", err)

	p("")
	p("OK: ModeCounter rate-limiter walkthrough passed")
	return nil
}
