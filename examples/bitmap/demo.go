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

const (
	ks   = "flags"
	seen = "seen"
)

func runDemo(out io.Writer) error {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: ks, Mode: keyspace.ModeBitmap,
			MaxBytes: 4 << 20, TTL: time.Hour, ReplicationFactor: 2,
			MaxValueSize: 16,
		}},
	})
	if err != nil {
		return err
	}
	defer c.Close()

	ctx := context.Background()
	nodes := c.Nodes()
	clis := make([]*client.Client, 0, len(nodes))
	for _, n := range nodes {
		cli, err := client.Dial(ctx, n.CacheAddr)
		if err != nil {
			return fmt.Errorf("dial %s: %w", n.CacheAddr, err)
		}
		defer cli.Close()
		clis = append(clis, cli)
	}

	p := func(format string, args ...any) { fmt.Fprintf(out, format+"\n", args...) }

	p("ModeBitmap example — packed addressable bits (not Bloom, not one Put blob)")
	p("3 in-process nodes, keyspace %q, RF=2.", ks)
	p("")

	p("=== 1) Why not Put / Bloom / Hash / Counter? ===")
	p("    A CacheOnly blob rewrite LWW-clobbers two SETBITs. Bloom is approximate.")
	p("    Hash is field→bytes. Counter is one int64.")
	p("")

	p("=== 2) BitSet two offsets ===")
	if err := clis[0].BitSet(ctx, ks, seen, 0, true); err != nil {
		return err
	}
	if err := clis[0].BitSet(ctx, ks, seen, 8, true); err != nil {
		return err
	}
	p("    BitSet %s 0=1 and 8=1", seen)

	p("")
	p("=== 3) BitGet ===")
	if err := waitBit(clis[0], 0, true, 2*time.Second); err != nil {
		return err
	}
	if err := waitBit(clis[0], 8, true, 2*time.Second); err != nil {
		return err
	}
	bit, ok, err := clis[0].BitGet(ctx, ks, seen, 3)
	if err != nil || !ok || bit {
		return fmt.Errorf("in-range zero: %v %v %v", bit, ok, err)
	}
	bit, ok, err = clis[0].BitGet(ctx, ks, seen, 100)
	if err != nil || !ok || bit {
		return fmt.Errorf("past end: %v %v %v", bit, ok, err)
	}
	_, miss, err := clis[0].BitGet(ctx, ks, "missing", 0)
	if err != nil || miss {
		return fmt.Errorf("missing name: %v %v", miss, err)
	}
	p("    BitGet 0 and 8 → 1; BitGet 3 / 100 → 0 (present); missing name → ok=false")

	p("")
	p("=== 4) BitCount / BitPos ===")
	n, err := clis[0].BitCount(ctx, ks, seen, 0, -1)
	if err != nil || n != 2 {
		return fmt.Errorf("count: %d %v", n, err)
	}
	n, err = clis[0].BitCount(ctx, ks, seen, 0, 0)
	if err != nil || n != 1 {
		return fmt.Errorf("count byte0: %d %v", n, err)
	}
	pos, found, err := clis[0].BitPos(ctx, ks, seen, true, 0, -1)
	if err != nil || !found || pos != 0 {
		return fmt.Errorf("pos: %d %v %v", pos, found, err)
	}
	pos, found, err = clis[0].BitPos(ctx, ks, seen, true, 1, 1)
	if err != nil || !found || pos != 8 {
		return fmt.Errorf("pos byte1: %d %v %v", pos, found, err)
	}
	p("    BitCount 0,-1 → 2; byte 0 only → 1; BitPos 1 in byte 1 → 8")

	p("")
	p("=== 5) Empty-until-delete ===")
	if err := clis[0].BitSet(ctx, ks, seen, 0, false); err != nil {
		return err
	}
	if err := clis[0].BitSet(ctx, ks, seen, 8, false); err != nil {
		return err
	}
	if err := waitCount(clis[0], 0, 2*time.Second); err != nil {
		return err
	}
	if err := waitLocals(nodes, seen, 2, 2*time.Second); err != nil {
		return err
	}
	if !anyLocal(nodes, seen) {
		return fmt.Errorf("cleared bits should remain until Delete")
	}
	p("    cleared last 1s; HasLocal %s", localSummary(nodes, seen))
	if err := clis[0].Delete(ctx, ks, seen); err != nil {
		return err
	}
	deadline := time.Now().Add(2 * time.Second)
	ok = true
	for time.Now().Before(deadline) {
		_, present, err := clis[0].BitGet(ctx, ks, seen, 0)
		if err == nil && !present {
			ok = false
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if ok {
		return fmt.Errorf("after Delete want miss")
	}
	p("    Delete(%s) tombstone; BitGet present=false", seen)

	p("")
	p("=== 6) Oversize offset ===")
	if err := clis[1].BitSet(ctx, ks, seen, 8*16, true); err == nil {
		return fmt.Errorf("expected oversize")
	}
	p("    BitSet offset 128 on MaxValueSize=16 → error (too large / invalid)")

	p("")
	p("=== 7) RF=2 after recreate ===")
	if err := clis[1].BitSet(ctx, ks, seen, 0, true); err != nil {
		return err
	}
	if err := waitLocals(nodes, seen, 2, 2*time.Second); err != nil {
		return err
	}
	p("    local copies: %s", localSummary(nodes, seen))

	p("")
	p("OK: ModeBitmap walkthrough passed")
	return nil
}

func waitBit(cli *client.Client, off uint64, want bool, d time.Duration) error {
	ctx := context.Background()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		bit, ok, err := cli.BitGet(ctx, ks, seen, off)
		if err == nil && ok && bit == want {
			return nil
		}
		time.Sleep(15 * time.Millisecond)
	}
	return fmt.Errorf("BitGet %d want %v", off, want)
}

func waitCount(cli *client.Client, want int64, d time.Duration) error {
	ctx := context.Background()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		n, err := cli.BitCount(ctx, ks, seen, 0, -1)
		if err == nil && n == want {
			return nil
		}
		time.Sleep(15 * time.Millisecond)
	}
	return fmt.Errorf("BitCount want %d", want)
}

func waitLocals(nodes []testcluster.Node, name string, want int, d time.Duration) error {
	deadline := time.Now().Add(d)
	var n int
	for time.Now().Before(deadline) {
		n = 0
		for _, node := range nodes {
			if node.Engine.HasLocal(ks, name) {
				n++
			}
		}
		if n == want {
			return nil
		}
		time.Sleep(15 * time.Millisecond)
	}
	return fmt.Errorf("local copies=%d want %d (%s)", n, want, localSummary(nodes, name))
}

func anyLocal(nodes []testcluster.Node, name string) bool {
	for _, n := range nodes {
		if n.Engine.HasLocal(ks, name) {
			return true
		}
	}
	return false
}

func localSummary(nodes []testcluster.Node, name string) string {
	s := ""
	for i, n := range nodes {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%s=%v", n.ID, n.Engine.HasLocal(ks, name))
	}
	return s
}
