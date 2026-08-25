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
	ks       = "uniq"
	visitors = "visitors"
)

func runDemo(out io.Writer) error {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: ks, Mode: keyspace.ModeHLL,
			MaxBytes: 4 << 20, TTL: time.Hour, ReplicationFactor: 2,
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

	p("ModeHLL example — approximate distinct count (not Set, Counter, Bloom, or Bitmap)")
	p("3 in-process nodes, keyspace %q, RF=2.", ks)
	p("")

	p("=== 1) Why not Set / Counter / Bloom / Bitmap? ===")
	p("    SetCard is exact O(n). Counter is not distinct.")
	p("    Bloom is membership. BitCount counts bits, not hashed items.")
	p("")

	p("=== 2) HLLAdd distinct members ===")
	for _, m := range []string{"alice", "bob", "carol"} {
		if err := clis[0].HLLAdd(ctx, ks, visitors, []byte(m)); err != nil {
			return err
		}
	}
	n, ok, err := waitCount(clis[0], 1, 4, 2*time.Second)
	if err != nil {
		return err
	}
	p("    HLLAdd alice, bob, carol → count=%d ok=%v", n, ok)

	p("")
	p("=== 3) Duplicate add does not grow ===")
	if err := clis[0].HLLAdd(ctx, ks, visitors, []byte("alice")); err != nil {
		return err
	}
	n2, ok, err := clis[0].HLLCount(ctx, ks, visitors)
	if err != nil || !ok {
		return fmt.Errorf("after dup: %v %v", n2, err)
	}
	if n2 != n {
		return fmt.Errorf("duplicate grew %d → %d", n, n2)
	}
	p("    HLLAdd alice again → still %d", n2)

	p("")
	p("=== 4) Missing name ===")
	_, present, err := clis[0].HLLCount(ctx, ks, "missing")
	if err != nil || present {
		return fmt.Errorf("missing name: %v %v", present, err)
	}
	p("    HLLCount missing → ok=false")

	p("")
	p("=== 5) RF=2 HasLocal ===")
	if err := waitLocals(nodes, visitors, 2, 2*time.Second); err != nil {
		return err
	}
	p("    local copies: %s", localSummary(nodes, visitors))

	p("")
	p("=== 6) Delete then recreate ===")
	if err := clis[0].Delete(ctx, ks, visitors); err != nil {
		return err
	}
	deadline := time.Now().Add(2 * time.Second)
	present = true
	for time.Now().Before(deadline) {
		_, ok, err = clis[0].HLLCount(ctx, ks, visitors)
		if err == nil && !ok {
			present = false
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if present {
		return fmt.Errorf("after Delete want miss")
	}
	if err := clis[1].HLLAdd(ctx, ks, visitors, []byte("dave")); err != nil {
		return err
	}
	if _, _, err := waitCount(clis[1], 1, 2, 2*time.Second); err != nil {
		return err
	}
	if err := waitLocals(nodes, visitors, 2, 2*time.Second); err != nil {
		return err
	}
	p("    Delete + HLLAdd dave recreates; copies: %s", localSummary(nodes, visitors))

	p("")
	p("OK: ModeHLL walkthrough passed")
	return nil
}

func waitCount(cli *client.Client, lo, hi uint64, d time.Duration) (uint64, bool, error) {
	ctx := context.Background()
	deadline := time.Now().Add(d)
	var last uint64
	var ok bool
	for time.Now().Before(deadline) {
		n, present, err := cli.HLLCount(ctx, ks, visitors)
		if err == nil && present && n >= lo && n <= hi {
			return n, true, nil
		}
		last, ok = n, present
		time.Sleep(15 * time.Millisecond)
	}
	return last, ok, fmt.Errorf("HLLCount want %d..%d got %d ok=%v", lo, hi, last, ok)
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
