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
	ks   = "profile"
	user = "alice"
)

// runDemo starts an in-process 3-node cluster and walks ModeHash verbs.
func runDemo(out io.Writer) error {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: ks, Mode: keyspace.ModeHash,
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

	p("ModeHash example — user profile as field → value (not one JSON blob)")
	p("3 in-process nodes, keyspace %q, RF=2. App clients: pkg/client.", ks)
	p("")

	p("=== 1) Why Hash, not Put(json)? ===")
	p("    A CacheOnly blob rewrite is one LWW value. Two writers updating")
	p("    different fields clobber each other. Hash last-write-wins *per field*.")
	p("")

	p("=== 2) HSet three fields on n0 ===")
	seed := []struct{ field, value string }{
		{"email", "alice@example.com"},
		{"name", "Alice"},
		{"plan", "pro"},
	}
	for _, f := range seed {
		if err := clis[0].HSet(ctx, ks, user, []byte(f.field), []byte(f.value)); err != nil {
			return fmt.Errorf("HSet %s: %w", f.field, err)
		}
		p("    HSet %s %s = %q", user, f.field, f.value)
	}
	if err := waitLocals(nodes, user, 2, 2*time.Second); err != nil {
		return err
	}
	p("    local copies (want 2 for RF=2): %s", localSummary(nodes, user))

	p("")
	p("=== 3) HGet / HExists / HLen / HGetAll from every node ===")
	for i, cli := range clis {
		v, ok, err := cli.HGet(ctx, ks, user, []byte("email"))
		if err != nil || !ok || string(v) != "alice@example.com" {
			return fmt.Errorf("n%d HGet email: ok=%v val=%q err=%v", i, ok, v, err)
		}
		ex, err := cli.HExists(ctx, ks, user, []byte("plan"))
		if err != nil || !ex {
			return fmt.Errorf("n%d HExists plan: %v %v", i, ex, err)
		}
		n, err := cli.HLen(ctx, ks, user)
		if err != nil || n != 3 {
			return fmt.Errorf("n%d HLen: %d %v", i, n, err)
		}
		all, err := cli.HGetAll(ctx, ks, user)
		if err != nil || len(all) != 3 {
			return fmt.Errorf("n%d HGetAll: %v %v", i, all, err)
		}
		p("    n%d HGet email=%q HLen=%d HGetAll=%s local=%v",
			i, v, n, formatAll(all), nodes[i].Engine.HasLocal(ks, user))
	}
	p("    (non-replica HGet owner-forwards; replica with a local hash answers locally)")

	p("")
	p("=== 4) Concurrent field updates (the Hash contract) ===")
	p("    n1 sets email; n2 sets bio. Both must survive — unlike one JSON Put.")
	if err := clis[1].HSet(ctx, ks, user, []byte("email"), []byte("alice@new.example")); err != nil {
		return fmt.Errorf("HSet email: %w", err)
	}
	if err := clis[2].HSet(ctx, ks, user, []byte("bio"), []byte("writes caches")); err != nil {
		return fmt.Errorf("HSet bio: %w", err)
	}
	if err := waitField(clis[0], "email", "alice@new.example", 2*time.Second); err != nil {
		return err
	}
	if err := waitField(clis[0], "bio", "writes caches", 2*time.Second); err != nil {
		return err
	}
	all, err := clis[0].HGetAll(ctx, ks, user)
	if err != nil {
		return err
	}
	p("    HGetAll after both writes: %s", formatAll(all))
	if !hasField(all, "email", "alice@new.example") || !hasField(all, "bio", "writes caches") || !hasField(all, "name", "Alice") {
		return fmt.Errorf("lost a field after concurrent HSet: %s", formatAll(all))
	}

	p("")
	p("=== 5) Empty value is present (not a miss) ===")
	if err := clis[0].HSet(ctx, ks, user, []byte("nickname"), nil); err != nil {
		return err
	}
	v, ok, err := clis[0].HGet(ctx, ks, user, []byte("nickname"))
	if err != nil || !ok || v == nil || len(v) != 0 {
		return fmt.Errorf("empty HGet: %#v ok=%v err=%v", v, ok, err)
	}
	_, miss, err := clis[0].HGet(ctx, ks, user, []byte("no-such-field"))
	if err != nil || miss {
		return fmt.Errorf("missing field should be ok=false: %v %v", miss, err)
	}
	p("    HGet nickname → present empty %q", v)
	p("    HGet no-such-field → present=false")

	p("")
	p("=== 6) HDel one field; name stays ===")
	if err := clis[0].HDel(ctx, ks, user, []byte("plan")); err != nil {
		return err
	}
	if err := waitGone(clis[0], "plan", 2*time.Second); err != nil {
		return err
	}
	n, err := clis[0].HLen(ctx, ks, user)
	if err != nil {
		return err
	}
	p("    HDel plan; HLen=%d (hash still exists)", n)

	p("")
	p("=== 7) Empty-until-Delete(name) ===")
	for _, f := range []string{"email", "name", "bio", "nickname"} {
		if err := clis[0].HDel(ctx, ks, user, []byte(f)); err != nil {
			return err
		}
	}
	n, err = clis[0].HLen(ctx, ks, user)
	if err != nil || n != 0 {
		return fmt.Errorf("want empty hash HLen=0 got %d %v", n, err)
	}
	if !anyLocal(nodes, user) {
		return fmt.Errorf("empty hash should remain until Delete(name); locals %s", localSummary(nodes, user))
	}
	p("    last HDel: HLen=0, locals %s (empty entry remains)", localSummary(nodes, user))
	if err := clis[0].Delete(ctx, ks, user); err != nil {
		return fmt.Errorf("Delete: %w", err)
	}
	time.Sleep(200 * time.Millisecond)
	_, ok, err = clis[0].HGet(ctx, ks, user, []byte("email"))
	if err != nil || ok {
		return fmt.Errorf("after Delete want miss: ok=%v err=%v", ok, err)
	}
	p("    Delete(%s) tombstone; HGet email present=false", user)

	p("")
	p("=== 8) HSet after Delete recreates ===")
	if err := clis[1].HSet(ctx, ks, user, []byte("email"), []byte("back@example.com")); err != nil {
		return err
	}
	v, ok, err = clis[1].HGet(ctx, ks, user, []byte("email"))
	if err != nil || !ok || string(v) != "back@example.com" {
		return fmt.Errorf("recreate: %q %v %v", v, ok, err)
	}
	p("    HSet email after Delete → %q", v)

	p("")
	p("OK: ModeHash profile walkthrough passed")
	return nil
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

func waitField(cli *client.Client, field, want string, d time.Duration) error {
	ctx := context.Background()
	deadline := time.Now().Add(d)
	var last string
	var ok bool
	for time.Now().Before(deadline) {
		v, present, err := cli.HGet(ctx, ks, user, []byte(field))
		if err == nil && present && string(v) == want {
			return nil
		}
		ok = present
		last = string(v)
		time.Sleep(15 * time.Millisecond)
	}
	return fmt.Errorf("HGet %s: last=%q present=%v want %q", field, last, ok, want)
}

func waitGone(cli *client.Client, field string, d time.Duration) error {
	ctx := context.Background()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		ok, err := cli.HExists(ctx, ks, user, []byte(field))
		if err == nil && !ok {
			return nil
		}
		time.Sleep(15 * time.Millisecond)
	}
	return fmt.Errorf("HExists %s still true", field)
}

func formatAll(all []client.HashField) string {
	s := "{"
	for i, f := range all {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%s=%q", f.Field, f.Value)
	}
	return s + "}"
}

func hasField(all []client.HashField, field, value string) bool {
	for _, f := range all {
		if string(f.Field) == field && string(f.Value) == value {
			return true
		}
	}
	return false
}
