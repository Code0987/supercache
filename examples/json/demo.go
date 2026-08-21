package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/jsonx"
	"github.com/Code0987/supercache/pkg/keyspace"
)

const (
	ks   = "doc"
	user = "user"
)

// runDemo starts an in-process 3-node cluster and walks ModeJSON verbs.
func runDemo(out io.Writer) error {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: ks, Mode: keyspace.ModeJSON,
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

	p("ModeJSON example — nested document with path mutate (not a Hash, not one Put blob)")
	p("3 in-process nodes, keyspace %q, RF=2. App clients: pkg/client.", ks)
	p("")

	p("=== 1) Why not Put(json) / Hash? ===")
	p("    A CacheOnly blob rewrite is one LWW value. Two writers on different")
	p("    paths clobber the whole document. Hash is flat field→bytes: no nesting,")
	p("    no arrays, no typed numbers/bools/null.")
	p("")

	p("=== 2) JsonSet root on n0 ===")
	if err := clis[0].JsonSet(ctx, ks, user, "$", []byte(`{"name":"Ada","n":1}`)); err != nil {
		return fmt.Errorf("JsonSet $: %w", err)
	}
	p("    JsonSet %s $ {\"name\":\"Ada\",\"n\":1}", user)

	p("")
	p("=== 3) Nested object parents ($.addr.city) ===")
	if err := clis[0].JsonSet(ctx, ks, user, "$.addr.city", []byte(`"Paris"`)); err != nil {
		return fmt.Errorf("JsonSet city: %w", err)
	}
	p("    JsonSet %s $.addr.city \"Paris\" — missing objects are created", user)

	p("")
	p("=== 4) JsonGet integer stays an integer ===")
	v, ok, err := clis[0].JsonGet(ctx, ks, user, "$.n")
	if err != nil || !ok || !jsonx.Equal(v, []byte("1")) {
		return fmt.Errorf("JsonGet $.n: ok=%v val=%q err=%v", ok, v, err)
	}
	p("    JsonGet $.n → %s (UseNumber; not a float)", v)

	p("")
	p("=== 5) Missing path is ok=false ===")
	_, miss, err := clis[0].JsonGet(ctx, ks, user, "$.nope")
	if err != nil || miss {
		return fmt.Errorf("missing path should be ok=false: %v %v", miss, err)
	}
	p("    JsonGet $.nope → present=false")

	p("")
	p("=== 6) JsonDel a subtree; document stays live ===")
	if err := clis[0].JsonDel(ctx, ks, user, "$.addr"); err != nil {
		return fmt.Errorf("JsonDel addr: %w", err)
	}
	root, ok, err := clis[0].JsonGet(ctx, ks, user, "$")
	if err != nil || !ok {
		return fmt.Errorf("root after del: %v %v", ok, err)
	}
	p("    JsonDel $.addr; JsonGet $ still present: %s", root)

	p("")
	p("=== 7) JsonDel $ → live {}; HasLocal until Delete ===")
	if err := clis[0].JsonDel(ctx, ks, user, "$"); err != nil {
		return err
	}
	root, ok, err = clis[0].JsonGet(ctx, ks, user, "$")
	if err != nil || !ok || !jsonx.Equal(root, []byte(`{}`)) {
		return fmt.Errorf("root del: %s %v %v", root, ok, err)
	}
	if err := waitLocals(nodes, user, 2, 2*time.Second); err != nil {
		return err
	}
	p("    JsonDel $ → {}; locals %s (empty document remains)", localSummary(nodes, user))
	if err := clis[0].Delete(ctx, ks, user); err != nil {
		return fmt.Errorf("Delete: %w", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, present, err := clis[0].JsonGet(ctx, ks, user, "$")
		if err == nil && !present {
			ok = false
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if ok {
		return fmt.Errorf("after Delete want miss")
	}
	p("    Delete(%s) tombstone; JsonGet $ present=false", user)

	p("")
	p("=== 8) RF=2 HasLocal after recreate ===")
	if err := clis[1].JsonSet(ctx, ks, user, "$", []byte(`{}`)); err != nil {
		return err
	}
	if err := waitLocals(nodes, user, 2, 2*time.Second); err != nil {
		return err
	}
	p("    local copies after recreate: %s", localSummary(nodes, user))

	p("")
	p("=== 9) No auto-created arrays ===")
	if err := clis[0].JsonSet(ctx, ks, user, "$.a[0]", []byte(`1`)); err == nil {
		return fmt.Errorf("expected error for $.a[0] on {}")
	}
	p("    JsonSet $.a[0] on {} → invalid argument (arrays are never auto-created)")

	p("")
	p("OK: ModeJSON document walkthrough passed")
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
