package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

const (
	ksCosine = "items"
	ksL2     = "items-l2"
	ksIP     = "items-ip"
	setName  = "set"
)

func runDemo(out io.Writer) error {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{
			{Name: ksCosine, Mode: keyspace.ModeVectorSet, MaxBytes: 4 << 20, TTL: time.Hour, ReplicationFactor: 2, VectorDim: 2, VectorMetric: keyspace.VectorMetricCosine},
			{Name: ksL2, Mode: keyspace.ModeVectorSet, MaxBytes: 4 << 20, TTL: time.Hour, ReplicationFactor: 2, VectorDim: 2, VectorMetric: keyspace.VectorMetricL2},
			{Name: ksIP, Mode: keyspace.ModeVectorSet, MaxBytes: 4 << 20, TTL: time.Hour, ReplicationFactor: 2, VectorDim: 2, VectorMetric: keyspace.VectorMetricIP},
		},
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
	cli := clis[0]
	p := func(format string, args ...any) { fmt.Fprintf(out, format+"\n", args...) }

	p("ModeVectorSet example — named embeddings + K-NN (not ZSet scores)")
	p("3 in-process nodes, RF=2. Cosine / L2 / IP are *keyspace* knobs.")
	p("")

	p("=== 1) Why not ZSet or Put(blob)? ===")
	p("    ZSet score is a scalar the caller writes. Put of [{id,vec}] is one LWW")
	p("    blob — two VAdds of different members would clobber each other.")
	p("")

	p("=== 2) VAdd three members (cosine keyspace, dim locked at 2) ===")
	seed := []struct {
		id  string
		vec []float32
	}{
		{"east", []float32{1, 0}},
		{"north", []float32{0, 1}},
		{"ne", []float32{0.7, 0.7}},
	}
	for _, m := range seed {
		if err := cli.VAdd(ctx, ksCosine, setName, []byte(m.id), m.vec); err != nil {
			return fmt.Errorf("VAdd %s: %w", m.id, err)
		}
		p("    VAdd %s %v", m.id, m.vec)
	}
	if err := waitLocals(nodes, ksCosine, setName, 2, 2*time.Second); err != nil {
		return err
	}
	n, ok, err := cli.VCard(ctx, ksCosine, setName)
	if err != nil || !ok || n != 3 {
		return fmt.Errorf("VCard: %d %v %v", n, ok, err)
	}
	dim, ok, err := cli.VDim(ctx, ksCosine, setName)
	if err != nil || !ok || dim != 2 {
		return fmt.Errorf("VDim: %d %v %v", dim, ok, err)
	}
	emb, ok, err := cli.VEmb(ctx, ksCosine, setName, []byte("east"))
	if err != nil || !ok || emb[0] != 1 || emb[1] != 0 {
		return fmt.Errorf("VEmb east: %v %v %v", emb, ok, err)
	}
	p("    VCard=%d VDim=%d VEmb(east)=%v copies: %s", n, dim, emb, localSummary(nodes, ksCosine, setName))

	p("")
	p("=== 3) VSim cosine from every node (best first) ===")
	for i, c := range clis {
		hits, err := waitSim(c, ksCosine, []float32{1, 0.05}, 3, "east", 2*time.Second)
		if err != nil {
			return fmt.Errorf("n%d VSim: %w", i, err)
		}
		if len(hits) != 3 || string(hits[0].Member) != "east" || string(hits[2].Member) != "north" {
			return fmt.Errorf("n%d order: %s", i, formatHits(hits))
		}
		p("    n%d VSim ~east → %s local=%v", i, formatHits(hits), nodes[i].Engine.HasLocal(ksCosine, setName))
	}
	p("    (non-replica GetOrLoads the snapshot and scores locally; does not install)")

	p("")
	p("=== 4) Replace member — VAdd same id overwrites the vector ===")
	if err := cli.VAdd(ctx, ksCosine, setName, []byte("east"), []float32{0, 1}); err != nil {
		return fmt.Errorf("replace east: %w", err)
	}
	hits, err := waitSim(cli, ksCosine, []float32{1, 0}, 3, "ne", 2*time.Second)
	if err != nil {
		return err
	}
	n, ok, err = cli.VCard(ctx, ksCosine, setName)
	if err != nil || !ok || n != 3 {
		return fmt.Errorf("replace grew/shrunk card=%d", n)
	}
	p("    VAdd east={0,1}; VSim ~east now %s (card still %d)", formatHits(hits), n)

	p("")
	p("=== 5) Dim lock + cosine rejects zero ===")
	if err := cli.VAdd(ctx, ksCosine, setName, []byte("bad"), []float32{1, 0, 0}); err == nil {
		return fmt.Errorf("dim-3 VAdd should fail")
	}
	p("    VAdd dim=3 → invalid")
	if err := cli.VAdd(ctx, ksCosine, setName, []byte("zero"), []float32{0, 0}); err == nil {
		return fmt.Errorf("zero VAdd should fail on cosine")
	}
	p("    VAdd {0,0} on cosine → invalid")
	if _, err := cli.Get(ctx, ksCosine, setName); !isInvalid(err) {
		return fmt.Errorf("Get: %v", err)
	}
	if err := cli.Put(ctx, ksCosine, setName, []byte("nope")); err == nil {
		return fmt.Errorf("Put should fail")
	}
	p("    Get/Put on ModeVectorSet → invalid (use VEmb / VAdd)")

	p("")
	p("=== 6) L2 vs IP — same members, different keyspace metric ===")
	for _, pair := range []struct {
		ks   string
		id   string
		vec  []float32
		note string
	}{
		{ksL2, "origin", []float32{0, 0}, "zero allowed on L2"},
		{ksL2, "far", []float32{10, 0}, ""},
		{ksIP, "origin", []float32{0, 0}, "zero allowed on IP"},
		{ksIP, "far", []float32{10, 0}, ""},
	} {
		if err := cli.VAdd(ctx, pair.ks, setName, []byte(pair.id), pair.vec); err != nil {
			return fmt.Errorf("VAdd %s/%s: %w", pair.ks, pair.id, err)
		}
		if pair.note != "" {
			p("    %s", pair.note)
		}
	}
	l2, err := waitSim(cli, ksL2, []float32{1, 0}, 1, "origin", 2*time.Second)
	if err != nil {
		return fmt.Errorf("L2: %w", err)
	}
	if l2[0].Score < 0 {
		return fmt.Errorf("L2 score %g want >=0", l2[0].Score)
	}
	ip, err := waitSim(cli, ksIP, []float32{1, 0}, 1, "far", 2*time.Second)
	if err != nil {
		return fmt.Errorf("IP: %w", err)
	}
	p("    L2 VSim {1,0} → %s  (nearest; lower distance first)", formatHits(l2))
	p("    IP VSim {1,0} → %s  (largest dot first)", formatHits(ip))

	p("")
	p("=== 7) k > card, ties, missing name ===")
	wide, err := cli.VSim(ctx, ksCosine, setName, []float32{0, 1}, 50)
	if err != nil || len(wide) != 3 {
		return fmt.Errorf("k>card: %d %v", len(wide), err)
	}
	p("    VSim k=50 → %d hits (min(k,card))", len(wide))
	miss, err := cli.VSim(ctx, ksCosine, "no-such", []float32{1, 0}, 3)
	if err != nil || len(miss) != 0 {
		return fmt.Errorf("missing VSim: %v %v", miss, err)
	}
	_, present, err := cli.VDim(ctx, ksCosine, "no-such")
	if err != nil || present {
		return fmt.Errorf("missing VDim: %v %v", present, err)
	}
	p("    missing name: VSim empty, VDim present=false")

	p("")
	p("=== 8) Last VRem keeps empty set (dim stays locked) ===")
	for _, id := range []string{"east", "north", "ne"} {
		if err := cli.VRem(ctx, ksCosine, setName, []byte(id)); err != nil {
			return fmt.Errorf("VRem %s: %w", id, err)
		}
	}
	if err := waitCard(cli, ksCosine, setName, 0, true, 2*time.Second); err != nil {
		return err
	}
	dim, ok, err = cli.VDim(ctx, ksCosine, setName)
	if err != nil || !ok || dim != 2 {
		return fmt.Errorf("empty VDim: %d %v %v", dim, ok, err)
	}
	empty, err := cli.VSim(ctx, ksCosine, setName, []float32{1, 0}, 3)
	if err != nil || len(empty) != 0 {
		return fmt.Errorf("empty VSim: %v %v", empty, err)
	}
	p("    after last VRem: VCard=0 VDim=2 VSim=[] (Delete is what drops the name)")

	p("")
	p("=== 9) Delete tombstone then recreate ===")
	if err := cli.Delete(ctx, ksCosine, setName); err != nil {
		return err
	}
	if err := waitCard(cli, ksCosine, setName, 0, false, 2*time.Second); err != nil {
		return err
	}
	if err := cli.VAdd(ctx, ksCosine, setName, []byte("west"), []float32{-1, 0}); err != nil {
		return fmt.Errorf("recreate: %w", err)
	}
	if err := waitCard(cli, ksCosine, setName, 1, true, 2*time.Second); err != nil {
		return err
	}
	if err := waitLocals(nodes, ksCosine, setName, 2, 2*time.Second); err != nil {
		return err
	}
	p("    Delete + VAdd west; copies: %s", localSummary(nodes, ksCosine, setName))

	p("")
	p("OK: ModeVectorSet walkthrough passed")
	return nil
}

func isInvalid(err error) bool {
	return err != nil && (errors.Is(err, engine.ErrInvalidArgument) ||
		strings.Contains(err.Error(), "InvalidArgument") ||
		strings.Contains(err.Error(), "use VEmb") ||
		strings.Contains(err.Error(), "use VAdd"))
}

func formatHits(hits []engine.VSimHit) string {
	parts := make([]string, len(hits))
	for i, h := range hits {
		parts[i] = fmt.Sprintf("%s=%.3f", h.Member, h.Score)
	}
	return strings.Join(parts, " ")
}

func waitSim(cli *client.Client, ks string, q []float32, k int, wantFirst string, d time.Duration) ([]engine.VSimHit, error) {
	ctx := context.Background()
	deadline := time.Now().Add(d)
	var last []engine.VSimHit
	for time.Now().Before(deadline) {
		hits, err := cli.VSim(ctx, ks, setName, q, k)
		if err != nil {
			return nil, err
		}
		last = hits
		if len(hits) > 0 && string(hits[0].Member) == wantFirst {
			return hits, nil
		}
		time.Sleep(15 * time.Millisecond)
	}
	return last, fmt.Errorf("VSim want first=%s got %s", wantFirst, formatHits(last))
}

func waitCard(cli *client.Client, ks, name string, want int, wantPresent bool, d time.Duration) error {
	ctx := context.Background()
	deadline := time.Now().Add(d)
	var n int
	var ok bool
	for time.Now().Before(deadline) {
		var err error
		n, ok, err = cli.VCard(ctx, ks, name)
		if err == nil && ok == wantPresent && n == want {
			return nil
		}
		time.Sleep(15 * time.Millisecond)
	}
	return fmt.Errorf("VCard want %d present=%v got %d present=%v", want, wantPresent, n, ok)
}

func waitLocals(nodes []testcluster.Node, ks, name string, want int, d time.Duration) error {
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
	return fmt.Errorf("local copies=%d want %d (%s)", n, want, localSummary(nodes, ks, name))
}

func localSummary(nodes []testcluster.Node, ks, name string) string {
	s := ""
	for i, n := range nodes {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%s=%v", n.ID, n.Engine.HasLocal(ks, name))
	}
	return s
}
