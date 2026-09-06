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

const ks = "items"

func runDemo(out io.Writer) error {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: ks, Mode: keyspace.ModeVectorSet,
			MaxBytes: 4 << 20, TTL: time.Hour, ReplicationFactor: 2,
			VectorDim: 2, VectorMetric: keyspace.VectorMetricCosine,
		}},
	})
	if err != nil {
		return err
	}
	defer c.Close()

	ctx := context.Background()
	cli, err := client.Dial(ctx, c.Nodes()[0].CacheAddr)
	if err != nil {
		return err
	}
	defer cli.Close()

	p := func(format string, args ...any) { fmt.Fprintf(out, format+"\n", args...) }
	p("ModeVectorSet example — cosine neighbors (not ZSet scores)")
	if err := cli.VAdd(ctx, ks, "set", []byte("east"), []float32{1, 0}); err != nil {
		return err
	}
	if err := cli.VAdd(ctx, ks, "set", []byte("north"), []float32{0, 1}); err != nil {
		return err
	}
	if err := cli.VAdd(ctx, ks, "set", []byte("ne"), []float32{0.7, 0.7}); err != nil {
		return err
	}
	var hits []struct {
		Member []byte
		Score  float32
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := cli.VSim(ctx, ks, "set", []float32{1, 0.05}, 2)
		if err != nil {
			return err
		}
		if len(got) >= 1 && string(got[0].Member) == "east" {
			hits = make([]struct {
				Member []byte
				Score  float32
			}, len(got))
			for i, h := range got {
				hits[i].Member = h.Member
				hits[i].Score = h.Score
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(hits) == 0 || string(hits[0].Member) != "east" {
		return fmt.Errorf("expected east first, got %+v", hits)
	}
	p("    VSim ~east → %s (score=%g)", hits[0].Member, hits[0].Score)
	p("OK: ModeVectorSet walkthrough passed")
	return nil
}
