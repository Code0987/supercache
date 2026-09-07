package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/keyspace"
)

const (
	rf          = 2
	ksMaxBytes  = 4 << 20
	defaultTTL  = 5 * time.Minute
	negativeTTL = 10 * time.Second
)

var modeNames = []string{
	"cacheonly", "loadthrough", "bloom", "set", "zset", "geo", "list",
	"hash", "counter", "json", "bitmap", "hll", "topk", "cms", "vectorset",
}

type mockSoT struct {
	latency time.Duration
	loads   atomic.Int64
}

func (m *mockSoT) Load(_ context.Context, key string) ([]byte, error) {
	m.loads.Add(1)
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	return []byte(fmt.Sprintf(`{"key":%q,"src":"sot"}`, key)), nil
}

func (m *mockSoT) Loads() int64 { return m.loads.Load() }

func labKeyspaces(src datasource.DataSource) []keyspace.Config {
	base := func(name string, mode keyspace.Mode) keyspace.Config {
		return keyspace.Config{
			Name: name, Mode: mode, MaxBytes: ksMaxBytes, TTL: defaultTTL,
			ReplicationFactor: rf,
		}
	}
	return []keyspace.Config{
		base("cacheonly", keyspace.ModeCacheOnly),
		{
			Name: "loadthrough", Mode: keyspace.ModeLoadThrough,
			MaxBytes: ksMaxBytes, TTL: defaultTTL, NegativeTTL: negativeTTL,
			ReplicationFactor: rf, DataSource: src, LoadTimeout: 3 * time.Second,
		},
		func() keyspace.Config {
			c := base("bloom", keyspace.ModeBloom)
			c.BloomBits = 64
			c.BloomHashes = 4
			return c
		}(),
		base("set", keyspace.ModeSet),
		base("zset", keyspace.ModeZSet),
		base("geo", keyspace.ModeGeo),
		base("list", keyspace.ModeList),
		base("hash", keyspace.ModeHash),
		base("counter", keyspace.ModeCounter),
		base("json", keyspace.ModeJSON),
		base("bitmap", keyspace.ModeBitmap),
		base("hll", keyspace.ModeHLL),
		func() keyspace.Config {
			c := base("topk", keyspace.ModeTopK)
			c.TopKSize = 10
			return c
		}(),
		base("cms", keyspace.ModeCMS),
		func() keyspace.Config {
			c := base("vectorset", keyspace.ModeVectorSet)
			c.VectorDim = 2
			return c
		}(),
	}
}

func startMesh(src datasource.DataSource) (*testcluster.Cluster, []*client.Client, error) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes:     3,
		Keyspaces: labKeyspaces(src),
	})
	if err != nil {
		return nil, nil, err
	}
	ctx := context.Background()
	clis := make([]*client.Client, 0, 3)
	for _, n := range c.Nodes() {
		cli, err := client.Dial(ctx, n.CacheAddr)
		if err != nil {
			c.Close()
			for _, x := range clis {
				_ = x.Close()
			}
			return nil, nil, fmt.Errorf("dial %s: %w", n.CacheAddr, err)
		}
		clis = append(clis, cli)
	}
	return c, clis, nil
}
