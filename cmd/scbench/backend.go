package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Code0987/supercache/pkg/client"
)

type kvStore interface {
	Name() string
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, val []byte) error
	Close() error
}

func openBackend(ctx context.Context, backend, addr, keyspace string, poolSize int) (kvStore, error) {
	switch backend {
	case "redis":
		if poolSize < 10 {
			poolSize = 10
		}
		rdb := redis.NewClient(&redis.Options{
			Addr:         addr,
			PoolSize:     poolSize,
			MinIdleConns: min(32, poolSize),
		})
		if err := rdb.Ping(ctx).Err(); err != nil {
			return nil, err
		}
		return &redisStore{c: rdb}, nil
	case "supercache":
		cli, err := client.Dial(ctx, addr)
		if err != nil {
			return nil, err
		}
		if err := cli.Put(ctx, keyspace, "__scbench_ping__", []byte("1"), client.WithTTL(time.Minute)); err != nil {
			_ = cli.Close()
			return nil, fmt.Errorf("ping put (is supercache-node up? keyspace %q?): %w", keyspace, err)
		}
		return &scStore{c: cli, ks: keyspace}, nil
	default:
		return nil, fmt.Errorf("unknown backend %q", backend)
	}
}

type redisStore struct{ c *redis.Client }

func (r *redisStore) Name() string { return "redis" }
func (r *redisStore) Get(ctx context.Context, key string) ([]byte, error) {
	return r.c.Get(ctx, key).Bytes()
}
func (r *redisStore) Set(ctx context.Context, key string, val []byte) error {
	return r.c.Set(ctx, key, val, 0).Err()
}
func (r *redisStore) Close() error { return r.c.Close() }

type scStore struct {
	c  *client.Client
	ks string
}

func (s *scStore) Name() string { return "supercache" }
func (s *scStore) Get(ctx context.Context, key string) ([]byte, error) {
	return s.c.Get(ctx, s.ks, key)
}
func (s *scStore) Set(ctx context.Context, key string, val []byte) error {
	return s.c.Put(ctx, s.ks, key, val)
}
func (s *scStore) Close() error { return s.c.Close() }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
