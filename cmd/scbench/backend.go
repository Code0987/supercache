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
	Get(ctx context.Context, worker int, key string) ([]byte, error)
	Set(ctx context.Context, worker int, key string, val []byte) error
	Delete(ctx context.Context, worker int, key string) error
	Close() error
}

func openBackend(ctx context.Context, backend, addr, keyspace string, poolSize, conns int) (kvStore, error) {
	if conns < 1 {
		conns = 1
	}
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
		return openSCPool(ctx, []string{addr}, keyspace, conns)
	default:
		return nil, fmt.Errorf("unknown backend %q", backend)
	}
}

// openSCPool dials connsPerAddr clients per address.
// Worker i uses clients[addrIdx][i%conns] with addrIdx = i%len(addrs) (or 0 if one addr).
func openSCPool(ctx context.Context, addrs []string, keyspace string, connsPerAddr int) (*scStore, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no SuperCache addresses")
	}
	if connsPerAddr < 1 {
		connsPerAddr = 1
	}
	clients := make([][]*client.Client, len(addrs))
	for i, addr := range addrs {
		clients[i] = make([]*client.Client, connsPerAddr)
		for j := 0; j < connsPerAddr; j++ {
			cli, err := client.Dial(ctx, addr)
			if err != nil {
				closeSCClients(clients)
				return nil, err
			}
			clients[i][j] = cli
		}
	}
	ping := clients[0][0]
	if err := ping.Put(ctx, keyspace, "__scbench_ping__", []byte("1"), client.WithTTL(time.Minute)); err != nil {
		closeSCClients(clients)
		return nil, fmt.Errorf("ping put (is supercache-node up? keyspace %q?): %w", keyspace, err)
	}
	return &scStore{cs: clients, ks: keyspace}, nil
}

func closeSCClients(cs [][]*client.Client) {
	for _, row := range cs {
		for _, c := range row {
			if c != nil {
				_ = c.Close()
			}
		}
	}
}

type redisStore struct{ c *redis.Client }

func (r *redisStore) Name() string { return "redis" }
func (r *redisStore) Get(ctx context.Context, _ int, key string) ([]byte, error) {
	return r.c.Get(ctx, key).Bytes()
}
func (r *redisStore) Set(ctx context.Context, _ int, key string, val []byte) error {
	return r.c.Set(ctx, key, val, 0).Err()
}
func (r *redisStore) Delete(ctx context.Context, _ int, key string) error {
	return r.c.Del(ctx, key).Err()
}
func (r *redisStore) Close() error { return r.c.Close() }

type scStore struct {
	cs [][]*client.Client // [addr][conn]
	ks string
}

func (s *scStore) Name() string { return "supercache" }

func (s *scStore) pick(worker int) *client.Client {
	nAddr := len(s.cs)
	nConn := len(s.cs[0])
	addrIdx := 0
	if nAddr > 1 {
		addrIdx = worker % nAddr
	}
	return s.cs[addrIdx][worker%nConn]
}

func (s *scStore) Get(ctx context.Context, worker int, key string) ([]byte, error) {
	return s.pick(worker).Get(ctx, s.ks, key)
}
func (s *scStore) Set(ctx context.Context, worker int, key string, val []byte) error {
	return s.pick(worker).Put(ctx, s.ks, key, val)
}
func (s *scStore) Delete(ctx context.Context, worker int, key string) error {
	return s.pick(worker).Delete(ctx, s.ks, key)
}
func (s *scStore) Close() error {
	closeSCClients(s.cs)
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
