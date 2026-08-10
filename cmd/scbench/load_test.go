package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Code0987/supercache/pkg/client"
)

func TestSampleCapPerWorker(t *testing.T) {
	sum := func(xs []int) int {
		n := 0
		for _, v := range xs {
			n += v
		}
		return n
	}
	cases := []struct {
		cap, conc int
	}{
		{262144, 1},
		{262144, 100},
		{262144, 1000},
		{10, 100},
		{0, 8},
	}
	for _, tc := range cases {
		got := sampleCapPerWorker(tc.cap, tc.conc)
		if len(got) != tc.conc {
			t.Fatalf("cap=%d conc=%d len=%d", tc.cap, tc.conc, len(got))
		}
		if s := sum(got); s != tc.cap {
			t.Fatalf("cap=%d conc=%d sum=%d want %d slice=%v", tc.cap, tc.conc, s, tc.cap, got)
		}
	}
	if sampleCapPerWorker(10, 0) != nil {
		t.Fatal("conc=0")
	}
}

type stubStore struct {
	getErr error
	gets   atomic.Int64
}

func (s *stubStore) Name() string { return "stub" }
func (s *stubStore) Get(_ context.Context, _ int, _ string) ([]byte, error) {
	s.gets.Add(1)
	if s.getErr != nil {
		return nil, s.getErr
	}
	return []byte("v"), nil
}
func (s *stubStore) Set(context.Context, int, string, []byte) error { return nil }
func (s *stubStore) Delete(context.Context, int, string) error      { return nil }
func (s *stubStore) Close() error                                   { return nil }

func TestRequireHitSwallowNotFound(t *testing.T) {
	st := &stubStore{getErr: client.ErrNotFound}
	res, err := runLoad(context.Background(), st, loadConfig{
		op: "get", prefix: "k", keys: 10, value: []byte("v"),
		concurrency: 1, duration: 20 * time.Millisecond,
		requireHit: false, sampleCap: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Ops == 0 {
		t.Fatal("expected swallowed not-found to count as ops")
	}
	if res.Errors != 0 {
		t.Fatalf("errors=%d", res.Errors)
	}
}

func TestRequireHitCountsNotFound(t *testing.T) {
	st := &stubStore{getErr: client.ErrNotFound}
	res, err := runLoad(context.Background(), st, loadConfig{
		op: "get", prefix: "k", keys: 10, value: []byte("v"),
		concurrency: 1, duration: 20 * time.Millisecond,
		requireHit: true, sampleCap: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Errors == 0 {
		t.Fatal("expected not-found to count as errors")
	}
	if res.Ops != 0 {
		t.Fatalf("ops=%d want 0", res.Ops)
	}
}

func TestRequireHitSwallowRedisNil(t *testing.T) {
	st := &stubStore{getErr: redis.Nil}
	res, err := runLoad(context.Background(), st, loadConfig{
		op: "get", prefix: "k", keys: 4, value: []byte("v"),
		concurrency: 1, duration: 15 * time.Millisecond,
		requireHit: false, sampleCap: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Ops == 0 || res.Errors != 0 {
		t.Fatalf("redis.Nil should swallow: ops=%d err=%d", res.Ops, res.Errors)
	}
}

func TestMissOpAlwaysGets(t *testing.T) {
	st := &stubStore{getErr: client.ErrNotFound}
	res, err := runLoad(context.Background(), st, loadConfig{
		op: "miss", prefix: "k", keys: 8, value: []byte("v"),
		concurrency: 2, duration: 20 * time.Millisecond,
		sampleCap: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.gets.Load() == 0 || res.Ops == 0 {
		t.Fatalf("miss should Get: gets=%d ops=%d", st.gets.Load(), res.Ops)
	}
}

func TestRunLoadRespectsSampleCap(t *testing.T) {
	st := &stubStore{}
	const capN = 50
	res, err := runLoad(context.Background(), st, loadConfig{
		op: "get", prefix: "k", keys: 4, value: []byte("v"),
		concurrency: 10, duration: 30 * time.Millisecond,
		sampleCap: capN,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Samples > capN {
		t.Fatalf("samples=%d > cap=%d", res.Samples, capN)
	}
}

func TestIsNotFound(t *testing.T) {
	if !isNotFound(client.ErrNotFound) || !isNotFound(redis.Nil) {
		t.Fatal("known not-found")
	}
	if isNotFound(nil) || isNotFound(errors.New("boom")) {
		t.Fatal("other")
	}
}
