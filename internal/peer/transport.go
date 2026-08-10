package peer

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	peerv1 "github.com/Code0987/supercache/api/gen/peer/v1"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/store"
)

// Transport is a client pool for peer RPCs.
type Transport struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	timeout time.Duration
	tls     *tls.Config // nil = insecure (dev only)

	FanoutErrors  atomic.Uint64
	FanoutDropped atomic.Uint64
}

// TransportOption configures NewTransport.
type TransportOption func(*Transport)

// WithTLS enables TLS (and optional mTLS client certs) for peer dials.
// The config is cloned per dial so ServerName can be set from the target address when empty.
func WithTLS(cfg *tls.Config) TransportOption {
	return func(t *Transport) {
		if cfg != nil {
			t.tls = cfg.Clone()
		}
	}
}

// NewTransport creates a peer client pool.
func NewTransport(timeout time.Duration, opts ...TransportOption) *Transport {
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	t := &Transport{
		conns:   make(map[string]*grpc.ClientConn),
		timeout: timeout,
	}
	for _, o := range opts {
		o(t)
	}
	return t
}

// Timeout returns the per-peer RPC timeout.
func (t *Transport) Timeout() time.Duration {
	if t == nil || t.timeout <= 0 {
		return 500 * time.Millisecond
	}
	return t.timeout
}

// rpcContext applies the transport default timeout only when the parent context
// has no deadline. Callers (e.g. engine with keyspace.PeerTimeout) can set a
// tighter or looser bound on ctx and it will be respected.
func (t *Transport) rpcContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, t.Timeout())
}

// Close closes all connections.
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var first error
	for addr, c := range t.conns {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
		delete(t.conns, addr)
	}
	return first
}

func (t *Transport) client(addr string) (peerv1.PeerClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[addr]; ok {
		return peerv1.NewPeerClient(c), nil
	}
	var creds credentials.TransportCredentials
	if t.tls != nil {
		cfg := t.tls.Clone()
		if cfg.ServerName == "" {
			// Derive SNI from dial target host when not set explicitly.
			if host, _, err := splitHostPort(addr); err == nil {
				cfg.ServerName = host
			}
		}
		creds = credentials.NewTLS(cfg)
	} else {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	t.conns[addr] = conn
	return peerv1.NewPeerClient(conn), nil
}

func splitHostPort(addr string) (host, port string, err error) {
	return net.SplitHostPort(addr)
}

// ApplyPut sends ApplyPut to addr.
func (t *Transport) ApplyPut(ctx context.Context, addr, keyspace, key string, ent store.Entry, ringGen uint64) (bool, error) {
	cli, err := t.client(addr)
	if err != nil {
		return false, err
	}
	ctx, cancel := t.rpcContext(ctx)
	defer cancel()
	resp, err := cli.ApplyPut(ctx, &peerv1.ApplyPutRequest{
		Keyspace:       keyspace,
		Key:            key,
		RingGeneration: ringGen,
		Entry: &peerv1.Entry{
			Value:            ent.Value,
			Version:          ent.Version,
			ExpireAtUnixNano: ent.ExpireAt,
			Flags:            ent.Flags,
		},
	})
	if err != nil {
		return false, err
	}
	return resp.GetApplied(), nil
}

// ForwardPut forwards a Put to the owner.
// hopCount is the number of prior forwards (0 = first hop from non-owner client path).
func (t *Transport) ForwardPut(ctx context.Context, addr, keyspace, key string, value []byte, ttlNanos int64, ttlSet bool, hopCount uint32) error {
	cli, err := t.client(addr)
	if err != nil {
		return err
	}
	ctx, cancel := t.rpcContext(ctx)
	defer cancel()
	_, err = cli.ForwardPut(ctx, &peerv1.ForwardPutRequest{
		Keyspace: keyspace,
		Key:      key,
		Value:    value,
		TtlNanos: ttlNanos,
		TtlSet:   ttlSet,
		HopCount: hopCount,
	})
	return err
}

// FanoutConfig controls async ApplyPut fan-out.
type FanoutConfig struct {
	Workers   int
	QueueSize int
	// DisableHints skips remembering failed ApplyPuts for later replay.
	DisableHints bool
	// HintMaxPerPeer caps distinct (keyspace, key) hints per peer (0 = 4096).
	HintMaxPerPeer int
	// HintRetryInterval is how often to flush hints (0 = 100ms).
	HintRetryInterval time.Duration
}

// FanoutPool async-fans ApplyPut to peers. Failed ApplyPuts are queued as
// bounded per-peer hints and replayed until the peer accepts them (or the hint
// is dropped under cap). Put ACK stays non-blocking.
type FanoutPool struct {
	t      *Transport
	jobs   chan fanoutJob
	wg     sync.WaitGroup
	closed atomic.Bool

	hintsDisabled bool
	hintMax       int
	hintEvery     time.Duration
	hintCancel    context.CancelFunc
	hintWG        sync.WaitGroup
	hintMu        sync.Mutex
	hints         map[string]*peerHintQ // addr -> queue

	HintsFlushed atomic.Uint64
	HintsDropped atomic.Uint64
}

type fanoutJob struct {
	peers   []ring.Peer
	ks      string
	key     string
	ent     store.Entry
	ringGen uint64
}

// NewFanoutPool starts workers.
func NewFanoutPool(t *Transport, cfg FanoutConfig) *FanoutPool {
	if cfg.Workers <= 0 {
		cfg.Workers = 32
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 10_000
	}
	hintMax := cfg.HintMaxPerPeer
	if hintMax <= 0 {
		hintMax = 4096
	}
	hintEvery := cfg.HintRetryInterval
	if hintEvery <= 0 {
		hintEvery = 100 * time.Millisecond
	}
	p := &FanoutPool{
		t:             t,
		jobs:          make(chan fanoutJob, cfg.QueueSize),
		hintsDisabled: cfg.DisableHints,
		hintMax:       hintMax,
		hintEvery:     hintEvery,
		hints:         make(map[string]*peerHintQ),
	}
	for i := 0; i < cfg.Workers; i++ {
		p.wg.Add(1)
		go p.loop()
	}
	if !p.hintsDisabled {
		ctx, cancel := context.WithCancel(context.Background())
		p.hintCancel = cancel
		p.hintWG.Add(1)
		go p.hintLoop(ctx)
	}
	return p
}

func (p *FanoutPool) loop() {
	defer p.wg.Done()
	for job := range p.jobs {
		p.applyJob(job)
	}
}

// applyJob fans ApplyPut to all peers concurrently.
func (p *FanoutPool) applyJob(job fanoutJob) {
	var wg sync.WaitGroup
	for _, peer := range job.peers {
		if peer.Addr == "" {
			continue
		}
		wg.Add(1)
		go func(peer ring.Peer) {
			defer wg.Done()
			// Copy entry bytes per peer so concurrent RPC marshaling is safe.
			ent := store.Entry{
				Value:    job.ent.CloneValue(),
				Version:  job.ent.Version,
				ExpireAt: job.ent.ExpireAt,
				Flags:    job.ent.Flags,
			}
			_, err := p.t.ApplyPut(context.Background(), peer.Addr, job.ks, job.key, ent, job.ringGen)
			if err != nil {
				p.t.FanoutErrors.Add(1)
				p.enqueueHint(peer, job.ks, job.key, ent, job.ringGen)
			}
		}(peer)
	}
	wg.Wait()
}

// Submit queues a fan-out. Drops if full (no retry of the submit itself).
func (p *FanoutPool) Submit(peers []ring.Peer, keyspace, key string, ent store.Entry, ringGen uint64) {
	if p == nil || p.closed.Load() {
		return
	}
	job := fanoutJob{peers: peers, ks: keyspace, key: key, ent: ent, ringGen: ringGen}
	select {
	case p.jobs <- job:
	default:
		p.t.FanoutDropped.Add(1)
	}
}

// Close stops workers and the hint flusher.
func (p *FanoutPool) Close() {
	if p == nil || !p.closed.CompareAndSwap(false, true) {
		return
	}
	close(p.jobs)
	p.wg.Wait()
	if p.hintCancel != nil {
		p.hintCancel()
	}
	p.hintWG.Wait()
}

// FormatAddr joins host port for errors.
func FormatAddr(p ring.Peer) string {
	return fmt.Sprintf("%s(%s)", p.ID, p.Addr)
}

// ApplyDelete sends ApplyDelete to addr.
func (t *Transport) ApplyDelete(ctx context.Context, addr, keyspace, key string, deleteVersion, ringGen uint64) (bool, error) {
	cli, err := t.client(addr)
	if err != nil {
		return false, err
	}
	ctx, cancel := t.rpcContext(ctx)
	defer cancel()
	resp, err := cli.ApplyDelete(ctx, &peerv1.ApplyDeleteRequest{
		Keyspace:       keyspace,
		Key:            key,
		DeleteVersion:  deleteVersion,
		RingGeneration: ringGen,
	})
	if err != nil {
		return false, err
	}
	return resp.GetApplied(), nil
}

// ForwardDelete asks the owner to coordinate a cluster delete.
// Uses a longer deadline than a single peer RPC so the owner can fan out ApplyDelete.
func (t *Transport) ForwardDelete(ctx context.Context, addr, keyspace, key string) ([]PeerFailure, error) {
	cli, err := t.client(addr)
	if err != nil {
		return nil, err
	}
	// Budget: several per-peer RTTs (owner parallelizes ApplyDelete).
	budget := t.Timeout() * 8
	if budget < 2*time.Second {
		budget = 2 * time.Second
	}
	if budget > 15*time.Second {
		budget = 15 * time.Second
	}
	// Respect parent deadline when tighter.
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	resp, err := cli.ForwardDelete(ctx, &peerv1.ForwardDeleteRequest{
		Keyspace: keyspace,
		Key:      key,
	})
	if err != nil {
		return nil, err
	}
	out := make([]PeerFailure, 0, len(resp.GetFailures()))
	for _, f := range resp.GetFailures() {
		out = append(out, PeerFailure{PeerID: f.GetPeerId(), Message: f.GetMessage()})
	}
	return out, nil
}

// PeerFailure is a remote delete failure.
type PeerFailure struct {
	PeerID  string
	Message string
}

// GetOrLoadResult is the owner fill response.
type GetOrLoadResult struct {
	Found bool
	Entry store.Entry
}

// GetOrLoad asks the owner to serve or load a key.
func (t *Transport) GetOrLoad(ctx context.Context, addr, keyspace, key string) (GetOrLoadResult, error) {
	cli, err := t.client(addr)
	if err != nil {
		return GetOrLoadResult{}, err
	}
	ctx, cancel := t.rpcContext(ctx)
	defer cancel()
	resp, err := cli.GetOrLoad(ctx, &peerv1.GetOrLoadRequest{
		Keyspace: keyspace,
		Key:      key,
	})
	if err != nil {
		return GetOrLoadResult{}, err
	}
	res := GetOrLoadResult{Found: resp.GetFound()}
	if resp.GetEntry() != nil {
		res.Entry = store.Entry{
			Value:    resp.Entry.Value,
			Version:  resp.Entry.Version,
			ExpireAt: resp.Entry.ExpireAtUnixNano,
			Flags:    resp.Entry.Flags,
		}
	}
	return res, nil
}
