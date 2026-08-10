package peer

import (
	"context"
	"time"

	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/store"
)

type fanoutHint struct {
	peer    ring.Peer
	ks      string
	key     string
	ent     store.Entry
	ringGen uint64
}

type peerHintQ struct {
	order []string
	items map[string]fanoutHint
}

func hintID(ks, key string) string {
	return ks + "\x00" + key
}

func (p *FanoutPool) enqueueHint(peer ring.Peer, ks, key string, ent store.Entry, ringGen uint64) {
	if p == nil || p.hintsDisabled || p.closed.Load() || peer.Addr == "" {
		return
	}
	h := fanoutHint{
		peer: peer,
		ks:   ks,
		key:  key,
		ent: store.Entry{
			Value:    ent.CloneValue(),
			Version:  ent.Version,
			ExpireAt: ent.ExpireAt,
			Flags:    ent.Flags,
		},
		ringGen: ringGen,
	}
	id := hintID(ks, key)

	p.hintMu.Lock()
	defer p.hintMu.Unlock()
	q := p.hints[peer.Addr]
	if q == nil {
		q = &peerHintQ{items: make(map[string]fanoutHint)}
		p.hints[peer.Addr] = q
	}
	if _, exists := q.items[id]; exists {
		q.items[id] = h
		return
	}
	for len(q.order) >= p.hintMax {
		old := q.order[0]
		q.order = q.order[1:]
		delete(q.items, old)
		p.HintsDropped.Add(1)
	}
	q.order = append(q.order, id)
	q.items[id] = h
}

func (p *FanoutPool) hintLoop(ctx context.Context) {
	defer p.hintWG.Done()
	t := time.NewTicker(p.hintEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.flushHints()
		}
	}
}

func (p *FanoutPool) flushHints() {
	p.hintMu.Lock()
	addrs := make([]string, 0, len(p.hints))
	for addr := range p.hints {
		addrs = append(addrs, addr)
	}
	p.hintMu.Unlock()
	for _, addr := range addrs {
		if p.closed.Load() {
			return
		}
		p.flushPeerHints(addr)
	}
}

func (p *FanoutPool) flushPeerHints(addr string) {
	for {
		h, ok := p.peekHint(addr)
		if !ok {
			return
		}
		_, err := p.t.ApplyPut(context.Background(), addr, h.ks, h.key, h.ent, h.ringGen)
		if err != nil {
			p.t.FanoutErrors.Add(1)
			return
		}
		p.popHint(addr, hintID(h.ks, h.key))
		p.HintsFlushed.Add(1)
	}
}

func (p *FanoutPool) peekHint(addr string) (fanoutHint, bool) {
	p.hintMu.Lock()
	defer p.hintMu.Unlock()
	q := p.hints[addr]
	if q == nil || len(q.order) == 0 {
		return fanoutHint{}, false
	}
	h, ok := q.items[q.order[0]]
	if !ok {
		return fanoutHint{}, false
	}
	out := h
	out.ent.Value = h.ent.CloneValue()
	return out, true
}

func (p *FanoutPool) popHint(addr, id string) {
	p.hintMu.Lock()
	defer p.hintMu.Unlock()
	q := p.hints[addr]
	if q == nil {
		return
	}
	delete(q.items, id)
	if len(q.order) > 0 && q.order[0] == id {
		q.order = q.order[1:]
	} else {
		for i, x := range q.order {
			if x == id {
				q.order = append(q.order[:i], q.order[i+1:]...)
				break
			}
		}
	}
	if len(q.order) == 0 {
		delete(p.hints, addr)
	}
}

// HintPending returns how many distinct missed ApplyPuts are waiting.
func (p *FanoutPool) HintPending() int {
	if p == nil {
		return 0
	}
	p.hintMu.Lock()
	defer p.hintMu.Unlock()
	n := 0
	for _, q := range p.hints {
		n += len(q.items)
	}
	return n
}
