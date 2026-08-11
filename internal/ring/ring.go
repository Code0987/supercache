package ring

import (
	"hash/fnv"
	"sort"
	"sync"
)

// Peer is a ring member identity + address for peer RPCs.
type Peer struct {
	ID   string
	Addr string // gRPC peer listen address host:port
}

// Ring is a consistent hash ring with virtual nodes.
type Ring struct {
	mu     sync.RWMutex
	vnodes int
	// sorted hashes
	keys     []uint64
	hashToID map[uint64]string
	peers    map[string]Peer // id -> peer
	gen      uint64
}

// New creates a ring. vnodes is virtual nodes per peer (default 64 if <=0).
func New(vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = 64
	}
	return &Ring{
		vnodes:   vnodes,
		hashToID: make(map[uint64]string),
		peers:    make(map[string]Peer),
	}
}

// Generation returns the membership generation.
func (r *Ring) Generation() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.gen
}

// SetPeers replaces the peer set and bumps generation.
func (r *Ring) SetPeers(peers []Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = make(map[string]Peer, len(peers))
	r.hashToID = make(map[uint64]string)
	r.keys = r.keys[:0]
	for _, p := range peers {
		if p.ID == "" {
			continue
		}
		r.peers[p.ID] = p
		for i := 0; i < r.vnodes; i++ {
			h := hashString(p.ID + "#" + itoa(i))
			r.hashToID[h] = p.ID
			r.keys = append(r.keys, h)
		}
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
	r.gen++
}

// Owner returns the peer that owns key, or zero Peer if empty ring.
func (r *Ring) Owner(key string) (Peer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.keys) == 0 {
		return Peer{}, false
	}
	h := hashString(key)
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if i == len(r.keys) {
		i = 0
	}
	id := r.hashToID[r.keys[i]]
	p, ok := r.peers[id]
	return p, ok
}

// Peers returns a snapshot of all peers.
func (r *Ring) Peers() []Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Peer, 0, len(r.peers))
	for _, p := range r.peers {
		out = append(out, p)
	}
	return out
}

// PeersExcept returns all peers except id.
func (r *Ring) PeersExcept(id string) []Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Peer, 0, len(r.peers))
	for _, p := range r.peers {
		if p.ID == id {
			continue
		}
		out = append(out, p)
	}
	return out
}

// Replicas returns up to n distinct peers for key, owner first, then clockwise
// successors on the vnode ring. n<=0 yields nil. n>=peer count yields every peer.
func (r *Ring) Replicas(key string, n int) []Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.replicasLocked(key, n)
}

// ReplicasExcept is Replicas with id removed.
func (r *Ring) ReplicasExcept(key, id string, n int) []Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reps := r.replicasLocked(key, n)
	if id == "" || len(reps) == 0 {
		return reps
	}
	out := make([]Peer, 0, len(reps))
	for _, p := range reps {
		if p.ID != id {
			out = append(out, p)
		}
	}
	return out
}

// IsReplica reports whether id is among the first n replicas for key.
func (r *Ring) IsReplica(key, id string, n int) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if id == "" || n <= 0 || len(r.keys) == 0 {
		return false
	}
	if n >= len(r.peers) {
		_, ok := r.peers[id]
		return ok
	}
	for _, p := range r.replicasLocked(key, n) {
		if p.ID == id {
			return true
		}
	}
	return false
}

func (r *Ring) replicasLocked(key string, n int) []Peer {
	if n <= 0 || len(r.keys) == 0 || len(r.peers) == 0 {
		return nil
	}
	if n > len(r.peers) {
		n = len(r.peers)
	}
	h := hashString(key)
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if i == len(r.keys) {
		i = 0
	}
	out := make([]Peer, 0, n)
	seen := make(map[string]struct{}, n)
	for walked := 0; walked < len(r.keys) && len(out) < n; walked++ {
		id := r.hashToID[r.keys[i]]
		i++
		if i == len(r.keys) {
			i = 0
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, r.peers[id])
	}
	return out
}

// Len returns peer count.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.peers)
}

func hashString(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
