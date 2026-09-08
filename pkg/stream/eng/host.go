package eng

import (
	"context"

	"github.com/Code0987/supercache/pkg/store"
)

// Host is the engine-owned wiring stream needs (no engine import).
type Host interface {
	Store() store.Store
	KeyspaceName() string
	ExpireAt() int64
	MaxValue() int
	StreamMaxLen() int
	ObserveVersion(name string, ver uint64)
	Replicate(name string, ent store.Entry)
	PeerCtx(ctx context.Context) (context.Context, context.CancelFunc)
	Owner(name string) (id, addr string, isSelf bool, ok bool)
	GetOrLoad(ctx context.Context, addr, keyspace, name string) (store.Entry, bool, error)
	HoldsReplica(name string) bool
	StreamAddRemote(ctx context.Context, addr, keyspace, name string, payload []byte) (id string, err error)
}
