package eng

import (
	"context"

	"github.com/Code0987/supercache/pkg/store"
)

// Host is engine-owned wiring. Implemented by engine; this package does not import engine.
type Host interface {
	Store() store.Store
	KeyspaceName() string
	ExpireAt() int64
	ObserveVersion(name string, ver uint64)
	Replicate(name string, ent store.Entry)
	HasList(name string) bool
	PeerCtx(ctx context.Context) (context.Context, context.CancelFunc)
	Owner(name string) (id, addr string, isSelf bool, ok bool)
	GetOrLoad(ctx context.Context, addr, keyspace, name string) (store.Entry, bool, error)
	HoldsReplica(name string) bool
}
