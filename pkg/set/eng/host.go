package eng

import (
	"context"

	"github.com/Code0987/supercache/pkg/store"
)

type Host interface {
	Store() store.Store
	ExpireAt() int64
	ObserveVersion(name string, ver uint64)
	Replicate(name string, ent store.Entry)
	HasSet(name string) bool
	NextVersion(name string) uint64
	PeerCtx(ctx context.Context) (context.Context, context.CancelFunc)
	Owner(name string) (id, addr string, isSelf bool, ok bool)
	GetOrLoad(ctx context.Context, addr, keyspace, name string) (store.Entry, bool, error)
	HoldsReplica(name string) bool
	KeyspaceName() string
}
