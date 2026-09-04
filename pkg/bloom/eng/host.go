package eng

import (
	"github.com/Code0987/supercache/pkg/store"
)

// Host is engine-owned wiring for ModeBloom.
type Host interface {
	Store() store.Store
	ExpireAt() int64
	Replicate(name string, ent store.Entry)
	BloomMK() (mBits, k int)
	NextVersion(name string) uint64
}
