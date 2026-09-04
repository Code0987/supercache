package eng

import (
	"errors"

	"github.com/Code0987/supercache/pkg/store"
)

var ErrRejected = errors.New("rejected")

func AddLocal(h Host, name string, item []byte, fanout bool) error {
	ver := uint64(1)
	if cur, ok := h.Store().Peek(name); ok {
		ver = h.NextVersion(name)
		if cur.IsBloom() && !cur.IsTombstone() {
			ver = cur.Version
		}
	}
	expire := h.ExpireAt()
	m, k := h.BloomMK()
	if !h.Store().BloomAdd(name, item, m, k, ver, expire) {
		return ErrRejected
	}
	if fanout {
		h.Replicate(name, store.Entry{
			Value:    append([]byte(nil), item...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagBloomAdd,
		})
	}
	return nil
}

func ApplyAdd(h Host, name string, item []byte, version uint64, expireAt int64) bool {
	m, k := h.BloomMK()
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().BloomAdd(name, item, m, k, version, expireAt)
}

func ApplyMerge(h Host, name string, bits []byte, version uint64, expireAt int64) bool {
	m, k := h.BloomMK()
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().BloomMerge(name, bits, m, k, version, expireAt)
}
