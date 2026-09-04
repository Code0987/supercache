package eng

import (
	"errors"

	"github.com/Code0987/supercache/pkg/set"
	"github.com/Code0987/supercache/pkg/store"
)

var ErrRejected = errors.New("rejected")

func AddLocal(h Host, name string, item []byte, fanout bool) error {
	ver := h.NextVersion(name)
	expire := h.ExpireAt()
	if !h.Store().SetAdd(name, item, ver, expire) {
		return ErrRejected
	}
	if fanout {
		h.Replicate(name, store.Entry{
			Value:    append([]byte(nil), item...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagSetAdd,
		})
	}
	return nil
}

func RemoveLocal(h Host, name string, item []byte, fanout bool) error {
	ver := h.NextVersion(name)
	expire := h.ExpireAt()
	if !h.Store().SetRemove(name, item, ver, expire) {
		if !h.HasSet(name) {
			return nil
		}
		return ErrRejected
	}
	if fanout {
		h.Replicate(name, store.Entry{
			Value:    append([]byte(nil), item...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagSetRemove,
		})
	}
	return nil
}

func ApplyAdd(h Host, name string, item []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().SetAdd(name, item, version, expireAt)
}

func ApplyRemove(h Host, name string, item []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().SetRemove(name, item, version, expireAt)
}

func ApplyInstall(h Host, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().SetInstall(name, blob, version, expireAt)
}

func ContainsBlob(blob, item []byte) bool {
	s, err := set.DecodeSet(blob)
	if err != nil {
		return false
	}
	return s.Contains(item)
}

func CardBlob(blob []byte) int {
	s, err := set.DecodeSet(blob)
	if err != nil {
		return 0
	}
	return s.Len()
}

func MembersBlob(blob []byte) [][]byte {
	s, err := set.DecodeSet(blob)
	if err != nil {
		return nil
	}
	return s.Members()
}
