package eng

import (
	"errors"

	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/vecset"
)

var (
	ErrRejected = errors.New("rejected")
	ErrTooLarge = errors.New("too large")
)

func ReplicateSnapshot(h Host, name string, ver uint64, expire int64) {
	ent, ok := h.Store().Peek(name)
	if !ok || !ent.IsVectorSet() || len(ent.Value) == 0 || ent.Value[0] != vecset.PrefixSnapshot {
		return
	}
	h.Replicate(name, store.Entry{
		Value:    ent.Value,
		Version:  ver,
		ExpireAt: expire,
		Flags:    store.FlagVectorSet,
	})
}

func AddLocal(h Host, name string, member []byte, vec []float32) error {
	expire := h.ExpireAt()
	cur, _ := h.Store().PeekVersion(name)
	applied, tooLarge := h.Store().VAdd(name, member, vec, cur+1, expire, h.MaxValue())
	if tooLarge {
		return ErrTooLarge
	}
	if !applied {
		return ErrRejected
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	ReplicateSnapshot(h, name, ver, expire)
	return nil
}

func RemLocal(h Host, name string, member []byte) error {
	expire := h.ExpireAt()
	cur, _ := h.Store().PeekVersion(name)
	if !h.Store().VRem(name, member, cur+1, expire) {
		return ErrRejected
	}
	if !h.Store().HasVectorSet(name) {
		return nil
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	ReplicateSnapshot(h, name, ver, expire)
	return nil
}

func ApplyInbox(h Host, name string, blob []byte, expireAt int64) bool {
	if len(blob) == 0 {
		return false
	}
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	switch blob[0] {
	case vecset.PrefixAdd:
		member, vec, err := vecset.DecodeInboxAdd(blob)
		if err != nil {
			return false
		}
		cur, _ := h.Store().PeekVersion(name)
		ok, too := h.Store().VAdd(name, member, vec, cur+1, expireAt, h.MaxValue())
		if !ok || too {
			return false
		}
	case vecset.PrefixRem:
		member, err := vecset.DecodeInboxRem(blob)
		if err != nil {
			return false
		}
		cur, _ := h.Store().PeekVersion(name)
		if !h.Store().VRem(name, member, cur+1, expireAt) {
			return false
		}
	default:
		return false
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	ReplicateSnapshot(h, name, ver, expireAt)
	return true
}

func ApplyInstall(h Host, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().VSInstall(name, blob, version, expireAt)
}
