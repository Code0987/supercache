package eng

import (
	"errors"

	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/stream"
)

var (
	ErrRejected = errors.New("rejected")
	ErrTooLarge = errors.New("too large")
	ErrFull     = errors.New("full")
	ErrBadID    = errors.New("bad id")
)

func ReplicateSnapshot(h Host, name string, ver uint64, expire int64) {
	ent, ok := h.Store().Peek(name)
	if !ok || !ent.IsStream() || len(ent.Value) == 0 || ent.Value[0] != stream.PrefixSnapshot {
		return
	}
	h.Replicate(name, store.Entry{
		Value:    ent.Value,
		Version:  ver,
		ExpireAt: expire,
		Flags:    store.FlagStream,
	})
}

func AddLocal(h Host, name string, payload []byte) (string, error) {
	expire := h.ExpireAt()
	cur, _ := h.Store().PeekVersion(name)
	id, applied, tooLarge, full := h.Store().XAdd(name, payload, cur+1, expire, h.MaxValue(), h.StreamMaxLen())
	if tooLarge {
		return "", ErrTooLarge
	}
	if full {
		return "", ErrFull
	}
	if !applied {
		return "", ErrRejected
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	ReplicateSnapshot(h, name, ver, expire)
	return id, nil
}

func DelLocal(h Host, name, id string) error {
	if _, _, err := stream.ParseID(id); err != nil {
		return ErrBadID
	}
	expire := h.ExpireAt()
	cur, _ := h.Store().PeekVersion(name)
	if !h.Store().XDel(name, id, cur+1, expire) {
		return ErrRejected
	}
	if !h.Store().HasStream(name) {
		return nil
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	ReplicateSnapshot(h, name, ver, expire)
	return nil
}

func TrimLocal(h Host, name string, maxLen int) error {
	if maxLen < 0 {
		return ErrBadID
	}
	expire := h.ExpireAt()
	cur, _ := h.Store().PeekVersion(name)
	if !h.Store().XTrim(name, maxLen, cur+1, expire) {
		return ErrRejected
	}
	if !h.Store().HasStream(name) {
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
	case stream.PrefixAdd:
		payload, err := stream.DecodeInboxAdd(blob)
		if err != nil {
			return false
		}
		_, err = AddLocal(h, name, payload)
		return err == nil
	case stream.PrefixDel:
		id, err := stream.DecodeInboxDel(blob)
		if err != nil {
			return false
		}
		return DelLocal(h, name, id) == nil
	case stream.PrefixTrim:
		n, err := stream.DecodeInboxTrim(blob)
		if err != nil {
			return false
		}
		return TrimLocal(h, name, n) == nil
	default:
		return false
	}
}

func ApplyInstall(h Host, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().SXInstall(name, blob, version, expireAt)
}
