package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/stream"
	streng "github.com/Code0987/supercache/pkg/stream/eng"
)

// StreamEntry is one XRange row (clients must not import pkg/stream).
type StreamEntry struct {
	ID      string
	Payload []byte
}

func (e *Engine) sxNeed(ks *ksRuntime, name string) error {
	if err := e.validateKey(ks.cfg.Name, name); err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeStream {
		return fmt.Errorf("%w: stream op requires ModeStream", ErrInvalidArgument)
	}
	return e.validateKeyLen(ks, name)
}

func (h modeHost) StreamMaxLen() int { return h.ks.cfg.EffectiveStreamMaxLen() }

func (h modeHost) HasStream(name string) bool { return h.ks.store.HasStream(name) }

func (h modeHost) StreamAddRemote(ctx context.Context, addr, keyspace, name string, payload []byte) (string, error) {
	c := h.e.clusterSnapshot()
	if c == nil || c.Transport == nil {
		return "", fmt.Errorf("%w: no transport", ErrUnavailable)
	}
	return c.Transport.StreamAdd(ctx, addr, keyspace, name, payload)
}

func (e *Engine) XAdd(ctx context.Context, keyspaceName, name string, payload []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return "", err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return "", err
	}
	if err := e.sxNeed(ks, name); err != nil {
		return "", err
	}
	if err := e.validateValueSize(ks, payload); err != nil {
		return "", err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return "", fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			pctx, cancel := e.peerCtx(ctx, ks)
			defer cancel()
			return c.Transport.StreamAdd(pctx, owner.Addr, ks.cfg.Name, name, payload)
		}
	}
	id, err := streng.AddLocal(e.modeHost(ks), name, payload)
	if err == streng.ErrTooLarge {
		return "", ErrValueTooLarge
	}
	if err == streng.ErrFull {
		return "", fmt.Errorf("%w: stream full", ErrInvalidArgument)
	}
	if err != nil {
		return "", fmt.Errorf("%w: xadd rejected", ErrInvalidArgument)
	}
	return id, nil
}

func (e *Engine) XDel(ctx context.Context, keyspaceName, name, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if err := e.sxNeed(ks, name); err != nil {
		return err
	}
	if _, _, perr := stream.ParseID(id); perr != nil {
		return fmt.Errorf("%w: bad stream id", ErrInvalidArgument)
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.sxInboxViaOwner(ctx, ks, name, stream.EncodeInboxDel(id))
		}
	}
	if err := streng.DelLocal(e.modeHost(ks), name, id); err == streng.ErrBadID {
		return fmt.Errorf("%w: bad stream id", ErrInvalidArgument)
	} else if err != nil {
		return fmt.Errorf("%w: xdel rejected", ErrInvalidArgument)
	}
	return nil
}

func (e *Engine) XTrim(ctx context.Context, keyspaceName, name string, maxLen int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if err := e.sxNeed(ks, name); err != nil {
		return err
	}
	if maxLen < 0 {
		return fmt.Errorf("%w: stream trim", ErrInvalidArgument)
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.sxInboxViaOwner(ctx, ks, name, stream.EncodeInboxTrim(maxLen))
		}
	}
	if err := streng.TrimLocal(e.modeHost(ks), name, maxLen); err != nil {
		return fmt.Errorf("%w: xtrim rejected", ErrInvalidArgument)
	}
	return nil
}

func (e *Engine) sxInboxViaOwner(ctx context.Context, ks *ksRuntime, name string, blob []byte) error {
	c := e.clusterSnapshot()
	owner, _ := c.Ring.Owner(name)
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	ent := store.Entry{Value: blob, Flags: store.FlagStream}
	applied, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("%w: stream mutate rejected", ErrInvalidArgument)
	}
	return nil
}

func (e *Engine) XRange(ctx context.Context, keyspaceName, name, start, end string, count int) ([]StreamEntry, error) {
	return e.sxRange(ctx, keyspaceName, name, start, end, count, false)
}

func (e *Engine) XRevRange(ctx context.Context, keyspaceName, name, start, end string, count int) ([]StreamEntry, error) {
	return e.sxRange(ctx, keyspaceName, name, start, end, count, true)
}

func (e *Engine) sxRange(ctx context.Context, keyspaceName, name, start, end string, count int, rev bool) ([]StreamEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, err
	}
	if err := e.sxNeed(ks, name); err != nil {
		return nil, err
	}
	if _, err := stream.ParseBound(start, true); err != nil {
		return nil, fmt.Errorf("%w: bad stream id", ErrInvalidArgument)
	}
	if _, err := stream.ParseBound(end, false); err != nil {
		return nil, fmt.Errorf("%w: bad stream id", ErrInvalidArgument)
	}
	rows, ok, err := ks.store.XRange(name, start, end, count, rev)
	if err != nil {
		return nil, fmt.Errorf("%w: bad stream id", ErrInvalidArgument)
	}
	if ok {
		return copyStream(rows), nil
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		ent, found, ferr := streng.FetchOwner(ctx, e.modeHost(ks), name)
		if ferr != nil {
			return nil, ferr
		}
		if found {
			lg, decErr := stream.DecodeSnapshot(ent.Value)
			if decErr != nil {
				return nil, nil
			}
			sb, _ := stream.ParseBound(start, true)
			eb, _ := stream.ParseBound(end, false)
			raw := lg.Range(sb, eb, count, rev)
			out := make([]StreamEntry, len(raw))
			for i, r := range raw {
				out[i] = StreamEntry{ID: r.ID(), Payload: r.Payload}
			}
			return out, nil
		}
	}
	return nil, nil
}

func (e *Engine) XLen(ctx context.Context, keyspaceName, name string) (int, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return 0, false, err
	}
	if err := e.sxNeed(ks, name); err != nil {
		return 0, false, err
	}
	n, ok := ks.store.XLen(name)
	if ok {
		return n, true, nil
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		ent, found, ferr := streng.FetchOwner(ctx, e.modeHost(ks), name)
		if ferr != nil {
			return 0, false, ferr
		}
		if found {
			lg, decErr := stream.DecodeSnapshot(ent.Value)
			if decErr != nil {
				return 0, false, nil
			}
			return len(lg.Entries), true, nil
		}
	}
	return 0, false, nil
}

func (e *Engine) applyStream(ks *ksRuntime, name string, ent store.Entry) bool {
	if len(ent.Value) == 0 {
		return false
	}
	switch ent.Value[0] {
	case stream.PrefixAdd, stream.PrefixDel, stream.PrefixTrim:
		c := e.clusterSnapshot()
		if c != nil && c.Ring != nil {
			if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
				return false
			}
		}
		return streng.ApplyInbox(e.modeHost(ks), name, ent.Value, ent.ExpireAt)
	case stream.PrefixSnapshot:
		return streng.ApplyInstall(e.modeHost(ks), name, ent.Value, ent.Version, ent.ExpireAt)
	default:
		return false
	}
}

func copyStream(in []store.StreamEntry) []StreamEntry {
	out := make([]StreamEntry, len(in))
	for i, e := range in {
		out[i] = StreamEntry{ID: e.ID, Payload: e.Payload}
	}
	return out
}
