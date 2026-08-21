package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/jsonx"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

// JsonSet upserts JSON value at path on a ModeJSON document.
func (e *Engine) JsonSet(ctx context.Context, keyspaceName, name, path string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeJSON {
		return fmt.Errorf("%w: JsonSet requires ModeJSON", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(path) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	if _, err := jsonx.ParsePath(path); err != nil {
		return fmt.Errorf("%w: invalid json path", ErrInvalidArgument)
	}
	if err := e.validateValueSize(ks, value); err != nil {
		return err
	}
	if _, err := jsonx.Decode(value); err != nil {
		return fmt.Errorf("%w: invalid json", ErrInvalidArgument)
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.jMutViaOwner(ctx, ks, name, jsonx.EncodeSet(path, value), store.FlagJSONSet)
		}
	}
	return e.jSetLocal(ks, name, path, value)
}

// JsonGet returns a copy of the JSON at path. Missing doc or path → ok=false.
func (e *Engine) JsonGet(ctx context.Context, keyspaceName, name, path string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return nil, false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, false, err
	}
	if ks.cfg.Mode != keyspace.ModeJSON {
		return nil, false, fmt.Errorf("%w: JsonGet requires ModeJSON", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, false, err
	}
	if len(path) > e.maxKeyLen {
		return nil, false, ErrKeyTooLarge
	}
	if _, err := jsonx.ParsePath(path); err != nil {
		return nil, false, fmt.Errorf("%w: invalid json path", ErrInvalidArgument)
	}
	if ks.store.HasJSON(name) {
		v, ok := ks.store.JGet(name, path)
		return v, ok, nil
	}
	ent, found, err := e.jFetchOwner(ctx, ks, name)
	if err != nil || !found {
		return nil, false, err
	}
	return jExtract(ent.Value, path)
}

// JsonDel removes the node at path. Missing name or path is a no-op.
func (e *Engine) JsonDel(ctx context.Context, keyspaceName, name, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeJSON {
		return fmt.Errorf("%w: JsonDel requires ModeJSON", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(path) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	if _, err := jsonx.ParsePath(path); err != nil {
		return fmt.Errorf("%w: invalid json path", ErrInvalidArgument)
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.jMutViaOwner(ctx, ks, name, []byte(path), store.FlagJSONDel)
		}
	}
	return e.jDelLocal(ks, name, path)
}

func (e *Engine) jMutViaOwner(ctx context.Context, ks *ksRuntime, name string, value []byte, flag uint32) error {
	c := e.clusterSnapshot()
	owner, _ := c.Ring.Owner(name)
	ent := store.Entry{Value: value, Flags: flag, Version: 1}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	applied, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("%w: json mutate rejected", ErrInvalidArgument)
	}
	return nil
}

func (e *Engine) jSetLocal(ks *ksRuntime, name, path string, value []byte) error {
	expire := e.expireAt(ks.cfg.TTL)
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	applied, tooLarge := ks.store.JSet(name, path, value, gate, expire, max)
	if tooLarge {
		return ErrValueTooLarge
	}
	if !applied {
		return fmt.Errorf("%w: json set rejected", ErrInvalidArgument)
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.jReplicateSnapshot(ks, name, ver, expire)
	return nil
}

func (e *Engine) jDelLocal(ks *ksRuntime, name, path string) error {
	expire := e.expireAt(ks.cfg.TTL)
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	ok, mutated := ks.store.JDel(name, path, gate, expire)
	if !ok {
		return fmt.Errorf("%w: json del rejected", ErrInvalidArgument)
	}
	if !mutated {
		return nil
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.jReplicateSnapshot(ks, name, ver, expire)
	return nil
}

func (e *Engine) jReplicateSnapshot(ks *ksRuntime, name string, ver uint64, expire int64) {
	ent, ok := ks.store.Peek(name)
	if !ok || !ent.IsJSON() {
		return
	}
	e.replicate(ks.cfg.Name, name, store.Entry{
		Value:    ent.Value,
		Version:  ver,
		ExpireAt: expire,
		Flags:    store.FlagJSON,
	}, false)
}

func (e *Engine) jFetchOwner(ctx context.Context, ks *ksRuntime, name string) (store.Entry, bool, error) {
	c := e.clusterSnapshot()
	if c == nil || c.Ring == nil || c.Transport == nil {
		return store.Entry{}, false, nil
	}
	owner, ok := c.Ring.Owner(name)
	if !ok || owner.ID == "" || owner.ID == c.SelfID || owner.Addr == "" {
		return store.Entry{}, false, nil
	}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	res, err := c.Transport.GetOrLoad(pctx, owner.Addr, ks.cfg.Name, name)
	if err != nil || !res.Found || !res.Entry.IsJSON() {
		return store.Entry{}, false, nil
	}
	if _, decErr := jsonx.Decode(res.Entry.Value); decErr != nil {
		return store.Entry{}, false, nil
	}
	if e.holdsReplica(c, ks, name) {
		_ = ks.store.JInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
	}
	return res.Entry, true, nil
}

func (e *Engine) applyJSONSet(ks *ksRuntime, name string, inbox []byte, expireAt int64) bool {
	path, raw, err := jsonx.DecodeSet(inbox)
	if err != nil {
		return false
	}
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	ok, tooLarge := ks.store.JSet(name, path, raw, gate, expireAt, max)
	if !ok || tooLarge {
		return false
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.jReplicateSnapshot(ks, name, ver, expireAt)
	return true
}

func (e *Engine) applyJSONDel(ks *ksRuntime, name string, path []byte, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	ok, mutated := ks.store.JDel(name, string(path), gate, expireAt)
	if !ok {
		return false
	}
	if !mutated {
		return true
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.jReplicateSnapshot(ks, name, ver, expireAt)
	return true
}

func (e *Engine) applyJSONInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.JInstall(name, blob, version, expireAt)
}

func jExtract(blob []byte, path string) ([]byte, bool, error) {
	doc, err := jsonx.Decode(blob)
	if err != nil {
		return nil, false, nil
	}
	p, err := jsonx.ParsePath(path)
	if err != nil {
		return nil, false, nil
	}
	node, ok := jsonx.Get(doc, p)
	if !ok {
		return nil, false, nil
	}
	out, err := jsonx.Encode(node)
	if err != nil {
		return nil, false, nil
	}
	return out, true, nil
}
