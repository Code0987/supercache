package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/jsonx"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

const (
	errJSONSetRequiresMode = "%w: JsonSet requires ModeJSON"
	errJSONGetRequiresMode = "%w: JsonGet requires ModeJSON"
	errJSONDelRequiresMode = "%w: JsonDel requires ModeJSON"
	errInvalidJSONPath     = "%w: invalid json path"
	errInvalidJSON         = "%w: invalid json"
	errJSONMutateRejected  = "%w: json mutate rejected"
	errJSONSetRejected     = "%w: json set rejected"
	errJSONDelRejected     = "%w: json del rejected"
	errOwnerNoAddress      = "%w: owner %s has no address"
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
		return fmt.Errorf(errJSONSetRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(path) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	if _, err := jsonx.ParsePath(path); err != nil {
		return fmt.Errorf(errInvalidJSONPath, ErrInvalidArgument)
	}
	if err := e.validateValueSize(ks, value); err != nil {
		return err
	}
	if _, err := jsonx.Decode(value); err != nil {
		return fmt.Errorf(errInvalidJSON, ErrInvalidArgument)
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf(errOwnerNoAddress, ErrUnavailable, owner.ID)
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
		return nil, false, fmt.Errorf(errJSONGetRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, false, err
	}
	if len(path) > e.maxKeyLen {
		return nil, false, ErrKeyTooLarge
	}
	if _, err := jsonx.ParsePath(path); err != nil {
		return nil, false, fmt.Errorf(errInvalidJSONPath, ErrInvalidArgument)
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
		return fmt.Errorf(errJSONDelRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(path) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	if _, err := jsonx.ParsePath(path); err != nil {
		return fmt.Errorf(errInvalidJSONPath, ErrInvalidArgument)
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf(errOwnerNoAddress, ErrUnavailable, owner.ID)
			}
			return e.jMutViaOwner(ctx, ks, name, []byte(path), store.FlagJSONDel)
		}
	}
	return e.jDelLocal(ks, name, path)
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
