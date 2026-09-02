package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/hashx"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

const (
	errEmptyHashField      = "%w: empty hash field"
	errHSetRequiresMode    = "%w: HSet requires ModeHash"
	errHDelRequiresMode    = "%w: HDel requires ModeHash"
	errHGetRequiresMode    = "%w: HGet requires ModeHash"
	errHExistsRequiresMode = "%w: HExists requires ModeHash"
	errHLenRequiresMode    = "%w: HLen requires ModeHash"
	errHGetAllRequiresMode = "%w: HGetAll requires ModeHash"
	errHSetRejected        = "%w: hset rejected"
	errHDelRejected        = "%w: hdel rejected"
	errOwnerNoAddress      = "%w: owner %s has no address"
)

// HashField is one field/value pair returned by HGetAll.
type HashField struct {
	Field []byte
	Value []byte
}

func (e *Engine) checkHashField(field []byte) error {
	if len(field) == 0 {
		return fmt.Errorf(errEmptyHashField, ErrInvalidArgument)
	}
	if len(field) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	return nil
}

// HSet upserts a field in a ModeHash map.
func (e *Engine) HSet(ctx context.Context, keyspaceName, name string, field, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	if err := e.checkHashField(field); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeHash {
		return fmt.Errorf(errHSetRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if err := e.validateValueSize(ks, value); err != nil {
		return err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf(errOwnerNoAddress, ErrUnavailable, owner.ID)
			}
			return e.hMutViaOwner(ctx, ks, name, hashx.EncodeSet(field, value), store.FlagHashSet)
		}
	}
	return e.hSetLocal(ks, name, field, value, true)
}

// HDel removes a field from a ModeHash map.
func (e *Engine) HDel(ctx context.Context, keyspaceName, name string, field []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	if err := e.checkHashField(field); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeHash {
		return fmt.Errorf(errHDelRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf(errOwnerNoAddress, ErrUnavailable, owner.ID)
			}
			return e.hMutViaOwner(ctx, ks, name, append([]byte(nil), field...), store.FlagHashDel)
		}
	}
	return e.hDelLocal(ks, name, field, true)
}

// HGet returns a copy of the field value.
func (e *Engine) HGet(ctx context.Context, keyspaceName, name string, field []byte) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return nil, false, err
	}
	if err := e.checkHashField(field); err != nil {
		return nil, false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, false, err
	}
	if ks.cfg.Mode != keyspace.ModeHash {
		return nil, false, fmt.Errorf(errHGetRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, false, err
	}
	if v, ok := ks.store.HGet(name, field); ok {
		return v, true, nil
	}
	if e.hasHashLocal(ks, name) {
		return nil, false, nil
	}
	ent, found, err := e.hFetchOwner(ctx, ks, name)
	if err != nil || !found {
		return nil, false, err
	}
	h, err := hashx.Decode(ent.Value)
	if err != nil {
		return nil, false, nil
	}
	v, ok := h.Get(field)
	return v, ok, nil
}

// HExists reports whether field is present.
func (e *Engine) HExists(ctx context.Context, keyspaceName, name string, field []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return false, err
	}
	if err := e.checkHashField(field); err != nil {
		return false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return false, err
	}
	if ks.cfg.Mode != keyspace.ModeHash {
		return false, fmt.Errorf(errHExistsRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return false, err
	}
	if e.hasHashLocal(ks, name) {
		return ks.store.HExists(name, field), nil
	}
	ent, found, err := e.hFetchOwner(ctx, ks, name)
	if err != nil || !found {
		return false, err
	}
	h, err := hashx.Decode(ent.Value)
	if err != nil {
		return false, nil
	}
	return h.Exists(field), nil
}

// HLen returns the number of fields.
func (e *Engine) HLen(ctx context.Context, keyspaceName, name string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return 0, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return 0, err
	}
	if ks.cfg.Mode != keyspace.ModeHash {
		return 0, fmt.Errorf(errHLenRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, err
	}
	if e.hasHashLocal(ks, name) {
		return ks.store.HLen(name), nil
	}
	ent, ok, err := e.hFetchOwner(ctx, ks, name)
	if err != nil || !ok {
		return 0, err
	}
	h, err := hashx.Decode(ent.Value)
	if err != nil {
		return 0, nil
	}
	return h.Len(), nil
}

// HGetAll returns all field/value pairs in field-byte order.
func (e *Engine) HGetAll(ctx context.Context, keyspaceName, name string) ([]HashField, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return nil, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, err
	}
	if ks.cfg.Mode != keyspace.ModeHash {
		return nil, fmt.Errorf(errHGetAllRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, err
	}
	if e.hasHashLocal(ks, name) {
		return toEngineHashFields(ks.store.HGetAll(name)), nil
	}
	ent, ok, err := e.hFetchOwner(ctx, ks, name)
	if err != nil || !ok {
		return nil, err
	}
	h, err := hashx.Decode(ent.Value)
	if err != nil {
		return nil, nil
	}
	return toEngineHashFieldsFromPkg(h.All()), nil
}

func (e *Engine) hasHashLocal(ks *ksRuntime, name string) bool {
	return ks.store.HasHash(name)
}

func toEngineHashFields(in []store.HashField) []HashField {
	if len(in) == 0 {
		return nil
	}
	out := make([]HashField, len(in))
	for i, f := range in {
		out[i] = HashField{Field: f.Field, Value: f.Value}
	}
	return out
}

func toEngineHashFieldsFromPkg(in []hashx.Field) []HashField {
	if len(in) == 0 {
		return nil
	}
	out := make([]HashField, len(in))
	for i, f := range in {
		out[i] = HashField{Field: f.Field, Value: f.Value}
	}
	return out
}
