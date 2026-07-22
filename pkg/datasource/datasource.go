package datasource

import (
	"context"
	"errors"
)

// ErrNotFound means the backend has no value for the key.
var ErrNotFound = errors.New("datasource: not found")

// DataSource loads values for LoadThrough keyspaces on cache miss.
type DataSource interface {
	Load(ctx context.Context, key string) ([]byte, error)
}

// Func adapts a function to DataSource.
type Func func(ctx context.Context, key string) ([]byte, error)

func (f Func) Load(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

// Map is an in-memory DataSource for tests.
type Map map[string][]byte

func (m Map) Load(_ context.Context, key string) ([]byte, error) {
	v, ok := m[key]
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}
