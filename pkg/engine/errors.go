package engine

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Code0987/supercache/pkg/datasource"
)

var (
	// ErrNotFound is returned when the key is absent (or negative-cached).
	// It wraps datasource.ErrNotFound so callers (e.g. warmup) can use errors.Is
	// without importing this package and creating a cycle.
	ErrNotFound = fmt.Errorf("supercache: not found: %w", datasource.ErrNotFound)
	// ErrKeyspaceNotFound means the named keyspace is not registered.
	ErrKeyspaceNotFound = errors.New("supercache: keyspace not found")
	// ErrUnavailable means the load path is rate-limited or circuit-open.
	ErrUnavailable = errors.New("supercache: unavailable")
	// ErrInvalidArgument is a caller error (limits, empty key, etc.).
	ErrInvalidArgument = errors.New("supercache: invalid argument")
	// ErrValueTooLarge means value exceeds MaxValueSize.
	ErrValueTooLarge = errors.New("supercache: value too large")
	// ErrKeyTooLarge means key exceeds MaxKeyLen.
	ErrKeyTooLarge = errors.New("supercache: key too large")
	// ErrBatchTooLarge means PutMany/DeleteMany exceeded max batch.
	ErrBatchTooLarge = errors.New("supercache: batch too large")
)

// PeerError is a failure talking to one peer (cluster mode).
type PeerError struct {
	PeerID string
	Op     string
	Err    error
}

func (e PeerError) Error() string {
	return fmt.Sprintf("peer %s %s: %v", e.PeerID, e.Op, e.Err)
}

func (e PeerError) Unwrap() error { return e.Err }

// MultiError aggregates peer failures (e.g. Delete).
type MultiError struct {
	Errors []PeerError
}

func (m *MultiError) Error() string {
	if m == nil || len(m.Errors) == 0 {
		return "supercache: multi-error (empty)"
	}
	parts := make([]string, len(m.Errors))
	for i, e := range m.Errors {
		parts[i] = e.Error()
	}
	return "supercache: " + strings.Join(parts, "; ")
}

func (m *MultiError) Unwrap() []error {
	if m == nil {
		return nil
	}
	out := make([]error, len(m.Errors))
	for i := range m.Errors {
		out[i] = m.Errors[i]
	}
	return out
}

// KeyError is a per-key failure inside PutMany/DeleteMany.
type KeyError struct {
	Key string
	Err error
}

func (e KeyError) Error() string {
	return fmt.Sprintf("key %q: %v", e.Key, e.Err)
}

func (e KeyError) Unwrap() error { return e.Err }
