package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
)

// ErrNotFound means the key is absent (or negative-cached on the server).
var ErrNotFound = errors.New("supercache/client: not found")

// Client is a remote SuperCache application client.
//
// Consistency notes (eventual):
//   - Put returns after the key's owner has accepted the write (server-side).
//   - A subsequent Get against a different node may lag until fan-out (ms typical).
//   - This client does not keep a process-local sticky buffer for read-your-writes;
//     dial the same node or accept brief staleness (see PLAN / docs).
type Client struct {
	conn *grpc.ClientConn
	api  cachev1.CacheClient
}

// Dial connects to a cache node application port (not the Peer mesh port).
func Dial(ctx context.Context, addr string, opts ...grpc.DialOption) (*Client, error) {
	_ = ctx // reserved for future dial deadlines via caller-provided opts
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, api: cachev1.NewCacheClient(conn)}, nil
}

// Close closes the connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Get fetches a key. Returns ErrNotFound when missing.
func (c *Client) Get(ctx context.Context, keyspace, key string) ([]byte, error) {
	resp, err := c.api.Get(ctx, &cachev1.GetRequest{Keyspace: keyspace, Key: key})
	if err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, ErrNotFound
	}
	return resp.Value, nil
}

// PutOption configures Put.
type PutOption func(*putOpts)

type putOpts struct {
	ttl    time.Duration
	ttlSet bool
}

// WithTTL sets an explicit TTL for this Put (0 = no expiry).
func WithTTL(d time.Duration) PutOption {
	return func(o *putOpts) {
		o.ttl = d
		o.ttlSet = true
	}
}

// Put stores a value.
func (c *Client) Put(ctx context.Context, keyspace, key string, value []byte, opts ...PutOption) error {
	po := putOpts{}
	for _, o := range opts {
		o(&po)
	}
	_, err := c.api.Put(ctx, &cachev1.PutRequest{
		Keyspace: keyspace,
		Key:      key,
		Value:    value,
		TtlNanos: int64(po.ttl),
		TtlSet:   po.ttlSet,
	})
	return err
}

// KV is a batch put item.
type KV struct {
	Key   string
	Value []byte
}

// PutMany puts many keys (not atomic). Partial failures return KeyErrors.
func (c *Client) PutMany(ctx context.Context, keyspace string, items []KV, opts ...PutOption) error {
	po := putOpts{}
	for _, o := range opts {
		o(&po)
	}
	req := &cachev1.PutManyRequest{
		Keyspace: keyspace,
		TtlNanos: int64(po.ttl),
		TtlSet:   po.ttlSet,
	}
	for _, it := range items {
		req.Items = append(req.Items, &cachev1.KV{Key: it.Key, Value: it.Value})
	}
	resp, err := c.api.PutMany(ctx, req)
	if err != nil {
		return err
	}
	if len(resp.Errors) == 0 {
		return nil
	}
	return keyErrorsFromProto(resp.Errors)
}

// Delete removes a key cluster-wide (best effort). Peer failures return PeerFailures.
func (c *Client) Delete(ctx context.Context, keyspace, key string) error {
	resp, err := c.api.Delete(ctx, &cachev1.DeleteRequest{Keyspace: keyspace, Key: key})
	if err != nil {
		return err
	}
	if len(resp.PeerFailures) == 0 {
		return nil
	}
	return peerFailuresFromProto(resp.PeerFailures)
}

// DeleteMany deletes many keys (not atomic).
func (c *Client) DeleteMany(ctx context.Context, keyspace string, keys []string) error {
	resp, err := c.api.DeleteMany(ctx, &cachev1.DeleteManyRequest{Keyspace: keyspace, Keys: keys})
	if err != nil {
		return err
	}
	if len(resp.Errors) == 0 {
		return nil
	}
	return keyErrorsFromProto(resp.Errors)
}

// KeyError is a per-key batch failure.
type KeyError struct {
	Key          string
	Message      string
	PeerFailures PeerFailures // set for DeleteMany when peers fail ApplyDelete
}

func (e KeyError) Error() string {
	if len(e.PeerFailures) > 0 {
		return fmt.Sprintf("key %q: %s (%s)", e.Key, e.Message, e.PeerFailures.Error())
	}
	return fmt.Sprintf("key %q: %s", e.Key, e.Message)
}

// KeyErrors is a list of per-key failures.
type KeyErrors []KeyError

func (e KeyErrors) Error() string {
	return fmt.Sprintf("%d key error(s)", len(e))
}

// PeerFailure is a cluster delete peer failure.
type PeerFailure struct {
	PeerID  string
	Message string
}

func (e PeerFailure) Error() string {
	return fmt.Sprintf("peer %s: %s", e.PeerID, e.Message)
}

// PeerFailures aggregates delete peer failures.
type PeerFailures []PeerFailure

func (e PeerFailures) Error() string {
	return fmt.Sprintf("%d peer failure(s)", len(e))
}

func keyErrorsFromProto(in []*cachev1.KeyError) error {
	out := make(KeyErrors, 0, len(in))
	for _, e := range in {
		ke := KeyError{Key: e.Key, Message: e.Message}
		if len(e.PeerFailures) > 0 {
			ke.PeerFailures = make(PeerFailures, 0, len(e.PeerFailures))
			for _, pf := range e.PeerFailures {
				ke.PeerFailures = append(ke.PeerFailures, PeerFailure{
					PeerID:  pf.PeerId,
					Message: pf.Message,
				})
			}
		}
		out = append(out, ke)
	}
	return out
}

func peerFailuresFromProto(in []*cachev1.PeerFailure) error {
	out := make(PeerFailures, 0, len(in))
	for _, e := range in {
		out = append(out, PeerFailure{PeerID: e.PeerId, Message: e.Message})
	}
	return out
}
