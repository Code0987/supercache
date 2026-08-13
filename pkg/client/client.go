package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
// With no DialOptions, uses insecure credentials (local/dev only).
// Prefer DialTLS for production.
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

// DialTLS connects with TLS (and optional client certs for mTLS) to the Cache port.
// tlsCfg must not be nil. ServerName should be set when dialing by IP.
func DialTLS(ctx context.Context, addr string, tlsCfg *tls.Config, opts ...grpc.DialOption) (*Client, error) {
	if tlsCfg == nil {
		return nil, fmt.Errorf("supercache/client: tls config is required")
	}
	all := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))}
	all = append(all, opts...)
	return Dial(ctx, addr, all...)
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

// BloomAdd inserts item into a ModeBloom filter named name.
func (c *Client) BloomAdd(ctx context.Context, keyspace, name string, item []byte) error {
	_, err := c.api.BloomAdd(ctx, &cachev1.BloomAddRequest{Keyspace: keyspace, Name: name, Item: item})
	return err
}

// BloomTest reports whether item may be in the named filter.
// false means definitely not present.
func (c *Client) BloomTest(ctx context.Context, keyspace, name string, item []byte) (bool, error) {
	resp, err := c.api.BloomTest(ctx, &cachev1.BloomTestRequest{Keyspace: keyspace, Name: name, Item: item})
	if err != nil {
		return false, err
	}
	return resp.GetMaybe(), nil
}

// SetAdd inserts item into a ModeSet named name.
func (c *Client) SetAdd(ctx context.Context, keyspace, name string, item []byte) error {
	_, err := c.api.SetAdd(ctx, &cachev1.SetAddRequest{Keyspace: keyspace, Name: name, Item: item})
	return err
}

// SetRemove removes item from the named set.
func (c *Client) SetRemove(ctx context.Context, keyspace, name string, item []byte) error {
	_, err := c.api.SetRemove(ctx, &cachev1.SetRemoveRequest{Keyspace: keyspace, Name: name, Item: item})
	return err
}

// SetContains reports exact membership.
func (c *Client) SetContains(ctx context.Context, keyspace, name string, item []byte) (bool, error) {
	resp, err := c.api.SetContains(ctx, &cachev1.SetContainsRequest{Keyspace: keyspace, Name: name, Item: item})
	if err != nil {
		return false, err
	}
	return resp.GetPresent(), nil
}

// SetCard returns the number of elements (0 if missing).
func (c *Client) SetCard(ctx context.Context, keyspace, name string) (int, error) {
	resp, err := c.api.SetCard(ctx, &cachev1.SetCardRequest{Keyspace: keyspace, Name: name})
	if err != nil {
		return 0, err
	}
	return int(resp.GetCard()), nil
}

// SetMembers returns all members (defensive copies from the wire).
func (c *Client) SetMembers(ctx context.Context, keyspace, name string) ([][]byte, error) {
	resp, err := c.api.SetMembers(ctx, &cachev1.SetMembersRequest{Keyspace: keyspace, Name: name})
	if err != nil {
		return nil, err
	}
	return resp.GetMembers(), nil
}

// ZMember is a scored sorted-set element.
type ZMember struct {
	Member []byte
	Score  float64
}

// ZAdd inserts or updates member score in a ModeZSet.
func (c *Client) ZAdd(ctx context.Context, keyspace, name string, member []byte, score float64) error {
	_, err := c.api.ZAdd(ctx, &cachev1.ZAddRequest{Keyspace: keyspace, Name: name, Member: member, Score: score})
	return err
}

// ZRem removes a member from a ModeZSet.
func (c *Client) ZRem(ctx context.Context, keyspace, name string, member []byte) error {
	_, err := c.api.ZRem(ctx, &cachev1.ZRemRequest{Keyspace: keyspace, Name: name, Member: member})
	return err
}

// ZScore returns the score if the member is present.
func (c *Client) ZScore(ctx context.Context, keyspace, name string, member []byte) (score float64, ok bool, err error) {
	resp, err := c.api.ZScore(ctx, &cachev1.ZScoreRequest{Keyspace: keyspace, Name: name, Member: member})
	if err != nil {
		return 0, false, err
	}
	return resp.GetScore(), resp.GetPresent(), nil
}

// ZCard returns the number of members (0 if missing).
func (c *Client) ZCard(ctx context.Context, keyspace, name string) (int, error) {
	resp, err := c.api.ZCard(ctx, &cachev1.ZCardRequest{Keyspace: keyspace, Name: name})
	if err != nil {
		return 0, err
	}
	return int(resp.GetCard()), nil
}

// ZRange returns members by rank (Redis-style start/stop).
func (c *Client) ZRange(ctx context.Context, keyspace, name string, start, stop int) ([]ZMember, error) {
	resp, err := c.api.ZRange(ctx, &cachev1.ZRangeRequest{
		Keyspace: keyspace, Name: name, Start: int32(start), Stop: int32(stop),
	})
	if err != nil {
		return nil, err
	}
	return zMembersFromProto(resp.GetMembers()), nil
}

// ZRangeByScore returns members with min <= score <= max.
func (c *Client) ZRangeByScore(ctx context.Context, keyspace, name string, min, max float64) ([]ZMember, error) {
	resp, err := c.api.ZRangeByScore(ctx, &cachev1.ZRangeByScoreRequest{
		Keyspace: keyspace, Name: name, Min: min, Max: max,
	})
	if err != nil {
		return nil, err
	}
	return zMembersFromProto(resp.GetMembers()), nil
}

func zMembersFromProto(in []*cachev1.ZMember) []ZMember {
	if len(in) == 0 {
		return nil
	}
	out := make([]ZMember, len(in))
	for i, m := range in {
		out[i] = ZMember{Member: m.GetMember(), Score: m.GetScore()}
	}
	return out
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
