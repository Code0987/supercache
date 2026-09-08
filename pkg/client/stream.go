package client

import (
	"context"
	"fmt"
	"math"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
	"github.com/Code0987/supercache/pkg/engine"
)

func (c *Client) XAdd(ctx context.Context, keyspace, name string, payload []byte) (string, error) {
	resp, err := c.api.XAdd(ctx, &cachev1.XAddRequest{Keyspace: keyspace, Name: name, Payload: payload})
	if err != nil {
		return "", err
	}
	return resp.GetId(), nil
}

func (c *Client) XRange(ctx context.Context, keyspace, name, start, end string, count int) ([]engine.StreamEntry, error) {
	n, err := int32Arg(count, "count")
	if err != nil {
		return nil, err
	}
	resp, err := c.api.XRange(ctx, &cachev1.XRangeRequest{
		Keyspace: keyspace, Name: name, Start: start, End: end, Count: n,
	})
	if err != nil {
		return nil, err
	}
	return clientStream(resp.GetEntries()), nil
}

func (c *Client) XRevRange(ctx context.Context, keyspace, name, start, end string, count int) ([]engine.StreamEntry, error) {
	n, err := int32Arg(count, "count")
	if err != nil {
		return nil, err
	}
	resp, err := c.api.XRevRange(ctx, &cachev1.XRevRangeRequest{
		Keyspace: keyspace, Name: name, Start: start, End: end, Count: n,
	})
	if err != nil {
		return nil, err
	}
	return clientStream(resp.GetEntries()), nil
}

func (c *Client) XLen(ctx context.Context, keyspace, name string) (int, bool, error) {
	resp, err := c.api.XLen(ctx, &cachev1.XLenRequest{Keyspace: keyspace, Name: name})
	if err != nil {
		return 0, false, err
	}
	if !resp.GetPresent() {
		return 0, false, nil
	}
	return int(resp.GetN()), true, nil
}

func (c *Client) XDel(ctx context.Context, keyspace, name, id string) error {
	_, err := c.api.XDel(ctx, &cachev1.XDelRequest{Keyspace: keyspace, Name: name, Id: id})
	return err
}

func (c *Client) XTrim(ctx context.Context, keyspace, name string, maxLen int) error {
	if maxLen < 0 {
		return fmt.Errorf("%w: xtrim max_len", engine.ErrInvalidArgument)
	}
	n, err := int32Arg(maxLen, "max_len")
	if err != nil {
		return err
	}
	_, err = c.api.XTrim(ctx, &cachev1.XTrimRequest{Keyspace: keyspace, Name: name, MaxLen: n})
	return err
}

func int32Arg(n int, what string) (int32, error) {
	if n > math.MaxInt32 {
		return 0, fmt.Errorf("%w: %s", engine.ErrInvalidArgument, what)
	}
	return int32(n), nil
}

func clientStream(in []*cachev1.StreamEntry) []engine.StreamEntry {
	out := make([]engine.StreamEntry, len(in))
	for i, e := range in {
		out[i] = engine.StreamEntry{ID: e.GetId(), Payload: e.GetPayload()}
	}
	return out
}
