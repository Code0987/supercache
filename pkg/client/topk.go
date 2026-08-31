package client

import (
	"context"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
)

// TopKEntry is one chart row from TopKList.
type TopKEntry struct {
	Item  []byte
	Count uint64
}

// TopKAdd records one observation on a ModeTopK name (ACK-only; no changed-bool).
func (c *Client) TopKAdd(ctx context.Context, keyspace, name string, item []byte) error {
	_, err := c.api.TopKAdd(ctx, &cachev1.TopKAddRequest{Keyspace: keyspace, Name: name, Item: item})
	return err
}

// TopKList is the current chart. Missing name → ok=false, nil entries.
func (c *Client) TopKList(ctx context.Context, keyspace, name string) ([]TopKEntry, bool, error) {
	resp, err := c.api.TopKList(ctx, &cachev1.TopKListRequest{Keyspace: keyspace, Name: name})
	if err != nil {
		return nil, false, err
	}
	if !resp.GetPresent() {
		return nil, false, nil
	}
	in := resp.GetEntries()
	out := make([]TopKEntry, len(in))
	for i, e := range in {
		out[i] = TopKEntry{Item: e.GetItem(), Count: e.GetCount()}
	}
	return out, true, nil
}
