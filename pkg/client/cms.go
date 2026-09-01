package client

import (
	"context"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
)

// CMSIncr adds n to a ModeCMS sketch (ACK-only). n==0 means 1.
func (c *Client) CMSIncr(ctx context.Context, keyspace, name string, item []byte, n uint64) error {
	_, err := c.api.CMSIncr(ctx, &cachev1.CMSIncrRequest{Keyspace: keyspace, Name: name, Item: item, N: n})
	return err
}

// CMSQuery is min-of-d for item. Missing name → ok=false, n=0.
func (c *Client) CMSQuery(ctx context.Context, keyspace, name string, item []byte) (uint64, bool, error) {
	resp, err := c.api.CMSQuery(ctx, &cachev1.CMSQueryRequest{Keyspace: keyspace, Name: name, Item: item})
	if err != nil {
		return 0, false, err
	}
	if !resp.GetPresent() {
		return 0, false, nil
	}
	return resp.GetCount(), true, nil
}
