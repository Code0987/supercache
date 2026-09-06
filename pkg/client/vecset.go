package client

import (
	"context"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
	"github.com/Code0987/supercache/pkg/engine"
)

// VAdd inserts or replaces a member vector.
func (c *Client) VAdd(ctx context.Context, keyspace, name string, member []byte, vec []float32) error {
	_, err := c.api.VAdd(ctx, &cachev1.VAddRequest{Keyspace: keyspace, Name: name, Member: member, Vec: vec})
	return err
}

// VRem removes a member.
func (c *Client) VRem(ctx context.Context, keyspace, name string, member []byte) error {
	_, err := c.api.VRem(ctx, &cachev1.VRemRequest{Keyspace: keyspace, Name: name, Member: member})
	return err
}

// VSim is top-k neighbors. Missing name → empty, nil error.
func (c *Client) VSim(ctx context.Context, keyspace, name string, vec []float32, k int) ([]engine.VSimHit, error) {
	resp, err := c.api.VSim(ctx, &cachev1.VSimRequest{Keyspace: keyspace, Name: name, Vec: vec, K: int32(k)})
	if err != nil {
		return nil, err
	}
	hits := make([]engine.VSimHit, 0, len(resp.GetHits()))
	for _, h := range resp.GetHits() {
		hits = append(hits, engine.VSimHit{Member: h.GetMember(), Score: h.GetScore()})
	}
	return hits, nil
}

// VCard is member count. Missing → present=false.
func (c *Client) VCard(ctx context.Context, keyspace, name string) (int, bool, error) {
	resp, err := c.api.VCard(ctx, &cachev1.VCardRequest{Keyspace: keyspace, Name: name})
	if err != nil {
		return 0, false, err
	}
	if !resp.GetPresent() {
		return 0, false, nil
	}
	return int(resp.GetN()), true, nil
}

// VDim is locked dim. Missing → present=false.
func (c *Client) VDim(ctx context.Context, keyspace, name string) (int, bool, error) {
	resp, err := c.api.VDim(ctx, &cachev1.VDimRequest{Keyspace: keyspace, Name: name})
	if err != nil {
		return 0, false, err
	}
	if !resp.GetPresent() {
		return 0, false, nil
	}
	return int(resp.GetDim()), true, nil
}

// VEmb returns the stored vector. Missing → ok=false.
func (c *Client) VEmb(ctx context.Context, keyspace, name string, member []byte) ([]float32, bool, error) {
	resp, err := c.api.VEmb(ctx, &cachev1.VEmbRequest{Keyspace: keyspace, Name: name, Member: member})
	if err != nil {
		return nil, false, err
	}
	if !resp.GetFound() {
		return nil, false, nil
	}
	return resp.GetVec(), true, nil
}
