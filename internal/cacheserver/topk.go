package cacheserver

import (
	"context"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
	"github.com/Code0987/supercache/internal/grpcmap"
	"github.com/Code0987/supercache/pkg/engine"
)

// TopKAdd maps Cache.TopKAdd to engine.TopKAdd (ACK-only).
func (s *Server) TopKAdd(ctx context.Context, req *cachev1.TopKAddRequest) (*cachev1.TopKAddResponse, error) {
	if err := s.eng.TopKAdd(ctx, req.Keyspace, req.Name, req.Item); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.TopKAddResponse{}, nil
}

// TopKList maps Cache.TopKList to engine.TopKList (present-bit + rows).
func (s *Server) TopKList(ctx context.Context, req *cachev1.TopKListRequest) (*cachev1.TopKListResponse, error) {
	rows, ok, err := s.eng.TopKList(ctx, req.Keyspace, req.Name)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.TopKListResponse{Present: ok, Entries: engineTopKToProto(rows)}, nil
}

func engineTopKToProto(in []engine.TopKEntry) []*cachev1.TopKEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]*cachev1.TopKEntry, len(in))
	for i, e := range in {
		out[i] = &cachev1.TopKEntry{Item: e.Item, Count: e.Count}
	}
	return out
}
