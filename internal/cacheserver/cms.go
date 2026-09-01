package cacheserver

import (
	"context"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
	"github.com/Code0987/supercache/internal/grpcmap"
)

// CMSIncr maps Cache.CMSIncr to engine.CMSIncr (ACK-only; n==0 means 1).
func (s *Server) CMSIncr(ctx context.Context, req *cachev1.CMSIncrRequest) (*cachev1.CMSIncrResponse, error) {
	if err := s.eng.CMSIncr(ctx, req.Keyspace, req.Name, req.Item, req.N); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.CMSIncrResponse{}, nil
}

// CMSQuery maps Cache.CMSQuery to engine.CMSQuery (present-bit + count).
func (s *Server) CMSQuery(ctx context.Context, req *cachev1.CMSQueryRequest) (*cachev1.CMSQueryResponse, error) {
	n, ok, err := s.eng.CMSQuery(ctx, req.Keyspace, req.Name, req.Item)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.CMSQueryResponse{Present: ok, Count: n}, nil
}
