package cacheserver

import (
	"context"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
	"github.com/Code0987/supercache/internal/grpcmap"
	"github.com/Code0987/supercache/pkg/engine"
)

func (s *Server) XAdd(ctx context.Context, req *cachev1.XAddRequest) (*cachev1.XAddResponse, error) {
	id, err := s.eng.XAdd(ctx, req.GetKeyspace(), req.GetName(), req.GetPayload())
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.XAddResponse{Id: id}, nil
}

func (s *Server) XRange(ctx context.Context, req *cachev1.XRangeRequest) (*cachev1.XRangeResponse, error) {
	rows, err := s.eng.XRange(ctx, req.GetKeyspace(), req.GetName(), req.GetStart(), req.GetEnd(), int(req.GetCount()))
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.XRangeResponse{Entries: streamEntries(rows)}, nil
}

func (s *Server) XRevRange(ctx context.Context, req *cachev1.XRevRangeRequest) (*cachev1.XRangeResponse, error) {
	rows, err := s.eng.XRevRange(ctx, req.GetKeyspace(), req.GetName(), req.GetStart(), req.GetEnd(), int(req.GetCount()))
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.XRangeResponse{Entries: streamEntries(rows)}, nil
}

func (s *Server) XLen(ctx context.Context, req *cachev1.XLenRequest) (*cachev1.XLenResponse, error) {
	n, ok, err := s.eng.XLen(ctx, req.GetKeyspace(), req.GetName())
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.XLenResponse{N: int64(n), Present: ok}, nil
}

func (s *Server) XDel(ctx context.Context, req *cachev1.XDelRequest) (*cachev1.XDelResponse, error) {
	if err := s.eng.XDel(ctx, req.GetKeyspace(), req.GetName(), req.GetId()); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.XDelResponse{}, nil
}

func (s *Server) XTrim(ctx context.Context, req *cachev1.XTrimRequest) (*cachev1.XTrimResponse, error) {
	if err := s.eng.XTrim(ctx, req.GetKeyspace(), req.GetName(), int(req.GetMaxLen())); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.XTrimResponse{}, nil
}

func streamEntries(rows []engine.StreamEntry) []*cachev1.StreamEntry {
	out := make([]*cachev1.StreamEntry, len(rows))
	for i, r := range rows {
		out[i] = &cachev1.StreamEntry{Id: r.ID, Payload: r.Payload}
	}
	return out
}
