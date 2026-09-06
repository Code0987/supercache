package cacheserver

import (
	"context"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
	"github.com/Code0987/supercache/internal/grpcmap"
)

func (s *Server) VAdd(ctx context.Context, req *cachev1.VAddRequest) (*cachev1.VAddResponse, error) {
	if err := s.eng.VAdd(ctx, req.GetKeyspace(), req.GetName(), req.GetMember(), req.GetVec()); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.VAddResponse{}, nil
}

func (s *Server) VRem(ctx context.Context, req *cachev1.VRemRequest) (*cachev1.VRemResponse, error) {
	if err := s.eng.VRem(ctx, req.GetKeyspace(), req.GetName(), req.GetMember()); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.VRemResponse{}, nil
}

func (s *Server) VSim(ctx context.Context, req *cachev1.VSimRequest) (*cachev1.VSimResponse, error) {
	hits, err := s.eng.VSim(ctx, req.GetKeyspace(), req.GetName(), req.GetVec(), int(req.GetK()))
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	out := make([]*cachev1.VSimHit, len(hits))
	for i, h := range hits {
		out[i] = &cachev1.VSimHit{Member: h.Member, Score: h.Score}
	}
	return &cachev1.VSimResponse{Hits: out}, nil
}

func (s *Server) VCard(ctx context.Context, req *cachev1.VCardRequest) (*cachev1.VCardResponse, error) {
	n, ok, err := s.eng.VCard(ctx, req.GetKeyspace(), req.GetName())
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.VCardResponse{N: int64(n), Present: ok}, nil
}

func (s *Server) VDim(ctx context.Context, req *cachev1.VDimRequest) (*cachev1.VDimResponse, error) {
	dim, ok, err := s.eng.VDim(ctx, req.GetKeyspace(), req.GetName())
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.VDimResponse{Dim: int32(dim), Present: ok}, nil
}

func (s *Server) VEmb(ctx context.Context, req *cachev1.VEmbRequest) (*cachev1.VEmbResponse, error) {
	vec, ok, err := s.eng.VEmb(ctx, req.GetKeyspace(), req.GetName(), req.GetMember())
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.VEmbResponse{Vec: vec, Found: ok}, nil
}
