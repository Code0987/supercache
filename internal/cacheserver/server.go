package cacheserver

import (
	"context"
	"errors"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
	"github.com/Code0987/supercache/pkg/engine"
)

// Server implements the application-facing Cache gRPC API.
type Server struct {
	cachev1.UnimplementedCacheServer
	eng *engine.Engine
}

// New wraps eng.
func New(eng *engine.Engine) *Server {
	return &Server{eng: eng}
}

func (s *Server) Get(ctx context.Context, req *cachev1.GetRequest) (*cachev1.GetResponse, error) {
	val, err := s.eng.Get(ctx, req.Keyspace, req.Key)
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return &cachev1.GetResponse{Found: false}, nil
		}
		return nil, mapErr(err)
	}
	return &cachev1.GetResponse{Found: true, Value: val}, nil
}

func (s *Server) Put(ctx context.Context, req *cachev1.PutRequest) (*cachev1.PutResponse, error) {
	var opts []engine.PutOption
	if req.TtlSet {
		opts = append(opts, engine.WithTTL(time.Duration(req.TtlNanos)))
	}
	if err := s.eng.Put(ctx, req.Keyspace, req.Key, req.Value, opts...); err != nil {
		return nil, mapErr(err)
	}
	return &cachev1.PutResponse{}, nil
}

func (s *Server) PutMany(ctx context.Context, req *cachev1.PutManyRequest) (*cachev1.PutManyResponse, error) {
	kvs := make([]engine.KV, 0, len(req.Items))
	for _, it := range req.Items {
		kvs = append(kvs, engine.KV{Key: it.Key, Value: it.Value})
	}
	var opts []engine.PutOption
	if req.TtlSet {
		opts = append(opts, engine.WithTTL(time.Duration(req.TtlNanos)))
	}
	err := s.eng.PutMany(ctx, req.Keyspace, kvs, opts...)
	resp := &cachev1.PutManyResponse{}
	if err == nil {
		return resp, nil
	}
	// Flatten KeyError / joined errors into response when possible.
	var ke engine.KeyError
	if errors.As(err, &ke) {
		resp.Errors = append(resp.Errors, &cachev1.KeyError{Key: ke.Key, Message: ke.Err.Error()})
		return resp, nil
	}
	// errors.Join
	type multi interface{ Unwrap() []error }
	if m, ok := err.(multi); ok {
		for _, e := range m.Unwrap() {
			var k engine.KeyError
			if errors.As(e, &k) {
				resp.Errors = append(resp.Errors, &cachev1.KeyError{Key: k.Key, Message: k.Err.Error()})
			} else {
				resp.Errors = append(resp.Errors, &cachev1.KeyError{Message: e.Error()})
			}
		}
		return resp, nil
	}
	return nil, mapErr(err)
}

func (s *Server) Delete(ctx context.Context, req *cachev1.DeleteRequest) (*cachev1.DeleteResponse, error) {
	err := s.eng.Delete(ctx, req.Keyspace, req.Key)
	resp := &cachev1.DeleteResponse{}
	if err == nil {
		return resp, nil
	}
	var me *engine.MultiError
	if errors.As(err, &me) {
		for _, pe := range me.Errors {
			resp.PeerFailures = append(resp.PeerFailures, &cachev1.PeerFailure{
				PeerId:  pe.PeerID,
				Message: pe.Err.Error(),
			})
		}
		return resp, nil
	}
	return nil, mapErr(err)
}

func (s *Server) DeleteMany(ctx context.Context, req *cachev1.DeleteManyRequest) (*cachev1.DeleteManyResponse, error) {
	err := s.eng.DeleteMany(ctx, req.Keyspace, req.Keys)
	resp := &cachev1.DeleteManyResponse{}
	if err == nil {
		return resp, nil
	}
	var ke engine.KeyError
	if errors.As(err, &ke) {
		resp.Errors = append(resp.Errors, keyErrorToProto(ke))
		return resp, nil
	}
	type multi interface{ Unwrap() []error }
	if m, ok := err.(multi); ok {
		for _, e := range m.Unwrap() {
			var k engine.KeyError
			if errors.As(e, &k) {
				resp.Errors = append(resp.Errors, keyErrorToProto(k))
			} else {
				resp.Errors = append(resp.Errors, &cachev1.KeyError{Message: e.Error()})
			}
		}
		return resp, nil
	}
	return nil, mapErr(err)
}

// keyErrorToProto maps engine.KeyError, preserving MultiError peer failures.
func keyErrorToProto(ke engine.KeyError) *cachev1.KeyError {
	out := &cachev1.KeyError{Key: ke.Key, Message: ke.Err.Error()}
	var me *engine.MultiError
	if errors.As(ke.Err, &me) {
		for _, pe := range me.Errors {
			msg := ""
			if pe.Err != nil {
				msg = pe.Err.Error()
			}
			out.PeerFailures = append(out.PeerFailures, &cachev1.PeerFailure{
				PeerId:  pe.PeerID,
				Message: msg,
			})
		}
	}
	return out
}

func mapErr(err error) error {
	switch {
	case errors.Is(err, engine.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, engine.ErrKeyspaceNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, engine.ErrInvalidArgument),
		errors.Is(err, engine.ErrKeyTooLarge),
		errors.Is(err, engine.ErrValueTooLarge),
		errors.Is(err, engine.ErrBatchTooLarge):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, engine.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// ListenAndServe starts the Cache gRPC API on addr.
// Pass grpc.Creds(credentials.NewTLS(cfg)) for TLS; omit for plaintext (dev only).
func ListenAndServe(addr string, eng *engine.Engine, opts ...grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	gs := grpc.NewServer(opts...)
	cachev1.RegisterCacheServer(gs, New(eng))
	go func() { _ = gs.Serve(lis) }()
	return gs, lis, nil
}
