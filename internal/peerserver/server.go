package peerserver

import (
	"context"
	"errors"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	peerv1 "github.com/Code0987/supercache/api/gen/peer/v1"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/store"
)

// Server implements peerv1.PeerServer against an Engine.
type Server struct {
	peerv1.UnimplementedPeerServer
	eng *engine.Engine
}

// NewServer wraps eng.
func NewServer(eng *engine.Engine) *Server {
	return &Server{eng: eng}
}

// ApplyPut implements Peer.
func (s *Server) ApplyPut(ctx context.Context, req *peerv1.ApplyPutRequest) (*peerv1.ApplyPutResponse, error) {
	ent := store.Entry{}
	if req.Entry != nil {
		ent = store.Entry{
			Value:    req.Entry.Value,
			Version:  req.Entry.Version,
			ExpireAt: req.Entry.ExpireAtUnixNano,
			Flags:    req.Entry.Flags,
		}
	}
	// ring_generation is diagnostic: LWW by version remains the apply gate.
	applied, err := s.eng.ApplyPutWithRingGen(req.Keyspace, req.Key, ent, req.RingGeneration)
	if err != nil {
		return nil, mapPeerErr(err)
	}
	return &peerv1.ApplyPutResponse{Applied: applied}, nil
}

// ApplyDelete implements Peer.
func (s *Server) ApplyDelete(ctx context.Context, req *peerv1.ApplyDeleteRequest) (*peerv1.ApplyDeleteResponse, error) {
	applied, err := s.eng.ApplyDeleteWithRingGen(req.Keyspace, req.Key, req.DeleteVersion, req.RingGeneration)
	if err != nil {
		return nil, mapPeerErr(err)
	}
	return &peerv1.ApplyDeleteResponse{Applied: applied}, nil
}

// ForwardPut implements Peer — owner-local Put without re-forward.
func (s *Server) ForwardPut(ctx context.Context, req *peerv1.ForwardPutRequest) (*peerv1.ForwardPutResponse, error) {
	var opts []engine.PutOption
	if req.TtlSet {
		opts = append(opts, engine.WithTTL(time.Duration(req.TtlNanos)))
	}
	if err := s.eng.PutLocal(ctx, req.Keyspace, req.Key, req.Value, opts...); err != nil {
		return nil, mapPeerErr(err)
	}
	return &peerv1.ForwardPutResponse{}, nil
}

// ForwardDelete implements Peer — owner coordinates cluster delete.
func (s *Server) ForwardDelete(ctx context.Context, req *peerv1.ForwardDeleteRequest) (*peerv1.ForwardDeleteResponse, error) {
	err := s.eng.DeleteAsOwner(ctx, req.Keyspace, req.Key)
	resp := &peerv1.ForwardDeleteResponse{}
	if err == nil {
		return resp, nil
	}
	var me *engine.MultiError
	if errors.As(err, &me) {
		for _, pe := range me.Errors {
			resp.Failures = append(resp.Failures, &peerv1.PeerFailure{
				PeerId:  pe.PeerID,
				Message: pe.Err.Error(),
			})
		}
		return resp, nil
	}
	return nil, err
}

// GetOrLoad implements Peer (owner fill path, no re-forward).
func (s *Server) GetOrLoad(ctx context.Context, req *peerv1.GetOrLoadRequest) (*peerv1.GetOrLoadResponse, error) {
	ent, err := s.eng.GetOrLoadLocal(ctx, req.Keyspace, req.Key)
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			// Include negative envelope so peers install same version (PLAN §9.1).
			resp := &peerv1.GetOrLoadResponse{Found: false}
			if ent.IsNegative() || ent.Version > 0 {
				resp.Entry = &peerv1.Entry{
					Value:            ent.Value,
					Version:          ent.Version,
					ExpireAtUnixNano: ent.ExpireAt,
					Flags:            ent.Flags,
				}
			}
			return resp, nil
		}
		return nil, mapPeerErr(err)
	}
	return &peerv1.GetOrLoadResponse{
		Found: true,
		Entry: &peerv1.Entry{
			Value:            ent.Value,
			Version:          ent.Version,
			ExpireAtUnixNano: ent.ExpireAt,
			Flags:            ent.Flags,
		},
	}, nil
}

// ListenAndServe starts a gRPC server on addr (e.g. ":9001").
// Pass grpc.Creds(credentials.NewTLS(cfg)) for TLS/mTLS; omit for plaintext (dev only).
func ListenAndServe(addr string, eng *engine.Engine, opts ...grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	gs := grpc.NewServer(opts...)
	peerv1.RegisterPeerServer(gs, NewServer(eng))
	go func() { _ = gs.Serve(lis) }()
	return gs, lis, nil
}

func mapPeerErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, engine.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, engine.ErrKeyspaceNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, engine.ErrInvalidArgument),
		errors.Is(err, engine.ErrKeyTooLarge),
		errors.Is(err, engine.ErrValueTooLarge):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, engine.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
