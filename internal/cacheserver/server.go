package cacheserver

import (
	"context"
	"errors"
	"net"
	"time"

	"google.golang.org/grpc"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
	"github.com/Code0987/supercache/internal/grpcmap"
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
		return nil, grpcmap.Status(err)
	}
	return &cachev1.GetResponse{Found: true, Value: val}, nil
}

func (s *Server) Put(ctx context.Context, req *cachev1.PutRequest) (*cachev1.PutResponse, error) {
	var opts []engine.PutOption
	if req.TtlSet {
		opts = append(opts, engine.WithTTL(time.Duration(req.TtlNanos)))
	}
	if err := s.eng.Put(ctx, req.Keyspace, req.Key, req.Value, opts...); err != nil {
		return nil, grpcmap.Status(err)
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
	// Prefer multi unwrap first: errors.As finds only the first KeyError in a Join.
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
	var ke engine.KeyError
	if errors.As(err, &ke) {
		resp.Errors = append(resp.Errors, &cachev1.KeyError{Key: ke.Key, Message: ke.Err.Error()})
		return resp, nil
	}
	return nil, grpcmap.Status(err)
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
	return nil, grpcmap.Status(err)
}

func (s *Server) DeleteMany(ctx context.Context, req *cachev1.DeleteManyRequest) (*cachev1.DeleteManyResponse, error) {
	err := s.eng.DeleteMany(ctx, req.Keyspace, req.Keys)
	resp := &cachev1.DeleteManyResponse{}
	if err == nil {
		return resp, nil
	}
	// Prefer multi unwrap first: errors.As finds only the first KeyError in a Join.
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
	var ke engine.KeyError
	if errors.As(err, &ke) {
		resp.Errors = append(resp.Errors, keyErrorToProto(ke))
		return resp, nil
	}
	return nil, grpcmap.Status(err)
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

func (s *Server) BloomAdd(ctx context.Context, req *cachev1.BloomAddRequest) (*cachev1.BloomAddResponse, error) {
	if err := s.eng.BloomAdd(ctx, req.Keyspace, req.Name, req.Item); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.BloomAddResponse{}, nil
}

func (s *Server) BloomTest(ctx context.Context, req *cachev1.BloomTestRequest) (*cachev1.BloomTestResponse, error) {
	maybe, err := s.eng.BloomTest(ctx, req.Keyspace, req.Name, req.Item)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.BloomTestResponse{Maybe: maybe}, nil
}

func (s *Server) SetAdd(ctx context.Context, req *cachev1.SetAddRequest) (*cachev1.SetAddResponse, error) {
	if err := s.eng.SetAdd(ctx, req.Keyspace, req.Name, req.Item); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.SetAddResponse{}, nil
}

func (s *Server) SetRemove(ctx context.Context, req *cachev1.SetRemoveRequest) (*cachev1.SetRemoveResponse, error) {
	if err := s.eng.SetRemove(ctx, req.Keyspace, req.Name, req.Item); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.SetRemoveResponse{}, nil
}

func (s *Server) SetContains(ctx context.Context, req *cachev1.SetContainsRequest) (*cachev1.SetContainsResponse, error) {
	present, err := s.eng.SetContains(ctx, req.Keyspace, req.Name, req.Item)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.SetContainsResponse{Present: present}, nil
}

func (s *Server) SetCard(ctx context.Context, req *cachev1.SetCardRequest) (*cachev1.SetCardResponse, error) {
	n, err := s.eng.SetCard(ctx, req.Keyspace, req.Name)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.SetCardResponse{Card: int64(n)}, nil
}

func (s *Server) SetMembers(ctx context.Context, req *cachev1.SetMembersRequest) (*cachev1.SetMembersResponse, error) {
	mem, err := s.eng.SetMembers(ctx, req.Keyspace, req.Name)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.SetMembersResponse{Members: mem}, nil
}

func (s *Server) ZAdd(ctx context.Context, req *cachev1.ZAddRequest) (*cachev1.ZAddResponse, error) {
	if err := s.eng.ZAdd(ctx, req.Keyspace, req.Name, req.Member, req.Score); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.ZAddResponse{}, nil
}

func (s *Server) ZRem(ctx context.Context, req *cachev1.ZRemRequest) (*cachev1.ZRemResponse, error) {
	if err := s.eng.ZRem(ctx, req.Keyspace, req.Name, req.Member); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.ZRemResponse{}, nil
}

func (s *Server) ZScore(ctx context.Context, req *cachev1.ZScoreRequest) (*cachev1.ZScoreResponse, error) {
	sc, present, err := s.eng.ZScore(ctx, req.Keyspace, req.Name, req.Member)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.ZScoreResponse{Present: present, Score: sc}, nil
}

func (s *Server) ZCard(ctx context.Context, req *cachev1.ZCardRequest) (*cachev1.ZCardResponse, error) {
	n, err := s.eng.ZCard(ctx, req.Keyspace, req.Name)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.ZCardResponse{Card: int64(n)}, nil
}

func (s *Server) ZRange(ctx context.Context, req *cachev1.ZRangeRequest) (*cachev1.ZRangeResponse, error) {
	mem, err := s.eng.ZRange(ctx, req.Keyspace, req.Name, int(req.Start), int(req.Stop))
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.ZRangeResponse{Members: zMembersToProto(mem)}, nil
}

func (s *Server) ZRangeByScore(ctx context.Context, req *cachev1.ZRangeByScoreRequest) (*cachev1.ZRangeResponse, error) {
	mem, err := s.eng.ZRangeByScore(ctx, req.Keyspace, req.Name, req.Min, req.Max)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.ZRangeResponse{Members: zMembersToProto(mem)}, nil
}

func (s *Server) GeoAdd(ctx context.Context, req *cachev1.GeoAddRequest) (*cachev1.GeoAddResponse, error) {
	if err := s.eng.GeoAdd(ctx, req.Keyspace, req.Name, req.Member, req.Lon, req.Lat); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.GeoAddResponse{}, nil
}

func (s *Server) GeoRem(ctx context.Context, req *cachev1.GeoRemRequest) (*cachev1.GeoRemResponse, error) {
	if err := s.eng.GeoRem(ctx, req.Keyspace, req.Name, req.Member); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.GeoRemResponse{}, nil
}

func (s *Server) GeoPos(ctx context.Context, req *cachev1.GeoPosRequest) (*cachev1.GeoPosResponse, error) {
	lon, lat, present, err := s.eng.GeoPos(ctx, req.Keyspace, req.Name, req.Member)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.GeoPosResponse{Present: present, Lon: lon, Lat: lat}, nil
}

func (s *Server) GeoCard(ctx context.Context, req *cachev1.GeoCardRequest) (*cachev1.GeoCardResponse, error) {
	n, err := s.eng.GeoCard(ctx, req.Keyspace, req.Name)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.GeoCardResponse{Card: int64(n)}, nil
}

func (s *Server) GeoDist(ctx context.Context, req *cachev1.GeoDistRequest) (*cachev1.GeoDistResponse, error) {
	m, present, err := s.eng.GeoDist(ctx, req.Keyspace, req.Name, req.A, req.B)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.GeoDistResponse{Present: present, Meters: m}, nil
}

func (s *Server) GeoRadius(ctx context.Context, req *cachev1.GeoRadiusRequest) (*cachev1.GeoRadiusResponse, error) {
	mem, err := s.eng.GeoRadius(ctx, req.Keyspace, req.Name, req.Lon, req.Lat, req.RadiusMeters, int(req.Limit))
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.GeoRadiusResponse{Members: geoMembersToProto(mem)}, nil
}

func (s *Server) LPush(ctx context.Context, req *cachev1.LPushRequest) (*cachev1.LPushResponse, error) {
	if err := s.eng.LPush(ctx, req.Keyspace, req.Name, req.Item); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.LPushResponse{}, nil
}

func (s *Server) RPush(ctx context.Context, req *cachev1.RPushRequest) (*cachev1.RPushResponse, error) {
	if err := s.eng.RPush(ctx, req.Keyspace, req.Name, req.Item); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.RPushResponse{}, nil
}

func (s *Server) LPop(ctx context.Context, req *cachev1.LPopRequest) (*cachev1.LPopResponse, error) {
	item, ok, err := s.eng.LPop(ctx, req.Keyspace, req.Name)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.LPopResponse{Present: ok, Item: item}, nil
}

func (s *Server) RPop(ctx context.Context, req *cachev1.RPopRequest) (*cachev1.RPopResponse, error) {
	item, ok, err := s.eng.RPop(ctx, req.Keyspace, req.Name)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.RPopResponse{Present: ok, Item: item}, nil
}

func (s *Server) LLen(ctx context.Context, req *cachev1.LLenRequest) (*cachev1.LLenResponse, error) {
	n, err := s.eng.LLen(ctx, req.Keyspace, req.Name)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.LLenResponse{Len: int64(n)}, nil
}

func (s *Server) LIndex(ctx context.Context, req *cachev1.LIndexRequest) (*cachev1.LIndexResponse, error) {
	item, ok, err := s.eng.LIndex(ctx, req.Keyspace, req.Name, int(req.Index))
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.LIndexResponse{Present: ok, Item: item}, nil
}

func (s *Server) LRange(ctx context.Context, req *cachev1.LRangeRequest) (*cachev1.LRangeResponse, error) {
	items, err := s.eng.LRange(ctx, req.Keyspace, req.Name, int(req.Start), int(req.Stop))
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.LRangeResponse{Items: items}, nil
}

func (s *Server) HSet(ctx context.Context, req *cachev1.HSetRequest) (*cachev1.HSetResponse, error) {
	if err := s.eng.HSet(ctx, req.Keyspace, req.Name, req.Field, req.Value); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.HSetResponse{}, nil
}

func (s *Server) HGet(ctx context.Context, req *cachev1.HGetRequest) (*cachev1.HGetResponse, error) {
	v, ok, err := s.eng.HGet(ctx, req.Keyspace, req.Name, req.Field)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.HGetResponse{Present: ok, Value: v}, nil
}

func (s *Server) HDel(ctx context.Context, req *cachev1.HDelRequest) (*cachev1.HDelResponse, error) {
	if err := s.eng.HDel(ctx, req.Keyspace, req.Name, req.Field); err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.HDelResponse{}, nil
}

func (s *Server) HExists(ctx context.Context, req *cachev1.HExistsRequest) (*cachev1.HExistsResponse, error) {
	ok, err := s.eng.HExists(ctx, req.Keyspace, req.Name, req.Field)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.HExistsResponse{Present: ok}, nil
}

func (s *Server) HLen(ctx context.Context, req *cachev1.HLenRequest) (*cachev1.HLenResponse, error) {
	n, err := s.eng.HLen(ctx, req.Keyspace, req.Name)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.HLenResponse{Len: int64(n)}, nil
}

func (s *Server) HGetAll(ctx context.Context, req *cachev1.HGetAllRequest) (*cachev1.HGetAllResponse, error) {
	all, err := s.eng.HGetAll(ctx, req.Keyspace, req.Name)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.HGetAllResponse{Fields: hashFieldsToProto(all)}, nil
}

func (s *Server) Incr(ctx context.Context, req *cachev1.IncrRequest) (*cachev1.IncrResponse, error) {
	n, err := s.eng.Incr(ctx, req.Keyspace, req.Name, req.Delta)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.IncrResponse{Value: n}, nil
}

func (s *Server) CounterGet(ctx context.Context, req *cachev1.CounterGetRequest) (*cachev1.CounterGetResponse, error) {
	n, ok, err := s.eng.CounterGet(ctx, req.Keyspace, req.Name)
	if err != nil {
		return nil, grpcmap.Status(err)
	}
	return &cachev1.CounterGetResponse{Present: ok, Value: n}, nil
}

func hashFieldsToProto(in []engine.HashField) []*cachev1.HashField {
	if len(in) == 0 {
		return nil
	}
	out := make([]*cachev1.HashField, len(in))
	for i, f := range in {
		out[i] = &cachev1.HashField{Field: f.Field, Value: f.Value}
	}
	return out
}

func geoMembersToProto(in []engine.GeoMember) []*cachev1.GeoMember {
	if len(in) == 0 {
		return nil
	}
	out := make([]*cachev1.GeoMember, len(in))
	for i, m := range in {
		out[i] = &cachev1.GeoMember{Member: m.Member, Lon: m.Lon, Lat: m.Lat, DistMeters: m.Dist}
	}
	return out
}

func zMembersToProto(in []engine.ZMember) []*cachev1.ZMember {
	if len(in) == 0 {
		return nil
	}
	out := make([]*cachev1.ZMember, len(in))
	for i, m := range in {
		out[i] = &cachev1.ZMember{Member: m.Member, Score: m.Score}
	}
	return out
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
