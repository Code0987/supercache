// Package grpcmap maps engine sentinel errors to gRPC status codes.
// Used by application Cache and mesh Peer gRPC servers so the table cannot drift.
package grpcmap

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Code0987/supercache/pkg/engine"
)

// Status maps an engine (or compatible) error to a gRPC status error.
// The status message is err.Error() so callers still see the original text.
// nil returns nil. Unknown errors become codes.Internal.
//
// Callers that return structured success bodies for some failures (Get not-found,
// MultiError peer failures, batch KeyErrors) must handle those cases before Status.
func Status(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, engine.ErrNotFound),
		errors.Is(err, engine.ErrKeyspaceNotFound):
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
