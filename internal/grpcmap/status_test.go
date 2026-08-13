package grpcmap_test

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Code0987/supercache/internal/grpcmap"
	"github.com/Code0987/supercache/pkg/engine"
)

func TestStatusSentinels(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code codes.Code
	}{
		{"nil", nil, codes.OK}, // Status returns nil
		{"not found", engine.ErrNotFound, codes.NotFound},
		{"wrapped not found", fmt.Errorf("x: %w", engine.ErrNotFound), codes.NotFound},
		{"keyspace", engine.ErrKeyspaceNotFound, codes.NotFound},
		{"invalid", engine.ErrInvalidArgument, codes.InvalidArgument},
		{"key large", engine.ErrKeyTooLarge, codes.InvalidArgument},
		{"value large", engine.ErrValueTooLarge, codes.InvalidArgument},
		{"batch large", engine.ErrBatchTooLarge, codes.InvalidArgument},
		{"unavailable", engine.ErrUnavailable, codes.Unavailable},
		{"wrapped unavailable", fmt.Errorf("%w: detail", engine.ErrUnavailable), codes.Unavailable},
		{"unknown", errors.New("boom"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := grpcmap.Status(tc.err)
			if tc.err == nil {
				if got != nil {
					t.Fatalf("nil in → %v", got)
				}
				return
			}
			if status.Code(got) != tc.code {
				t.Fatalf("code=%v want %v msg=%v", status.Code(got), tc.code, got)
			}
			// Message preserves original text (status may wrap; message field holds it).
			st, ok := status.FromError(got)
			if !ok {
				t.Fatal("not a status")
			}
			if st.Message() != tc.err.Error() {
				t.Fatalf("message=%q want %q", st.Message(), tc.err.Error())
			}
		})
	}
}
