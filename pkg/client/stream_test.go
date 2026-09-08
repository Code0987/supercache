package client

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/Code0987/supercache/pkg/engine"
)

func TestXTrimRejectsOverflow(t *testing.T) {
	c := &Client{}
	ctx := context.Background()
	if err := c.XTrim(ctx, "events", "logs", 1<<32); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("1<<32: %v", err)
	}
	if err := c.XTrim(ctx, "events", "logs", -1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("neg: %v", err)
	}
}

func TestXRangeRejectsCountOverflow(t *testing.T) {
	c := &Client{}
	ctx := context.Background()
	if _, err := c.XRange(ctx, "events", "logs", "-", "+", math.MaxInt32+1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("xrange: %v", err)
	}
	if _, err := c.XRevRange(ctx, "events", "logs", "-", "+", math.MaxInt32+1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("xrevrange: %v", err)
	}
}
