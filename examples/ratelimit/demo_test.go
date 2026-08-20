package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestExampleRateLimit(t *testing.T) {
	var buf bytes.Buffer
	if err := runDemo(&buf); err != nil {
		t.Fatalf("%v\n%s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "OK: ModeCounter rate-limiter walkthrough passed") {
		t.Fatalf("missing OK:\n%s", buf.String())
	}
}

func TestAllowWindowTooSmall(t *testing.T) {
	_, _, _, _, err := Allow(context.Background(), nil, "rl", "k", 3, 500*time.Millisecond)
	if err == nil {
		t.Fatal("want error")
	}
}
