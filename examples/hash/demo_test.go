package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestExampleHash(t *testing.T) {
	var buf bytes.Buffer
	if err := runDemo(&buf); err != nil {
		t.Fatalf("%v\n%s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "OK: ModeHash profile walkthrough passed") {
		t.Fatalf("missing OK line:\n%s", buf.String())
	}
}
