package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestExampleHLL(t *testing.T) {
	var buf bytes.Buffer
	if err := runDemo(&buf); err != nil {
		t.Fatalf("%v\n%s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "OK: ModeHLL walkthrough passed") {
		t.Fatalf("missing OK line:\n%s", buf.String())
	}
}
