package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestExampleVecSet(t *testing.T) {
	var buf bytes.Buffer
	if err := runDemo(&buf); err != nil {
		t.Fatalf("%v\n%s", err, buf.String())
	}
	got := buf.String()
	for _, want := range []string{
		"OK: ModeVectorSet walkthrough passed",
		"=== 3) VSim cosine",
		"=== 4) Replace member",
		"=== 6) L2 vs IP",
		"=== 8) Last VRem",
		"=== 9) Delete tombstone",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q:\n%s", want, got)
		}
	}
}
