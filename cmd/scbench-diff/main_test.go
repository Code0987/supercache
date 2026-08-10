package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderNoBaseline(t *testing.T) {
	dir := t.TempDir()
	cur := filepath.Join(dir, "new.json")
	mustWrite(t, cur, `{
	  "generated_at": "2026-08-08T00:00:00Z",
	  "git_sha": "aaaaaaaa",
	  "runs": [{"op":"get","path":"hit","nodes":1,"concurrency":1,"dist":"uniform","median_ops_per_sec":1000,"median_p50_ns":1000,"median_p99_ns":2000}]
	}`)
	md, err := render("", cur, "", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(md, "No previous") || !strings.Contains(md, "get/hit n=1 c=1") {
		t.Fatalf("md:\n%s", md)
	}
	if strings.Contains(md, "same GitHub runner") {
		t.Fatal("did not pass same-runner")
	}
}

func TestRenderDelta(t *testing.T) {
	dir := t.TempDir()
	oldP := filepath.Join(dir, "old.json")
	newP := filepath.Join(dir, "new.json")
	mustWrite(t, oldP, `{
	  "git_sha": "1111111",
	  "generated_at": "t0",
	  "runs": [{"op":"get","path":"hit","nodes":1,"concurrency":10,"dist":"uniform","median_ops_per_sec":1000,"median_p50_ns":400000,"median_p99_ns":800000}]
	}`)
	mustWrite(t, newP, `{
	  "git_sha": "2222222",
	  "generated_at": "t1",
	  "runs": [{
	    "op":"get","path":"hit","nodes":1,"concurrency":10,"dist":"uniform",
	    "median_ops_per_sec":800,
	    "trials":[
	      {"ops_per_sec":700,"p50_ns":500000,"p99_ns":900000},
	      {"ops_per_sec":800,"p50_ns":500000,"p99_ns":900000},
	      {"ops_per_sec":900,"p50_ns":500000,"p99_ns":900000}
	    ]
	  }]
	}`)
	md, err := render(oldP, newP, "", "", true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(md, "1000") || !strings.Contains(md, "800") || !strings.Contains(md, "-20.0%") {
		t.Fatalf("md:\n%s", md)
	}
	if !strings.Contains(md, "same GitHub runner") {
		t.Fatal("expected same-runner note")
	}
}

func TestParseMicro(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "micro.txt")
	mustWrite(t, p, "BenchmarkStoreGetHit-2\t1\t400 ns/op\t271 B/op\t2 allocs/op\nBenchmarkStoreGetHit-2\t1\t500 ns/op\t271 B/op\t4 allocs/op\nPASS\n")
	m, err := parseMicro(p)
	if err != nil {
		t.Fatal(err)
	}
	row, ok := m["BenchmarkStoreGetHit"]
	if !ok || row.nsPerOp != 450 || row.allocs != 3 {
		t.Fatalf("%+v", row)
	}
}

func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
