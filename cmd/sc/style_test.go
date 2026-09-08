package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestColorOffWhenNoColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	t.Setenv("TERM", "xterm")
	disableColor = false
	if colorOn(os.Stdout) {
		t.Fatal("NO_COLOR should disable color")
	}
	if got := paint(os.Stdout, ansiGreen, "OK"); got != "OK" {
		t.Fatalf("paint=%q", got)
	}
}

func TestColorOffWhenDumbTerm(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	t.Setenv("TERM", "dumb")
	disableColor = false
	if colorOn(os.Stdout) {
		t.Fatal("TERM=dumb should disable color")
	}
}

func TestColorOffWhenJSON(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	t.Setenv("TERM", "xterm")
	applyJSONColor(true)
	t.Cleanup(func() { applyJSONColor(false) })
	if colorOn(os.Stdout) {
		t.Fatal("-json should disable color")
	}
}

func TestPrintHelpersPlain(t *testing.T) {
	applyJSONColor(false)
	out, _ := captureOut(func() int {
		printOK("put k (3 bytes)")
		printNil()
		printBool(true)
		printBool(false)
		return 0
	})
	got := strings.Split(strings.TrimSpace(out), "\n")
	want := []string{"OK put k (3 bytes)", "(nil)", "true", "false"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("out=%q want %q", out, strings.Join(want, "\n"))
	}
	if strings.Contains(out, "\033") {
		t.Fatal("piped stdout must not contain ANSI")
	}
}

func TestUsageAndUnknownText(t *testing.T) {
	errOut, code := captureErr(func() int {
		return run([]string{"get"})
	})
	if code != 2 {
		t.Fatalf("get usage: exit %d", code)
	}
	if !strings.Contains(errOut, "usage: get <key> [key...]") {
		t.Fatalf("get usage: %q", errOut)
	}

	errOut, code = captureErr(func() int {
		printUnknown("nope")
		return 2
	})
	if code != 2 || !strings.Contains(errOut, `sc: unknown command "nope"`) {
		t.Fatalf("unknown: exit %d out=%q", code, errOut)
	}

	errOut, code = captureErr(func() int {
		printCmdMsg("put", "bad base64")
		printWarn("peer failed")
		printUsageLine("usage: del <key> [key...]")
		return 0
	})
	if code != 0 {
		t.Fatal(code)
	}
	if !strings.Contains(errOut, "put: bad base64") ||
		!strings.Contains(errOut, "warning: peer failed") ||
		!strings.Contains(errOut, "usage: del <key> [key...]") {
		t.Fatalf("stderr=%q", errOut)
	}
}

func TestCompactHelp(t *testing.T) {
	errOut, code := captureErr(func() int {
		return run([]string{"help"})
	})
	if code != 0 {
		t.Fatalf("help: exit %d", code)
	}
	for _, want := range []string{
		"sc — SuperCache CLI",
		"Usage",
		"Commands",
		"get  put  del",
		"vadd  vrem  vsim",
		"xadd  xlen  xrange",
		"Flags",
		"Examples",
	} {
		if !strings.Contains(errOut, want) {
			t.Fatalf("missing %q in help:\n%s", want, errOut)
		}
	}
	for _, no := range []string{
		"ModeZSet upsert",
		"Put string value",
		"ForwardPut",
		"Global flags:",
		"sc cacheonly@:9000>",
	} {
		if strings.Contains(errOut, no) {
			t.Fatalf("clutter %q still in help:\n%s", no, errOut)
		}
	}
	if n := strings.Count(errOut, "\n"); n > 45 {
		t.Fatalf("help too long: %d lines\n%s", n, errOut)
	}
}

func TestREPLHelpCompact(t *testing.T) {
	out, _ := captureOut(func() int {
		printREPLHelp()
		return 0
	})
	if !strings.Contains(out, "get  put  del") || !strings.Contains(out, "keyspace") {
		t.Fatalf("repl help:\n%s", out)
	}
	if strings.Contains(out, "Usage") || strings.Contains(out, "-addr") {
		t.Fatalf("repl help should skip one-shot usage/flags:\n%s", out)
	}
}

func TestColorPromptPlain(t *testing.T) {
	applyJSONColor(false)
	got := colorPrompt("cacheonly", ":9000")
	if got != "sc cacheonly@:9000> " {
		t.Fatalf("prompt=%q", got)
	}
}

func TestGetMissAndPutOKText(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name: "cacheonly", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	gs, lis, err := cacheserver.ListenAndServe("127.0.0.1:0", eng)
	if err != nil {
		t.Fatal(err)
	}
	defer gs.Stop()
	addr := lis.Addr().String()

	out, code := captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "cacheonly", "get", "missing"})
	})
	if code != 1 || strings.TrimSpace(out) != "(nil)" {
		t.Fatalf("get miss: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "cacheonly", "put", "k", "abc"})
	})
	if code != 0 || strings.TrimSpace(out) != "OK put k (3 bytes)" {
		t.Fatalf("put: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "cacheonly", "del", "k"})
	})
	if code != 0 || strings.TrimSpace(out) != "OK del k" {
		t.Fatalf("del: exit %d out=%q", code, out)
	}
}

func captureErr(fn func() int) (string, int) {
	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w
	code := fn()
	_ = w.Close()
	os.Stderr = old
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	_ = r.Close()
	return buf.String(), code
}
