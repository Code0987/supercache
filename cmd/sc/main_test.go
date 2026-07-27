package main

import (
	"reflect"
	"testing"
)

func TestSplitArgs(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		wantCmd  string
		wantFlag []string
		wantPos  []string
	}{
		{
			name: "flags before command",
			args: []string{"-addr", "h:9", "get", "k1"},
			wantCmd: "get", wantFlag: []string{"-addr", "h:9"}, wantPos: []string{"k1"},
		},
		{
			name: "flags after command",
			args: []string{"get", "-addr", "h:9", "k1"},
			wantCmd: "get", wantFlag: []string{"-addr", "h:9"}, wantPos: []string{"k1"},
		},
		{
			name: "put with value",
			args: []string{"put", "k", "hello", "world"},
			wantCmd: "put", wantFlag: nil, wantPos: []string{"k", "hello", "world"},
		},
		{
			name: "put with file flag",
			args: []string{"put", "k", "-file", "./x.bin"},
			wantCmd: "put", wantFlag: []string{"-file", "./x.bin"}, wantPos: []string{"k"},
		},
		{
			name: "equals form",
			args: []string{"-keyspace=demo", "del", "a", "b"},
			wantCmd: "del", wantFlag: []string{"-keyspace=demo"}, wantPos: []string{"a", "b"},
		},
		{
			name: "bool after",
			args: []string{"peers", "-json"},
			wantCmd: "peers", wantFlag: []string{"-json"}, wantPos: nil,
		},
		{
			name: "dash positional",
			args: []string{"put", "k", "-"},
			wantCmd: "put", wantFlag: nil, wantPos: []string{"k", "-"},
		},
		{
			name: "multi seed addr",
			args: []string{"-addr", "a:1,b:2", "ping"},
			wantCmd: "ping", wantFlag: []string{"-addr", "a:1,b:2"}, wantPos: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, fl, pos, err := splitArgs(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if cmd != tc.wantCmd {
				t.Fatalf("cmd=%q want %q", cmd, tc.wantCmd)
			}
			if !reflect.DeepEqual(fl, tc.wantFlag) {
				t.Fatalf("flags=%v want %v", fl, tc.wantFlag)
			}
			if !reflect.DeepEqual(pos, tc.wantPos) {
				t.Fatalf("pos=%v want %v", pos, tc.wantPos)
			}
		})
	}
}

func TestParseList(t *testing.T) {
	got := parseList("  a:1, b:2,,a:1, c:3 ")
	want := []string{"a:1", "b:2", "c:3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if parseList("") != nil && len(parseList("")) != 0 {
		t.Fatal("empty")
	}
}

func TestTokenize(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{`put k hello`, []string{"put", "k", "hello"}},
		{`put k "hello world"`, []string{"put", "k", "hello world"}},
		{`put k 'a b'`, []string{"put", "k", "a b"}},
		{`get a b c`, []string{"get", "a", "b", "c"}},
		{`put k he\ llo`, []string{"put", "k", "he llo"}},
		{`  seeds  `, []string{"seeds"}},
	}
	for _, tc := range cases {
		got, err := tokenize(tc.in)
		if err != nil {
			t.Fatalf("%q: %v", tc.in, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%q: got %v want %v", tc.in, got, tc.want)
		}
	}
	if _, err := tokenize(`put k "oops`); err == nil {
		t.Fatal("expected unclosed quote error")
	}
}

func TestIsDialRetryable(t *testing.T) {
	if !isDialRetryable(errStr("rpc error: code = Unavailable desc = connection refused")) {
		t.Fatal("expected retryable")
	}
	if isDialRetryable(errStr("key not found")) {
		t.Fatal("should not retry app errors")
	}
}

type errStr string

func (e errStr) Error() string { return string(e) }
