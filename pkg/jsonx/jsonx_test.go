package jsonx

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParsePathRoot(t *testing.T) {
	for _, s := range []string{"", "$"} {
		p, err := ParsePath(s)
		if err != nil || len(p) != 0 {
			t.Fatalf("%q: %+v %v", s, p, err)
		}
	}
}

func TestParsePathLexer(t *testing.T) {
	p, err := ParsePath(`$["0"]`)
	if err != nil || len(p) != 1 || p[0].IsIndex || p[0].Key != "0" {
		t.Fatalf(`$["0"] key: %+v %v`, p, err)
	}
	p, err = ParsePath(`$[0]`)
	if err != nil || len(p) != 1 || !p[0].IsIndex || p[0].Index != 0 {
		t.Fatalf("$[0] index: %+v %v", p, err)
	}
	p, err = ParsePath(`$[""]`)
	if err != nil || len(p) != 1 || p[0].IsIndex || p[0].Key != "" {
		t.Fatalf(`$[""]: %+v %v`, p, err)
	}
	p, err = ParsePath(`$["a b"]`)
	if err != nil || p[0].Key != "a b" {
		t.Fatalf(`$["a b"]: %+v %v`, p, err)
	}
	p, err = ParsePath(`$["\u0041"]`)
	if err != nil || p[0].Key != "A" {
		t.Fatalf(`$["\\u0041"]: %+v %v`, p, err)
	}
	p, err = ParsePath(`$["\u0022"]`)
	if err != nil || p[0].Key != `"` {
		t.Fatalf(`$["\\u0022"]: %+v %v`, p, err)
	}
	p, err = ParsePath(`$["a\/b"]`)
	if err != nil || p[0].Key != "a/b" {
		t.Fatalf(`$["a\\/b"] want a/b: %+v %v`, p, err)
	}
	p, err = ParsePath(`$.addr.city`)
	if err != nil || len(p) != 2 || p[0].Key != "addr" || p[1].Key != "city" {
		t.Fatalf("ident: %+v %v", p, err)
	}
}

func TestParsePathReject(t *testing.T) {
	bads := []string{
		"foo", "$.", "$..a", "$.1a", "$.a.b[", "$[-1]", "$[01]", "$[+1]",
		"$.a[ 0 ]", "$['x']", `$["unterminated]`, "$[18446744073709551616]",
		"$ ", "$.a ",
	}
	for _, s := range bads {
		if _, err := ParsePath(s); !errors.Is(err, ErrPath) {
			t.Fatalf("%q: want ErrPath got %v", s, err)
		}
	}
}

func TestDecodeUseNumber(t *testing.T) {
	v, err := Decode([]byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	n, ok := v.(json.Number)
	if !ok || n.String() != "1" {
		t.Fatalf("1: %#v", v)
	}
	v, err = Decode([]byte("1e2"))
	if err != nil {
		t.Fatal(err)
	}
	n, ok = v.(json.Number)
	if !ok || n.String() != "1e2" {
		t.Fatalf("1e2: %#v", v)
	}
	v, err = Decode([]byte("null"))
	if err != nil || v != nil {
		t.Fatalf("null: %#v %v", v, err)
	}
	if _, err := Decode([]byte("{}\n1")); !errors.Is(err, ErrJSON) {
		t.Fatalf("trailing: %v", err)
	}
	if _, err := Decode(nil); !errors.Is(err, ErrJSON) {
		t.Fatalf("empty: %v", err)
	}
	if _, err := Decode([]byte("")); !errors.Is(err, ErrJSON) {
		t.Fatalf("empty2: %v", err)
	}
}

func TestEncodeEqualNoHTMLEscape(t *testing.T) {
	a := []byte(`{"b":1,"a":"<x>"}`)
	b := []byte(`{"a":"<x>","b":1}`)
	if !Equal(a, b) {
		t.Fatal("semantic equal")
	}
	enc, err := Encode(map[string]any{"x": "<"})
	if err != nil {
		t.Fatal(err)
	}
	if bytesContain(enc, []byte(`\u003c`)) {
		t.Fatalf("html escaped: %s", enc)
	}
	if !bytesContain(enc, []byte("<")) {
		t.Fatalf("want raw <: %s", enc)
	}
	// Equal must Decode, not Unmarshal (UseNumber)
	if !Equal([]byte(`9007199254740993`), []byte(`9007199254740993`)) {
		t.Fatal("big int")
	}
}

func bytesContain(b, sub []byte) bool {
	return bytesIndex(b, sub) >= 0
}

func bytesIndex(b, sub []byte) int {
	for i := 0; i+len(sub) <= len(b); i++ {
		ok := true
		for j := range sub {
			if b[i+j] != sub[j] {
				ok = false
				break
			}
		}
		if ok {
			return i
		}
	}
	return -1
}

func TestCloneSetKeepNumber(t *testing.T) {
	doc, err := Decode([]byte(`{"n":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := ParsePath("$.s")
	next, err := Set(doc, p, json.Number("1"))
	if err != nil {
		t.Fatal(err)
	}
	n, ok := Get(next, mustPath("$.n"))
	if !ok {
		t.Fatal("lost n")
	}
	num, ok := n.(json.Number)
	if !ok || num.String() != "9007199254740993" {
		t.Fatalf("number mutated: %#v", n)
	}
	// original unchanged
	if _, ok := Get(doc, mustPath("$.s")); ok {
		t.Fatal("set mutated original")
	}
}

func TestSetNilIsJSONNull(t *testing.T) {
	if _, err := Set(nil, mustPath("$.a"), 1); !errors.Is(err, ErrType) {
		t.Fatalf("null object: %v", err)
	}
	if _, err := Set(nil, mustPath("$[0]"), 1); !errors.Is(err, ErrType) {
		t.Fatalf("null array: %v", err)
	}
	got, err := Set(nil, Path{}, json.Number("1"))
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := got.(json.Number); !ok || n.String() != "1" {
		t.Fatalf("root replace: %#v", got)
	}
}

func TestSetParents(t *testing.T) {
	next, err := Set(map[string]any{}, mustPath("$.a.b"), json.Number("1"))
	if err != nil {
		t.Fatal(err)
	}
	v, ok := Get(next, mustPath("$.a.b"))
	if !ok || v.(json.Number).String() != "1" {
		t.Fatalf("%#v %v", v, ok)
	}
	if _, err := Set(map[string]any{}, mustPath("$.a[0]"), json.Number("1")); !errors.Is(err, ErrType) && !errors.Is(err, ErrIndex) {
		t.Fatalf("no auto array: %v", err)
	}
}

func TestSetArray(t *testing.T) {
	doc := []any{json.Number("1"), json.Number("2")}
	next, err := Set(doc, mustPath("$[0]"), json.Number("9"))
	if err != nil {
		t.Fatal(err)
	}
	v, _ := Get(next, mustPath("$[0]"))
	if v.(json.Number).String() != "9" {
		t.Fatal(v)
	}
	next, err = Set(doc, mustPath("$[2]"), json.Number("3"))
	if err != nil {
		t.Fatal(err)
	}
	if len(next.([]any)) != 3 {
		t.Fatal(next)
	}
	if _, err := Set(doc, mustPath("$[3]"), json.Number("4")); !errors.Is(err, ErrIndex) {
		t.Fatalf("beyond: %v", err)
	}
}

func TestDelArrayShift(t *testing.T) {
	doc := []any{json.Number("1"), json.Number("2"), json.Number("3")}
	next, ok, err := Del(doc, mustPath("$[1]"))
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	arr := next.([]any)
	if len(arr) != 2 || arr[0].(json.Number).String() != "1" || arr[1].(json.Number).String() != "3" {
		t.Fatalf("%#v", arr)
	}
}

func TestDelRootAndMissing(t *testing.T) {
	next, ok, err := Del(map[string]any{"a": 1}, Path{})
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	obj, okm := next.(map[string]any)
	if !okm || len(obj) != 0 {
		t.Fatalf("%#v", next)
	}
	next, ok, err = Del(nil, Path{})
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	if len(next.(map[string]any)) != 0 {
		t.Fatal(next)
	}
	_, ok, err = Del(nil, mustPath("$.a"))
	if err != nil || ok {
		t.Fatalf("null non-root: %v %v", ok, err)
	}
	_, ok, err = Del(map[string]any{"a": 1}, mustPath("$.b"))
	if err != nil || ok {
		t.Fatalf("missing: %v %v", ok, err)
	}
}

func TestEncodeDecodeSet(t *testing.T) {
	b := EncodeSet("", []byte(`{"a":1}`))
	path, raw, err := DecodeSet(b)
	if err != nil || path != "" || string(raw) != `{"a":1}` {
		t.Fatalf("%q %q %v", path, raw, err)
	}
	b = EncodeSet("$.a", []byte("1"))
	path, raw, err = DecodeSet(b)
	if err != nil || path != "$.a" || string(raw) != "1" {
		t.Fatalf("%q %q %v", path, raw, err)
	}
	// empty JSON tail is DecodeSet-ok
	b = EncodeSet("$.a", nil)
	path, raw, err = DecodeSet(b)
	if err != nil || path != "$.a" || len(raw) != 0 {
		t.Fatalf("empty tail: %q %q %v", path, raw, err)
	}
	if _, _, err := DecodeSet([]byte{0x80}); err == nil {
		t.Fatal("truncated uvarint")
	}
	// v > len(rest)
	if _, _, err := DecodeSet([]byte{0x05, 'a'}); err == nil {
		t.Fatal("oversize path")
	}
}

func mustPath(s string) Path {
	p, err := ParsePath(s)
	if err != nil {
		panic(err)
	}
	return p
}
