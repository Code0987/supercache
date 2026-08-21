// Package jsonx encodes nested JSON documents for ModeJSON.
package jsonx

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// Path is a sequence of object-key or array-index segments.
// An empty Path is the document root ($).
type Path []Seg

// Seg is one path segment.
type Seg struct {
	Key     string // object key; valid when !IsIndex
	Index   int    // array index; valid when IsIndex
	IsIndex bool
}

var (
	ErrPath  = errors.New("jsonx: invalid path")
	ErrType  = errors.New("jsonx: type mismatch")
	ErrIndex = errors.New("jsonx: array index")
	ErrJSON  = errors.New("jsonx: invalid json")
)

// ParsePath parses the tiny ModeJSON path language.
// "" and "$" are the root (empty Path).
func ParsePath(s string) (Path, error) {
	if s == "" || s == "$" {
		return Path{}, nil
	}
	if s[0] != '$' {
		return nil, ErrPath
	}
	i := 1
	var out Path
	for i < len(s) {
		switch s[i] {
		case '.':
			i++
			if i >= len(s) || !isIdentStart(s[i]) {
				return nil, ErrPath
			}
			start := i
			i++
			for i < len(s) && isIdentCont(s[i]) {
				i++
			}
			out = append(out, Seg{Key: s[start:i]})
		case '[':
			i++
			if i >= len(s) {
				return nil, ErrPath
			}
			if s[i] == '"' {
				key, n, err := parseJSONString(s[i:])
				if err != nil {
					return nil, ErrPath
				}
				i += n
				if i >= len(s) || s[i] != ']' {
					return nil, ErrPath
				}
				i++
				out = append(out, Seg{Key: key})
			} else {
				idx, n, err := parseIndex(s[i:])
				if err != nil {
					return nil, ErrPath
				}
				i += n
				if i >= len(s) || s[i] != ']' {
					return nil, ErrPath
				}
				i++
				out = append(out, Seg{Index: idx, IsIndex: true})
			}
		default:
			return nil, ErrPath
		}
	}
	return out, nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func isIdentCont(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

func parseJSONString(s string) (string, int, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	var key string
	if err := dec.Decode(&key); err != nil {
		return "", 0, err
	}
	return key, int(dec.InputOffset()), nil
}

func parseIndex(s string) (int, int, error) {
	if len(s) == 0 {
		return 0, 0, ErrPath
	}
	if s[0] == '0' {
		return 0, 1, nil
	}
	if s[0] < '1' || s[0] > '9' {
		return 0, 0, ErrPath
	}
	i := 1
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	n, err := strconv.ParseUint(s[:i], 10, 64)
	if err != nil || n > uint64(math.MaxInt) {
		return 0, 0, ErrPath
	}
	return int(n), i, nil
}

// Decode parses one JSON value with UseNumber. Empty, trailing junk, or
// invalid JSON → ErrJSON. Decode("null") returns Go nil.
func Decode(b []byte) (any, error) {
	if len(b) == 0 {
		return nil, ErrJSON
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, ErrJSON
	}
	if dec.More() {
		return nil, ErrJSON
	}
	return v, nil
}

// Encode writes compact JSON (no HTML escape). Encode(nil) is "null".
func Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	return append([]byte(nil), b...), nil
}

// Equal reports semantic JSON equality using Decode (UseNumber), not Unmarshal.
func Equal(a, b []byte) bool {
	va, err := Decode(a)
	if err != nil {
		return false
	}
	vb, err := Decode(b)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(va, vb)
}

// Clone is a typed walk that keeps json.Number.
func Clone(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case bool:
		return x
	case string:
		return x
	case json.Number:
		return x
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, vv := range x {
			out[k] = Clone(vv)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, vv := range x {
			out[i] = Clone(vv)
		}
		return out
	default:
		return v
	}
}

// Get walks p. Missing / type-mismatch → ok=false.
// Get of a live JSON null node returns (nil, true).
func Get(doc any, p Path) (any, bool) {
	cur := doc
	for _, s := range p {
		if s.IsIndex {
			arr, ok := cur.([]any)
			if !ok || s.Index < 0 || s.Index >= len(arr) {
				return nil, false
			}
			cur = arr[s.Index]
			continue
		}
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok := obj[s.Key]
		if !ok {
			return nil, false
		}
		cur = v
	}
	return cur, true
}

// Set returns a new root with val at p. Does not mutate doc.
// Go nil is JSON null. Set(nil, non-root, …) is ErrType.
func Set(doc any, p Path, val any) (any, error) {
	if len(p) == 0 {
		return val, nil
	}
	if doc == nil {
		return nil, ErrType
	}
	return setRec(Clone(doc), p, val)
}

func setRec(cur any, p Path, val any) (any, error) {
	seg := p[0]
	rest := p[1:]
	if seg.IsIndex {
		arr, ok := cur.([]any)
		if !ok {
			return nil, ErrType
		}
		n := seg.Index
		if n < 0 {
			return nil, ErrIndex
		}
		if n < len(arr) {
			if len(rest) == 0 {
				arr[n] = val
				return arr, nil
			}
			child, err := setRec(arr[n], rest, val)
			if err != nil {
				return nil, err
			}
			arr[n] = child
			return arr, nil
		}
		if n == len(arr) {
			if len(rest) == 0 {
				return append(arr, val), nil
			}
			child, err := createPath(rest, val)
			if err != nil {
				return nil, err
			}
			return append(arr, child), nil
		}
		return nil, ErrIndex
	}
	obj, ok := cur.(map[string]any)
	if !ok {
		return nil, ErrType
	}
	if len(rest) == 0 {
		obj[seg.Key] = val
		return obj, nil
	}
	child, exists := obj[seg.Key]
	if !exists {
		next, err := createPath(rest, val)
		if err != nil {
			return nil, err
		}
		obj[seg.Key] = next
		return obj, nil
	}
	newChild, err := setRec(child, rest, val)
	if err != nil {
		return nil, err
	}
	obj[seg.Key] = newChild
	return obj, nil
}

func createPath(p Path, val any) (any, error) {
	if len(p) == 0 {
		return val, nil
	}
	if p[0].IsIndex {
		return nil, ErrType
	}
	child, err := createPath(p[1:], val)
	if err != nil {
		return nil, err
	}
	return map[string]any{p[0].Key: child}, nil
}

// Del returns a new root with the node at p removed.
// Root delete yields {}. ok=false if nothing was removed.
func Del(doc any, p Path) (any, bool, error) {
	if len(p) == 0 {
		return map[string]any{}, true, nil
	}
	if doc == nil {
		return nil, false, nil
	}
	root := Clone(doc)
	newRoot, ok, err := delRec(root, p)
	if err != nil || !ok {
		return doc, ok, err
	}
	return newRoot, true, nil
}

func delRec(cur any, p Path) (any, bool, error) {
	seg := p[0]
	rest := p[1:]
	if seg.IsIndex {
		arr, ok := cur.([]any)
		if !ok || seg.Index < 0 || seg.Index >= len(arr) {
			return cur, false, nil
		}
		if len(rest) == 0 {
			out := make([]any, 0, len(arr)-1)
			out = append(out, arr[:seg.Index]...)
			out = append(out, arr[seg.Index+1:]...)
			return out, true, nil
		}
		child, ok, err := delRec(arr[seg.Index], rest)
		if err != nil || !ok {
			return cur, ok, err
		}
		arr[seg.Index] = child
		return arr, true, nil
	}
	obj, ok := cur.(map[string]any)
	if !ok {
		return cur, false, nil
	}
	child, exists := obj[seg.Key]
	if !exists {
		return cur, false, nil
	}
	if len(rest) == 0 {
		delete(obj, seg.Key)
		return obj, true, nil
	}
	newChild, ok, err := delRec(child, rest)
	if err != nil || !ok {
		return cur, ok, err
	}
	obj[seg.Key] = newChild
	return obj, true, nil
}

// EncodeSet packs uvarint(len(path)) + path + raw JSON.
func EncodeSet(path string, raw []byte) []byte {
	var buf bytes.Buffer
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(len(path)))
	buf.Write(scratch[:n])
	buf.WriteString(path)
	buf.Write(raw)
	return buf.Bytes()
}

// DecodeSet unpacks one inbox set payload. v==0 is an empty path (OK).
// An empty JSON tail is not an error here.
func DecodeSet(b []byte) (path string, raw []byte, err error) {
	v, w := binary.Uvarint(b)
	if w <= 0 {
		return "", nil, errors.New("jsonx: bad set uvarint")
	}
	rest := b[w:]
	if v > uint64(len(rest)) {
		return "", nil, errors.New("jsonx: truncated set path")
	}
	path = string(rest[:v])
	raw = append([]byte(nil), rest[v:]...)
	return path, raw, nil
}

// ApproxWireBytes is the encoded document size.
func ApproxWireBytes(v any) int64 {
	b, err := Encode(v)
	if err != nil {
		return 0
	}
	return int64(len(b))
}
