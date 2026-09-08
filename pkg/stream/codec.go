// Package stream is the ModeStream codec: append-only log with millis-seq ids.
package stream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	PrefixSnapshot byte = 'S'
	PrefixAdd      byte = 'A'
	PrefixDel      byte = 'D'
	PrefixTrim     byte = 'T'

	HardCap    = 4096
	DefaultMax = 512
)

// NowMilli is the clock used to mint ids. Tests may replace it.
var NowMilli = func() uint64 { return uint64(time.Now().UnixMilli()) }

// Entry is one log record.
type Entry struct {
	Ms, Seq uint64
	Payload []byte
}

// ID is "millis-seq".
func (e Entry) ID() string { return FormatID(e.Ms, e.Seq) }

// Log is an in-memory stream (index 0 = oldest).
type Log struct {
	LastMs, LastSeq uint64
	Entries         []Entry
}

func New() *Log { return &Log{} }

func FormatID(ms, seq uint64) string { return fmt.Sprintf("%d-%d", ms, seq) }

func ParseID(s string) (ms, seq uint64, err error) {
	i := strings.IndexByte(s, '-')
	if i <= 0 || i == len(s)-1 {
		return 0, 0, errBadID
	}
	ms, err = strconv.ParseUint(s[:i], 10, 64)
	if err != nil {
		return 0, 0, errBadID
	}
	seq, err = strconv.ParseUint(s[i+1:], 10, 64)
	if err != nil {
		return 0, 0, errBadID
	}
	return ms, seq, nil
}

var (
	errBadBlob = errors.New("stream: bad blob")
	errBadID   = errors.New("stream: bad id")
	ErrFull    = errors.New("stream: full")
)

// Bound is an XRange endpoint.
type Bound struct {
	Ms, Seq   uint64
	Exclusive bool
	Min, Max  bool
}

// ParseBound: "-" / "+" / millis[-seq]; "(" only on start.
func ParseBound(s string, isStart bool) (Bound, error) {
	if s == "" || s == "-" {
		return Bound{Min: true}, nil
	}
	if s == "+" {
		return Bound{Max: true}, nil
	}
	ex := false
	if strings.HasPrefix(s, "(") {
		if !isStart {
			return Bound{}, errBadID
		}
		ex = true
		s = s[1:]
		if s == "" {
			return Bound{}, errBadID
		}
	}
	if strings.IndexByte(s, '-') < 0 {
		ms, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return Bound{}, errBadID
		}
		if isStart {
			return Bound{Ms: ms, Seq: 0, Exclusive: ex}, nil
		}
		return Bound{Ms: ms, Seq: ^uint64(0), Exclusive: ex}, nil
	}
	ms, seq, err := ParseID(s)
	if err != nil {
		return Bound{}, err
	}
	return Bound{Ms: ms, Seq: seq, Exclusive: ex}, nil
}

func cmpID(ms, seq, oms, oseq uint64) int {
	if ms < oms {
		return -1
	}
	if ms > oms {
		return 1
	}
	if seq < oseq {
		return -1
	}
	if seq > oseq {
		return 1
	}
	return 0
}

func (b Bound) below(ms, seq uint64) bool {
	if b.Min {
		return false
	}
	if b.Max {
		return true
	}
	c := cmpID(ms, seq, b.Ms, b.Seq)
	if b.Exclusive {
		return c <= 0
	}
	return c < 0
}

func (b Bound) above(ms, seq uint64) bool {
	if b.Max {
		return false
	}
	if b.Min {
		return true
	}
	return cmpID(ms, seq, b.Ms, b.Seq) > 0
}

func ClampCount(n int) int {
	if n <= 0 {
		return 0
	}
	if n > DefaultMax {
		return DefaultMax
	}
	return n
}

func (l *Log) Mint(now uint64) (ms, seq uint64) {
	if l == nil {
		return now, 0
	}
	if now < l.LastMs {
		now = l.LastMs
	}
	if now == l.LastMs {
		return now, l.LastSeq + 1
	}
	return now, 0
}

func (l *Log) Append(payload []byte, now uint64, autoTrim int) (id string, err error) {
	if l == nil {
		return "", errBadBlob
	}
	if autoTrim <= 0 && len(l.Entries) >= HardCap {
		return "", ErrFull
	}
	ms, seq := l.Mint(now)
	cp := append([]byte(nil), payload...)
	l.Entries = append(l.Entries, Entry{Ms: ms, Seq: seq, Payload: cp})
	l.LastMs, l.LastSeq = ms, seq
	if autoTrim > 0 {
		l.Trim(autoTrim)
	}
	if len(l.Entries) > HardCap {
		l.Trim(HardCap)
	}
	return FormatID(ms, seq), nil
}

func (l *Log) Trim(maxLen int) {
	if l == nil || maxLen < 0 {
		return
	}
	if maxLen == 0 {
		l.Entries = l.Entries[:0]
		return
	}
	if n := len(l.Entries); n > maxLen {
		l.Entries = append([]Entry(nil), l.Entries[n-maxLen:]...)
	}
}

func (l *Log) Del(ms, seq uint64) {
	if l == nil {
		return
	}
	out := l.Entries[:0]
	for _, e := range l.Entries {
		if e.Ms == ms && e.Seq == seq {
			continue
		}
		out = append(out, e)
	}
	l.Entries = out
}

func (l *Log) Range(start, end Bound, count int, rev bool) []Entry {
	if l == nil {
		return nil
	}
	count = ClampCount(count)
	var out []Entry
	if !rev {
		for _, e := range l.Entries {
			if start.below(e.Ms, e.Seq) || end.above(e.Ms, e.Seq) {
				continue
			}
			out = append(out, copyEnt(e))
			if count > 0 && len(out) >= count {
				break
			}
		}
		return out
	}
	for i := len(l.Entries) - 1; i >= 0; i-- {
		e := l.Entries[i]
		if start.below(e.Ms, e.Seq) || end.above(e.Ms, e.Seq) {
			continue
		}
		out = append(out, copyEnt(e))
		if count > 0 && len(out) >= count {
			break
		}
	}
	return out
}

func copyEnt(e Entry) Entry {
	return Entry{Ms: e.Ms, Seq: e.Seq, Payload: append([]byte(nil), e.Payload...)}
}

func (l *Log) Encode() []byte {
	if l == nil {
		l = New()
	}
	n := 1 + 10*3
	for _, e := range l.Entries {
		n += 10*3 + len(e.Payload)
	}
	buf := make([]byte, 0, n)
	buf = append(buf, PrefixSnapshot)
	buf = binary.AppendUvarint(buf, l.LastMs)
	buf = binary.AppendUvarint(buf, l.LastSeq)
	buf = binary.AppendUvarint(buf, uint64(len(l.Entries)))
	for _, e := range l.Entries {
		buf = binary.AppendUvarint(buf, e.Ms)
		buf = binary.AppendUvarint(buf, e.Seq)
		buf = binary.AppendUvarint(buf, uint64(len(e.Payload)))
		buf = append(buf, e.Payload...)
	}
	return buf
}

func DecodeSnapshot(blob []byte) (*Log, error) {
	if len(blob) < 1 || blob[0] != PrefixSnapshot {
		return nil, errBadBlob
	}
	p := blob[1:]
	var ok bool
	l := New()
	if l.LastMs, p, ok = uvarint(p); !ok {
		return nil, errBadBlob
	}
	if l.LastSeq, p, ok = uvarint(p); !ok {
		return nil, errBadBlob
	}
	var n uint64
	if n, p, ok = uvarint(p); !ok || n > HardCap {
		return nil, errBadBlob
	}
	l.Entries = make([]Entry, 0, n)
	for i := uint64(0); i < n; i++ {
		var e Entry
		var plen uint64
		if e.Ms, p, ok = uvarint(p); !ok {
			return nil, errBadBlob
		}
		if e.Seq, p, ok = uvarint(p); !ok {
			return nil, errBadBlob
		}
		if plen, p, ok = uvarint(p); !ok || uint64(len(p)) < plen {
			return nil, errBadBlob
		}
		e.Payload = append([]byte(nil), p[:plen]...)
		p = p[plen:]
		l.Entries = append(l.Entries, e)
	}
	if len(p) != 0 {
		return nil, errBadBlob
	}
	return l, nil
}

func EncodeInboxAdd(payload []byte) []byte {
	buf := make([]byte, 0, 1+10+len(payload))
	buf = append(buf, PrefixAdd)
	buf = binary.AppendUvarint(buf, uint64(len(payload)))
	return append(buf, payload...)
}

func DecodeInboxAdd(blob []byte) ([]byte, error) {
	if len(blob) < 1 || blob[0] != PrefixAdd {
		return nil, errBadBlob
	}
	n, p, ok := uvarint(blob[1:])
	if !ok || uint64(len(p)) < n {
		return nil, errBadBlob
	}
	return append([]byte(nil), p[:n]...), nil
}

func EncodeInboxDel(id string) []byte {
	b := []byte(id)
	buf := make([]byte, 0, 1+10+len(b))
	buf = append(buf, PrefixDel)
	buf = binary.AppendUvarint(buf, uint64(len(b)))
	return append(buf, b...)
}

func DecodeInboxDel(blob []byte) (string, error) {
	if len(blob) < 1 || blob[0] != PrefixDel {
		return "", errBadBlob
	}
	n, p, ok := uvarint(blob[1:])
	if !ok || uint64(len(p)) < n {
		return "", errBadBlob
	}
	return string(p[:n]), nil
}

func EncodeInboxTrim(maxLen int) []byte {
	buf := []byte{PrefixTrim}
	return binary.AppendUvarint(buf, uint64(maxLen))
}

func DecodeInboxTrim(blob []byte) (int, error) {
	if len(blob) < 1 || blob[0] != PrefixTrim {
		return 0, errBadBlob
	}
	n, p, ok := uvarint(blob[1:])
	if !ok || len(p) != 0 {
		return 0, errBadBlob
	}
	if n > uint64(^uint(0)>>1) {
		return 0, errBadBlob
	}
	return int(n), nil
}

func uvarint(p []byte) (uint64, []byte, bool) {
	v, n := binary.Uvarint(p)
	if n <= 0 {
		return 0, p, false
	}
	return v, p[n:], true
}
