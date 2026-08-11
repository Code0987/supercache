package store

import "time"

// Flag bits for Entry.Flags.
const (
	FlagNegative  uint32 = 1 << 0
	FlagTombstone uint32 = 1 << 1 // versioned delete marker (blocks stale ApplyPut)
	FlagBloom     uint32 = 1 << 2 // value is a Bloom bitset
	FlagBloomAdd  uint32 = 1 << 3 // fan-out only: value is an item to OR into the filter
)

// Entry is the on-node stored value envelope (versioned LWW + TTL).
type Entry struct {
	Value    []byte
	Version  uint64
	ExpireAt int64 // unix nano; 0 = no expiry
	Flags    uint32
}

// IsNegative reports whether this is a negative-cache sentinel.
func (e Entry) IsNegative() bool {
	return e.Flags&FlagNegative != 0
}

// IsTombstone reports whether this is a delete tombstone (not a readable value).
func (e Entry) IsTombstone() bool {
	return e.Flags&FlagTombstone != 0
}

// IsBloom reports whether Value is a Bloom bitset.
func (e Entry) IsBloom() bool {
	return e.Flags&FlagBloom != 0
}

// IsBloomAdd reports a replica item-add (Value is the item, not the bitset).
func (e Entry) IsBloomAdd() bool {
	return e.Flags&FlagBloomAdd != 0
}

// Expired reports whether the entry is past ExpireAt at time now.
func (e Entry) Expired(now time.Time) bool {
	if e.ExpireAt == 0 {
		return false
	}
	return now.UnixNano() >= e.ExpireAt
}

// CloneValue returns a copy of Value (nil-safe).
func (e Entry) CloneValue() []byte {
	if e.Value == nil {
		return nil
	}
	out := make([]byte, len(e.Value))
	copy(out, e.Value)
	return out
}

// Cost estimates memory cost for MaxBytes accounting.
func (e Entry) Cost() int64 {
	// key cost is tracked separately by the store; this is value envelope cost.
	return int64(len(e.Value)) + 64 // rough overhead for metadata
}
