package store

import "time"

// Flag bits for Entry.Flags.
const (
	FlagNegative  uint32 = 1 << 0
	FlagTombstone uint32 = 1 << 1  // versioned delete marker (blocks stale ApplyPut)
	FlagBloom     uint32 = 1 << 2  // value is a Bloom bitset
	FlagBloomAdd  uint32 = 1 << 3  // fan-out only: value is an item to OR into the filter
	FlagSet       uint32 = 1 << 4  // value is encoded exact-set membership
	FlagSetAdd    uint32 = 1 << 5  // fan-out only: value is an item to insert
	FlagSetRemove uint32 = 1 << 6  // fan-out only: value is an item to remove
	FlagZSet      uint32 = 1 << 7  // value is encoded sorted set
	FlagZSetAdd   uint32 = 1 << 8  // fan-out: single scored member
	FlagZSetRem   uint32 = 1 << 9  // fan-out: member to remove
	FlagGeo       uint32 = 1 << 10 // value is encoded geo index
	FlagGeoAdd    uint32 = 1 << 11 // fan-out: single lon/lat member
	FlagGeoRem    uint32 = 1 << 12 // fan-out: member to remove
	FlagList      uint32 = 1 << 13 // value is encoded list
	FlagListLPush uint32 = 1 << 14 // owner-inbox: prepend item
	FlagListRPush uint32 = 1 << 15 // owner-inbox: append item
	FlagHash      uint32 = 1 << 16 // value is encoded hash
	FlagHashSet   uint32 = 1 << 17 // fan-out: single field/value
	FlagHashDel   uint32 = 1 << 18 // fan-out: field to remove
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

// IsSet reports whether Value is an encoded exact set.
func (e Entry) IsSet() bool {
	return e.Flags&FlagSet != 0
}

// IsSetAdd reports a replica set item-add.
func (e Entry) IsSetAdd() bool {
	return e.Flags&FlagSetAdd != 0
}

// IsSetRemove reports a replica set item-remove.
func (e Entry) IsSetRemove() bool {
	return e.Flags&FlagSetRemove != 0
}

// IsZSet reports whether Value is an encoded sorted set.
func (e Entry) IsZSet() bool {
	return e.Flags&FlagZSet != 0
}

// IsZSetAdd reports a replica zset scored-member upsert.
func (e Entry) IsZSetAdd() bool {
	return e.Flags&FlagZSetAdd != 0
}

// IsZSetRem reports a replica zset member remove.
func (e Entry) IsZSetRem() bool {
	return e.Flags&FlagZSetRem != 0
}

// IsGeo reports whether Value is an encoded geo index.
func (e Entry) IsGeo() bool {
	return e.Flags&FlagGeo != 0
}

// IsGeoAdd reports a replica geo position upsert.
func (e Entry) IsGeoAdd() bool {
	return e.Flags&FlagGeoAdd != 0
}

// IsGeoRem reports a replica geo member remove.
func (e Entry) IsGeoRem() bool {
	return e.Flags&FlagGeoRem != 0
}

func (e Entry) IsList() bool {
	return e.Flags&FlagList != 0
}

func (e Entry) IsListLPush() bool {
	return e.Flags&FlagListLPush != 0
}

func (e Entry) IsListRPush() bool {
	return e.Flags&FlagListRPush != 0
}

func (e Entry) IsHash() bool {
	return e.Flags&FlagHash != 0
}

func (e Entry) IsHashSet() bool {
	return e.Flags&FlagHashSet != 0
}

func (e Entry) IsHashDel() bool {
	return e.Flags&FlagHashDel != 0
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
