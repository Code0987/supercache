package store

import "time"

// Flag bits for Entry.Flags.
const (
	FlagNegative  uint64 = 1 << 0
	FlagTombstone uint64 = 1 << 1  // versioned delete marker (blocks stale ApplyPut)
	FlagBloom     uint64 = 1 << 2  // value is a Bloom bitset
	FlagBloomAdd  uint64 = 1 << 3  // fan-out only: value is an item to OR into the filter
	FlagSet       uint64 = 1 << 4  // value is encoded exact-set membership
	FlagSetAdd    uint64 = 1 << 5  // fan-out only: value is an item to insert
	FlagSetRemove uint64 = 1 << 6  // fan-out only: value is an item to remove
	FlagZSet      uint64 = 1 << 7  // value is encoded sorted set
	FlagZSetAdd   uint64 = 1 << 8  // fan-out: single scored member
	FlagZSetRem   uint64 = 1 << 9  // fan-out: member to remove
	FlagGeo       uint64 = 1 << 10 // value is encoded geo index
	FlagGeoAdd    uint64 = 1 << 11 // fan-out: single lon/lat member
	FlagGeoRem    uint64 = 1 << 12 // fan-out: member to remove
	FlagList      uint64 = 1 << 13 // value is encoded list
	FlagListLPush uint64 = 1 << 14 // owner-inbox: prepend item
	FlagListRPush uint64 = 1 << 15 // owner-inbox: append item
	FlagHash      uint64 = 1 << 16 // value is encoded hash
	FlagHashSet   uint64 = 1 << 17 // fan-out: single field/value
	FlagHashDel   uint64 = 1 << 18 // fan-out: field to remove
	FlagCounter   uint64 = 1 << 19 // value is 8-byte LE int64 snapshot
	FlagJSON      uint64 = 1 << 20 // value is encoded JSON document snapshot
	FlagJSONSet   uint64 = 1 << 21 // owner-inbox: path + JSON value
	FlagJSONDel   uint64 = 1 << 22 // owner-inbox: path
	FlagBitmap    uint64 = 1 << 23 // value is packed Redis-order bit string snapshot
	FlagBitmapSet uint64 = 1 << 24 // owner-inbox: uvarint(offset) + 0/1
	FlagHLL       uint64 = 1 << 25 // value is dense 12 KiB register snapshot
	FlagHLLAdd    uint64 = 1 << 26 // owner-inbox: raw item bytes
	FlagTopK      uint64 = 1 << 27 // value is encoded Space-Saving snapshot
	FlagTopKAdd   uint64 = 1 << 28 // owner-inbox only: raw item to Add (replicas ignore)
	FlagCMS       uint64 = 1 << 29 // value is dense 64 KiB Count-Min snapshot
	FlagCMSIncr   uint64 = 1 << 30 // owner-inbox only: uvarint(n) || item (replicas ignore)
	FlagVectorSet uint64 = 1 << 31 // snapshot or owner-inbox; discriminate by Value[0]
	FlagStream    uint64 = 1 << 32 // snapshot or owner-inbox; discriminate by Value[0]
)

// Entry is the on-node stored value envelope (versioned LWW + TTL).
type Entry struct {
	Value    []byte
	Version  uint64
	ExpireAt int64 // unix nano; 0 = no expiry
	Flags    uint64
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

func (e Entry) IsCounter() bool {
	return e.Flags&FlagCounter != 0
}

func (e Entry) IsJSON() bool {
	return e.Flags&FlagJSON != 0
}

func (e Entry) IsJSONSet() bool {
	return e.Flags&FlagJSONSet != 0
}

func (e Entry) IsJSONDel() bool {
	return e.Flags&FlagJSONDel != 0
}

func (e Entry) IsBitmap() bool {
	return e.Flags&FlagBitmap != 0
}

func (e Entry) IsBitmapSet() bool {
	return e.Flags&FlagBitmapSet != 0
}

func (e Entry) IsHLL() bool {
	return e.Flags&FlagHLL != 0
}

func (e Entry) IsHLLAdd() bool {
	return e.Flags&FlagHLLAdd != 0
}

func (e Entry) IsTopK() bool {
	return e.Flags&FlagTopK != 0
}

func (e Entry) IsTopKAdd() bool {
	return e.Flags&FlagTopKAdd != 0
}

func (e Entry) IsCMS() bool {
	return e.Flags&FlagCMS != 0
}

func (e Entry) IsCMSIncr() bool {
	return e.Flags&FlagCMSIncr != 0
}

func (e Entry) IsVectorSet() bool {
	return e.Flags&FlagVectorSet != 0
}

func (e Entry) IsStream() bool {
	return e.Flags&FlagStream != 0
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
