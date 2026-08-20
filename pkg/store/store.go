package store

// Stats are best-effort counters for a Store.
type Stats struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
	Items     int64
	Bytes     int64
	StaleSkip uint64 // ApplyPut rejected as stale
}

// Store is a concurrent local key/entry map with MaxBytes pressure.
//
// Get/Set/Delete are immediately visible to the calling goroutine (required for
// owner read-your-writes after Put). We intentionally do not use Ristretto as
// the primary backend: its async admission can drop Sets and delay visibility.
type Store interface {
	// Get returns a copy of the entry if present and not expired.
	// Expired entries are deleted and reported as missing.
	Get(key string) (Entry, bool)

	// Peek is like Get but does not update LRU order or hit/miss stats.
	Peek(key string) (Entry, bool)

	// Set stores entry unconditionally (caller enforces LWW if needed).
	// Returns false if the entry was rejected for size limits.
	Set(key string, e Entry) bool

	// AcceptIfNewer stores entry only if missing or entry.Version > local.Version.
	// Returns true if stored.
	AcceptIfNewer(key string, e Entry) bool

	// AcceptNegative stores a negative entry only when the key is missing, expired,
	// or already a lower-version negative. It never replaces a live positive value,
	// regardless of version (concurrent Put must win over miss→NotFound).
	AcceptNegative(key string, e Entry) bool

	// Delete removes key if present (hard remove, no tombstone). Returns true if removed.
	Delete(key string) bool

	// DeleteIfVersion always installs a versioned tombstone when local is missing
	// or local.Version <= deleteVersion. Returns false only when a higher
	// live/tombstone version already exists (stale delete). Tombstones make Get
	// miss and reject AcceptIfNewer with version <= tombstone until the marker
	// expires. LRU must not evict an unexpired tombstone (budget may overshoot).
	DeleteIfVersion(key string, deleteVersion uint64) bool

	// Stats returns a snapshot of counters.
	Stats() Stats

	// Range visits live (non-expired) entries that are not tombstones.
	// Positives and negatives are included. Does not update LRU order.
	// fn must not call back into the store. Return false from fn to stop early.
	// Expired entries encountered during the walk may be removed.
	Range(fn func(key string, e Entry) bool)

	// RangeAll is Range plus unexpired tombstones (for delete handoff).
	RangeAll(fn func(key string, e Entry) bool)

	// BloomAdd ORs item into the named filter, creating it at version if needed.
	// Returns false if a higher tombstone or a non-bloom live entry blocks.
	BloomAdd(key string, item []byte, mBits, k int, version uint64, expireAt int64) bool

	// BloomTest reports maybe-present. false if missing, expired, tombstone, or definite miss.
	// Does not update LRU (read-hot path).
	BloomTest(key string, item []byte, mBits, k int) bool

	// BloomMerge ORs a remote bitset into the named filter (handoff).
	BloomMerge(key string, bits []byte, mBits, k int, version uint64, expireAt int64) bool

	// SetAdd inserts item into the named exact set (creates if missing).
	SetAdd(key string, item []byte, version uint64, expireAt int64) bool
	// SetRemove removes item from the named set.
	SetRemove(key string, item []byte, version uint64, expireAt int64) bool
	// SetContains reports exact membership.
	SetContains(key string, item []byte) bool
	// HasSet reports a live (non-tombstone, non-expired) FlagSet entry without copying Value.
	HasSet(key string) bool
	// PeekVersion returns entry version without cloning Value (and without flushing dirty sets).
	PeekVersion(key string) (uint64, bool)
	// SetCard returns element count (0 if missing).
	SetCard(key string) int
	// SetMembers returns defensive copies of all members.
	SetMembers(key string) [][]byte
	// SetInstall installs a versioned full-set snapshot (handoff).
	SetInstall(key string, blob []byte, version uint64, expireAt int64) bool

	// ZAdd upserts member/score in a sorted set.
	ZAdd(key string, member []byte, score float64, version uint64, expireAt int64) bool
	// ZRem removes a member from a sorted set.
	ZRem(key string, member []byte, version uint64, expireAt int64) bool
	// ZScore returns score if present.
	ZScore(key string, member []byte) (float64, bool)
	// ZCard returns member count.
	ZCard(key string) int
	// HasZSet reports a live FlagZSet entry without cloning Value.
	HasZSet(key string) bool
	// ZRange returns members by rank (start/stop Redis-style).
	ZRange(key string, start, stop int) []ZMember
	// ZRangeByScore returns members in an inclusive score window.
	ZRangeByScore(key string, min, max float64) []ZMember
	// ZInstall installs a versioned full zset snapshot.
	ZInstall(key string, blob []byte, version uint64, expireAt int64) bool

	// GeoAdd upserts member position.
	GeoAdd(key string, member []byte, lon, lat float64, version uint64, expireAt int64) bool
	// GeoRem removes a member from a geo index.
	GeoRem(key string, member []byte, version uint64, expireAt int64) bool
	// GeoPos returns lon/lat if present.
	GeoPos(key string, member []byte) (lon, lat float64, ok bool)
	// GeoCard returns member count.
	GeoCard(key string) int
	// HasGeo reports a live FlagGeo entry without cloning Value.
	HasGeo(key string) bool
	// GeoDist returns meters between two members.
	GeoDist(key string, a, b []byte) (meters float64, ok bool)
	// GeoRadius returns members within radiusM (limit<=0 = all).
	GeoRadius(key string, lon, lat, radiusM float64, limit int) []GeoMember
	// GeoInstall installs a versioned full geo snapshot.
	GeoInstall(key string, blob []byte, version uint64, expireAt int64) bool

	LPush(key string, item []byte, version uint64, expireAt int64) bool
	RPush(key string, item []byte, version uint64, expireAt int64) bool
	// LPop pops the head. popped is false if missing/empty. applied is false if version-gated.
	LPop(key string, version uint64, expireAt int64) (item []byte, popped, applied bool)
	RPop(key string, version uint64, expireAt int64) (item []byte, popped, applied bool)
	LLen(key string) int
	HasList(key string) bool
	LIndex(key string, idx int) ([]byte, bool)
	LRange(key string, start, stop int) [][]byte
	LInstall(key string, blob []byte, version uint64, expireAt int64) bool

	// HSet upserts a field (creates the hash if missing).
	HSet(key string, field, value []byte, version uint64, expireAt int64) bool
	// HDel removes a field. Missing name: true, no insert.
	HDel(key string, field []byte, version uint64, expireAt int64) bool
	// HGet returns a copy of the field value.
	HGet(key string, field []byte) (value []byte, ok bool)
	HExists(key string, field []byte) bool
	HLen(key string) int
	HasHash(key string) bool
	// HGetAll returns copies in field-byte order.
	HGetAll(key string) []HashField
	// HInstall installs a versioned full-hash snapshot (incoming > local).
	HInstall(key string, blob []byte, version uint64, expireAt int64) bool

	// Close releases resources.
	Close()
}

// HashField is one field/value pair returned by HGetAll.
type HashField struct {
	Field []byte
	Value []byte
}

// GeoMember is a point returned by store geo queries.
type GeoMember struct {
	Member []byte
	Lon    float64
	Lat    float64
	Dist   float64
}

// ZMember is a scored sorted-set element returned by store range ops.
type ZMember struct {
	Member []byte
	Score  float64
}
