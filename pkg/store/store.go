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

	// Close releases resources.
	Close()
}
