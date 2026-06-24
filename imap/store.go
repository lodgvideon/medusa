package imap

import (
	"bytes"
	"encoding/binary"
	"sync"
	"time"

	"github.com/lodgvideon/medusa/partition"
)

// nowNano returns the current time in unix nanoseconds. It is the single clock
// source for entry expiry.
func nowNano() int64 { return time.Now().UnixNano() }

// value is a stored entry: its bytes plus an optional absolute expiry time
// (unix nanoseconds, 0 = never expires).
type value struct {
	data     []byte
	expireAt int64
}

func (v value) expired(now int64) bool { return v.expireAt != 0 && now > v.expireAt }

// store holds the entries this node is responsible for, sharded by partition so
// operations on different partitions never contend on the same lock.
//
// Read lookups use m[string(key)], which the Go compiler lowers to an
// allocation-free map access. Writes copy the key (into the map key) and the
// value (so the store owns it independent of the caller's buffer). Expired
// entries are reported absent on read (lazy expiry) and reclaimed by
// sweepExpired (active expiry).
type store struct {
	shards [partition.Count]shard
}

type shard struct {
	mu sync.RWMutex
	m  map[string]map[string]value // map name -> key -> value
}

func newStore() *store {
	s := &store{}
	for i := range s.shards {
		s.shards[i].m = make(map[string]map[string]value)
	}
	return s
}

// get returns the value for key in the named map within partition p, treating
// an expired entry as absent. The returned slice aliases internal storage;
// callers must treat it as read-only.
func (s *store) get(p int, name string, key []byte) ([]byte, bool) {
	sh := &s.shards[p]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	inner := sh.m[name]
	if inner == nil {
		return nil, false
	}
	v, ok := inner[string(key)] // allocation-free map lookup
	if !ok || v.expired(nowNano()) {
		return nil, false
	}
	return v.data, true
}

// lookup returns the current value and absolute expiry for key, treating an
// expired entry as absent. Like get, the returned slice aliases internal storage
// and must be treated as read-only. Anti-entropy re-reads an entry with this just
// before re-pushing it, so a key the owner deleted (or changed) since the
// snapshot is not resurrected or clobbered on a backup.
func (s *store) lookup(p int, name string, key []byte) (data []byte, expireAt int64, ok bool) {
	sh := &s.shards[p]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	inner := sh.m[name]
	if inner == nil {
		return nil, 0, false
	}
	v, ok := inner[string(key)]
	if !ok || v.expired(nowNano()) {
		return nil, 0, false
	}
	return v.data, v.expireAt, true
}

// put stores a copy of value under key with the given absolute expiry (0 =
// never). It returns true when the key was newly created — overwriting an
// already-expired entry counts as a create.
func (s *store) put(p int, name string, key, data []byte, expireAt int64) bool {
	sh := &s.shards[p]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	inner := sh.m[name]
	if inner == nil {
		inner = make(map[string]value)
		sh.m[name] = inner
	}
	k := string(key)
	old, existed := inner[k]
	created := !existed || old.expired(nowNano())
	d := make([]byte, len(data))
	copy(d, data)
	inner[k] = value{data: d, expireAt: expireAt}
	return created
}

// entry is one key/value pair within a partition, used during migration. It
// carries the absolute expiry so a migrated entry keeps its lifetime.
type entry struct {
	mapName  string
	key      string
	value    []byte
	expireAt int64
}

// snapshotPartition returns a copy of every live entry in partition p, across
// all maps. Expired entries are skipped. Values are copied so the caller may
// use them after the lock drops.
func (s *store) snapshotPartition(p int) []entry {
	now := nowNano()
	sh := &s.shards[p]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	var out []entry
	for name, inner := range sh.m {
		for k, v := range inner {
			if v.expired(now) {
				continue
			}
			vc := make([]byte, len(v.data))
			copy(vc, v.data)
			out = append(out, entry{mapName: name, key: k, value: vc, expireAt: v.expireAt})
		}
	}
	return out
}

// partitionDigest returns an order-independent content hash of partition p's
// live entries: the XOR of a per-entry FNV-1a hash over (mapName, key, value).
// Two replicas holding the same set of (map,key,value) triples produce the same
// digest, so anti-entropy can detect divergence without transferring data.
//
// Each field is LENGTH-PREFIXED (big-endian uint32) before hashing rather than
// separated by a sentinel byte: keys and values are arbitrary binary (proto
// bytes), so any fixed separator can appear inside a field and create a
// boundary ambiguity — e.g. (key="k\x00",value="v") and (key="k",value="\x00v")
// would otherwise hash identically and XOR-cancel, falsely matching an empty
// backup and suppressing a needed heal. Length-prefixing makes the per-entry
// stream injective over (map,key,value) regardless of byte content.
//
// Expiry is intentionally excluded: replicas derive a fresh absolute expireAt
// from the remaining TTL at the instant each receives the write, so it legitimately
// differs for the same logical entry — including it would make in-sync replicas
// look diverged. A clock-skew-bounded TTL bucket could be added later if needed.
func (s *store) partitionDigest(p int) uint64 {
	now := nowNano()
	sh := &s.shards[p]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	var digest uint64
	var lbuf [4]byte
	for name, inner := range sh.m {
		for k, v := range inner {
			if v.expired(now) {
				continue
			}
			h := uint64(fnvOffset64)
			binary.BigEndian.PutUint32(lbuf[:], uint32(len(name)))
			h = fnv1a(h, lbuf[:])
			h = fnv1a(h, []byte(name))
			binary.BigEndian.PutUint32(lbuf[:], uint32(len(k)))
			h = fnv1a(h, lbuf[:])
			h = fnv1a(h, []byte(k))
			h = fnv1a(h, v.data) // terminal field — its length is implied
			digest ^= h
		}
	}
	return digest
}

// fnv1a is an allocation-free FNV-1a 64-bit hash step (no hash.Hash wrapper).
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

func fnv1a(h uint64, b []byte) uint64 {
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

// snapshotAll returns a copy of every live entry across all partitions, for
// persistence.
func (s *store) snapshotAll() []entry {
	var out []entry
	for p := range s.shards {
		out = append(out, s.snapshotPartition(p)...)
	}
	return out
}

// loadAll inserts entries into the store, routing each to its partition. Used
// to restore a snapshot on startup.
func (s *store) loadAll(entries []entry) {
	for _, e := range entries {
		k := []byte(e.key)
		s.put(partition.For(k), e.mapName, k, e.value, e.expireAt)
	}
}

// dropMigrated removes the given entries from partition p after they have been
// migrated away — but only those whose current stored value still matches what
// was captured (same bytes and expiry). An entry overwritten or newly created
// since the snapshot is left in place, so a write that raced the migration is
// never wiped: it stays local (served via backup fallback and moved on a later
// rebalance) rather than being silently lost. The compare-and-delete runs under
// the shard lock, so a concurrent write either lands before it (and is seen as
// changed, hence kept) or after it (and is never touched).
// It returns the entries it actually dropped so the caller can record those
// deletions durably (the WAL), preventing a crash before the next checkpoint
// from replaying the pre-drop snapshot and resurrecting handed-off data.
func (s *store) dropMigrated(p int, entries []entry) []entry {
	sh := &s.shards[p]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	var dropped []entry
	for _, e := range entries {
		inner := sh.m[e.mapName]
		if inner == nil {
			continue
		}
		if cur, ok := inner[e.key]; ok && cur.expireAt == e.expireAt && bytes.Equal(cur.data, e.value) {
			delete(inner, e.key)
			dropped = append(dropped, e)
		}
	}
	for name, inner := range sh.m {
		if len(inner) == 0 {
			delete(sh.m, name)
		}
	}
	return dropped
}

// sweepExpired removes expired entries across all shards, returning the count
// reclaimed. It runs periodically so memory is freed even for keys never read.
func (s *store) sweepExpired() int {
	now := nowNano()
	swept := 0
	for p := range s.shards {
		sh := &s.shards[p]
		sh.mu.Lock()
		for name, inner := range sh.m {
			for k, v := range inner {
				if v.expired(now) {
					delete(inner, k)
					swept++
				}
			}
			if len(inner) == 0 {
				delete(sh.m, name)
			}
		}
		sh.mu.Unlock()
	}
	return swept
}

// entryCount returns the number of live (non-expired) entries this node holds.
func (s *store) entryCount() int {
	now := nowNano()
	n := 0
	for p := range s.shards {
		sh := &s.shards[p]
		sh.mu.RLock()
		for _, inner := range sh.m {
			for _, v := range inner {
				if !v.expired(now) {
					n++
				}
			}
		}
		sh.mu.RUnlock()
	}
	return n
}

// countMap returns the number of live entries in the named map across the
// partitions for which owned reports true. Restricting to owned partitions lets
// a cluster-wide sum count each entry exactly once (never a backup copy).
func (s *store) countMap(name string, owned func(p int) bool) int {
	now := nowNano()
	n := 0
	for p := range s.shards {
		if !owned(p) {
			continue
		}
		sh := &s.shards[p]
		sh.mu.RLock()
		for _, v := range sh.m[name] {
			if !v.expired(now) {
				n++
			}
		}
		sh.mu.RUnlock()
	}
	return n
}

// collectOwnedValues returns the live values for the named map in the partitions
// owned reports true for — the input to a distributed aggregation's member-side
// reduce. Values alias internal storage (a stored value is immutable: put replaces
// the slice rather than mutating it, like get's returned slice), so they are safe
// to read after the lock drops without copying. Only owned partitions are visited,
// so a cluster-wide reduce folds each entry once and never a backup copy.
func (s *store) collectOwnedValues(name string, owned func(p int) bool) [][]byte {
	now := nowNano()
	var out [][]byte
	for p := range s.shards {
		if !owned(p) {
			continue
		}
		sh := &s.shards[p]
		sh.mu.RLock()
		for _, v := range sh.m[name] {
			if !v.expired(now) {
				out = append(out, v.data)
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// countOwned returns the number of live entries in the partitions owned reports
// true for. Eviction caps the OWNED count (not the total, which includes backup
// copies it cannot evict), so the cap is always reachable by evicting owned
// entries — never an over-eviction chasing a backup footprint it cannot shed.
func (s *store) countOwned(owned func(p int) bool) int {
	now := nowNano()
	n := 0
	for p := range s.shards {
		if !owned(p) {
			continue
		}
		sh := &s.shards[p]
		sh.mu.RLock()
		for _, inner := range sh.m {
			for _, v := range inner {
				if !v.expired(now) {
					n++
				}
			}
		}
		sh.mu.RUnlock()
	}
	return n
}

// sampleOwned collects up to limit live entries from the partitions owned reports
// true for, for max-size eviction. Map iteration order is unspecified, so the
// selection is effectively random — no per-access bookkeeping, so the read hot
// path stays allocation-free. Only owned partitions are sampled: evicting a backup
// copy would be futile (anti-entropy re-pushes it from the owner).
func (s *store) sampleOwned(owned func(p int) bool, limit int) []entry {
	if limit <= 0 {
		return nil
	}
	now := nowNano()
	out := make([]entry, 0, limit)
	for p := range s.shards {
		if !owned(p) {
			continue
		}
		sh := &s.shards[p]
		sh.mu.RLock()
		for name, inner := range sh.m {
			for k, v := range inner {
				if v.expired(now) {
					continue
				}
				out = append(out, entry{mapName: name, key: k})
				if len(out) >= limit {
					sh.mu.RUnlock()
					return out
				}
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// clearMap removes every entry for the named map across all shards — owner and
// backup copies alike — and returns the removed entries so the caller can record
// the deletions durably (the WAL). A broadcast of clearMap to all members empties
// the map cluster-wide. Already-expired entries are included (the whole map is
// going away); recording their removal is harmless and idempotent on replay.
func (s *store) clearMap(name string) []entry {
	var removed []entry
	for p := range s.shards {
		sh := &s.shards[p]
		sh.mu.Lock()
		if inner := sh.m[name]; len(inner) > 0 {
			for k, v := range inner {
				removed = append(removed, entry{mapName: name, key: k, value: v.data, expireAt: v.expireAt})
			}
			delete(sh.m, name)
		}
		sh.mu.Unlock()
	}
	return removed
}

// update applies fn to the current value of key atomically, under the shard
// lock, so concurrent updates to the same key are serialized. fn receives the
// live value (nil if absent/expired) and returns the new value plus an action.
// The entry's existing expiry is preserved on Set. It returns the resulting
// action and absolute expiry so the caller can replicate the new state.
func (s *store) update(p int, name string, key []byte, fn func(cur []byte, exists bool) (newVal []byte, action Action)) (Action, int64) {
	sh := &s.shards[p]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	inner := sh.m[name]
	if inner == nil {
		inner = make(map[string]value)
		sh.m[name] = inner
	}
	k := string(key)
	old, existed := inner[k]
	live := existed && !old.expired(nowNano())
	var cur []byte
	if live {
		cur = old.data
	}
	newVal, action := fn(cur, live)
	switch action {
	case Set:
		d := make([]byte, len(newVal))
		copy(d, newVal)
		expireAt := int64(0)
		if live {
			expireAt = old.expireAt // a processor does not change the entry's lifetime
		}
		inner[k] = value{data: d, expireAt: expireAt}
		return Set, expireAt
	case Delete:
		delete(inner, k)
		return Delete, 0
	default:
		return Keep, 0
	}
}

// remove deletes key from the named map, returning true if a live entry was
// present (an already-expired entry counts as absent).
func (s *store) remove(p int, name string, key []byte) bool {
	sh := &s.shards[p]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	inner := sh.m[name]
	if inner == nil {
		return false
	}
	k := string(key)
	v, ok := inner[k]
	if !ok {
		return false
	}
	delete(inner, k)
	return !v.expired(nowNano())
}
