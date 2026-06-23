package imap

import (
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

// dropPartition removes every entry in partition p across all maps.
func (s *store) dropPartition(p int) {
	sh := &s.shards[p]
	sh.mu.Lock()
	for name := range sh.m {
		delete(sh.m, name)
	}
	sh.mu.Unlock()
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
