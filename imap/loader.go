package imap

import "sync"

// MapLoader loads an entry from an external system on a cache miss (read-through):
// when Get does not find a key in the in-memory store and a loader is configured
// for the map, the owner loads it, caches it locally, and returns it. It is the
// injection seam (SOLID dependency inversion) for backing a map with a database,
// an object store, or any external source — the core never names the concrete
// one. Load must be safe for concurrent use and should manage its own timeouts.
type MapLoader interface {
	Load(key []byte) (value []byte, found bool, err error)
}

// MapStore is a MapLoader that also persists writes (write-through) and deletes
// (delete-through): a Put stores to the external system and a Remove deletes from
// it, synchronously on the partition owner, so the backing store stays consistent
// with the grid. Evict deliberately does NOT delete through — it only drops the
// cached copy so the next read reloads. By interface segregation a read-only
// source implements MapLoader alone; a read/write backing store implements MapStore.
//
// Store and Delete must be IDEMPOTENT. Delete in particular fires on every Remove
// — including a key the grid has no cached entry for (it may have been evicted
// from the cache but still live in the backing store, and Remove must delete it
// there). Deleting an already-absent key must therefore be a no-op, not an error.
type MapStore interface {
	MapLoader
	Store(key, value []byte) error
	Delete(key []byte) error
}

// loaderRegistry holds the per-map MapLoader/MapStore the operator injects.
// Loaders are configured at startup and read on every miss/write, so a RWMutex
// keeps the read path cheap while still allowing late registration.
type loaderRegistry struct {
	mu sync.RWMutex
	m  map[string]MapLoader
}

func newLoaderRegistry() *loaderRegistry { return &loaderRegistry{m: map[string]MapLoader{}} }

func (r *loaderRegistry) set(name string, l MapLoader) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[name] = l
}

// loader returns the MapLoader for a map, or nil if none is configured.
func (r *loaderRegistry) loader(name string) MapLoader {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.m[name]
}

// store returns the configured loader as a MapStore, or nil if none is configured
// or it is read-only (a plain MapLoader) — so write/delete-through is skipped.
func (r *loaderRegistry) store(name string) MapStore {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ms, _ := r.m[name].(MapStore)
	return ms
}
