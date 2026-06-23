package imap

import (
	"context"
	"time"

	"github.com/lodgvideon/medusa/metrics"
	"github.com/lodgvideon/medusa/partition"
)

// Map is a handle to a named distributed map. Operations route to the partition
// owner; if the owner is unreachable they fall back to the backups in replica
// order, so reads and writes survive up to `Backups` simultaneous holder
// failures even before the failures are detected.
type Map struct {
	svc  *Service
	name string
}

// route resolves the partition and owner for key. ownerLocal reports whether
// this node owns it (so we can skip the network). A node's own membership always
// contains at least itself, so the owner is never empty.
func (mp *Map) route(key []byte) (p int, owner string, ownerLocal bool) {
	p = partition.For(key)
	tbl := mp.svc.mem.Table()
	ownerID := tbl.OwnerOf(p)
	ownerLocal = ownerID == mp.svc.self
	if !ownerLocal {
		owner, _ = mp.svc.mem.AddrOf(ownerID)
	}
	return p, owner, ownerLocal
}

// tryBackups invokes fn for each backup replica of partition p in replica order
// until fn reports done (it returns then) or the backups are exhausted. local
// flags a backup this node holds (addr is then ""), so the caller can serve it
// from the local store instead of the network. It runs only on the cold
// failover path, after the owner missed or was unreachable.
func (mp *Map) tryBackups(p int, fn func(local bool, addr string) (done bool)) {
	tbl := mp.svc.mem.Table()
	for i, n := 0, tbl.NumBackups(p); i < n; i++ {
		id, ok := tbl.BackupAt(p, i)
		if !ok {
			continue
		}
		if id == mp.svc.self {
			if fn(true, "") {
				return
			}
			continue
		}
		addr, ok := mp.svc.mem.AddrOf(id)
		if !ok {
			continue
		}
		if fn(false, addr) {
			return
		}
	}
}

// Put stores value under key with no expiry. The key/value buffers may be
// reused after Put returns; the store keeps its own copies.
func (mp *Map) Put(ctx context.Context, key, value []byte) error {
	return mp.putWithTTL(ctx, key, value, 0)
}

// PutTTL stores value under key with a time-to-live; the entry is reported
// absent once ttl elapses and is reclaimed in the background. A non-positive
// ttl means no expiry (same as Put).
func (mp *Map) PutTTL(ctx context.Context, key, value []byte, ttl time.Duration) error {
	return mp.putWithTTL(ctx, key, value, ttl.Milliseconds())
}

func (mp *Map) putWithTTL(ctx context.Context, key, value []byte, ttlMs int64) error {
	metrics.PutOps.Add(1)
	p, owner, ownerLocal := mp.route(key)

	if ownerLocal {
		_, err := mp.svc.applyPut(ctx, mp.name, key, value, ttlMs, false)
		return err
	}

	err := mp.svc.sendPut(ctx, owner, mp.name, key, value, ttlMs, false)
	if err == nil {
		return nil
	}
	// Owner unreachable: land the write on the first reachable backup so it is
	// not lost. Backup writes (isBackup=true) are logged to the WAL but not
	// re-replicated.
	landed := false
	mp.tryBackups(p, func(local bool, addr string) bool {
		var e error
		if local {
			_, e = mp.svc.applyPut(ctx, mp.name, key, value, ttlMs, true)
		} else {
			e = mp.svc.sendPut(ctx, addr, mp.name, key, value, ttlMs, true)
		}
		if e == nil {
			landed = true
			return true
		}
		err = e // remember the last failure
		return false
	})
	if landed {
		return nil
	}
	return err
}

// Get returns the value for key and whether it was found.
//
// The returned slice is read-only and, for a locally-owned key, aliases
// internal storage; callers must not modify it and should copy if they need to
// retain it across a concurrent write to the same key.
func (mp *Map) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	metrics.GetOps.Add(1)
	p, owner, ownerLocal := mp.route(key)

	// Try the owner first.
	if ownerLocal {
		if v, ok := mp.svc.store.get(p, mp.name, key); ok {
			return v, ok, nil
		}
	} else if v, ok, err := mp.svc.sendGet(ctx, owner, mp.name, key); err == nil && ok {
		return v, ok, nil
	}

	// The owner missed or was unreachable. During a rebalance the entry can still
	// live on a backup (an old holder the new owner has not received from yet),
	// so consult each backup in order before reporting the key absent. This is a
	// deliberate availability-favouring choice: it also means a key the (live)
	// owner reports absent is double-checked against backups, so a delete whose
	// replication to a backup failed could briefly read stale until anti-entropy
	// reconciles — an accepted trade-off in this AP design.
	var (
		val   []byte
		found bool
		gerr  error
	)
	mp.tryBackups(p, func(local bool, addr string) bool {
		if local {
			// The local store is always reachable, so this is a definitive
			// answer — clear any error from an unreachable earlier backup.
			val, found = mp.svc.store.get(p, mp.name, key)
			gerr = nil
			return found
		}
		v, ok, err := mp.svc.sendGet(ctx, addr, mp.name, key)
		if err != nil {
			gerr = err
			return false
		}
		val, found, gerr = v, ok, nil
		return ok
	})
	return val, found, gerr
}

// Execute runs the named server-side processor against key atomically on the
// partition owner and returns its result. Processors provide lock-free,
// single-round-trip read-modify-write: "incr", for example, is an atomic
// distributed counter with no lost updates under concurrency.
func (mp *Map) Execute(ctx context.Context, key []byte, processor string, arg []byte) ([]byte, error) {
	metrics.ExecuteOps.Add(1)
	p, owner, ownerLocal := mp.route(key)

	if ownerLocal {
		return mp.svc.applyExecute(ctx, mp.name, key, processor, arg)
	}
	out, err := mp.svc.sendExecute(ctx, owner, mp.name, key, processor, arg)
	if err == nil {
		return out, nil
	}
	// Owner unreachable: execute on the first reachable backup (it becomes owner
	// after eviction).
	done := false
	mp.tryBackups(p, func(local bool, addr string) bool {
		var (
			o []byte
			e error
		)
		if local {
			o, e = mp.svc.applyExecute(ctx, mp.name, key, processor, arg)
		} else {
			o, e = mp.svc.sendExecute(ctx, addr, mp.name, key, processor, arg)
		}
		if e == nil {
			out, done = o, true
			return true
		}
		err = e
		return false
	})
	if done {
		return out, nil
	}
	return nil, err
}

// Remove deletes key, returning whether it existed.
func (mp *Map) Remove(ctx context.Context, key []byte) (bool, error) {
	metrics.RemoveOps.Add(1)
	p, owner, ownerLocal := mp.route(key)

	if ownerLocal {
		return mp.svc.applyRemove(ctx, mp.name, key, false)
	}

	existed, err := mp.svc.sendRemove(ctx, owner, mp.name, key, false)
	if err == nil {
		return existed, nil
	}
	// Owner unreachable: apply the delete on the first reachable backup.
	done := false
	mp.tryBackups(p, func(local bool, addr string) bool {
		var (
			ex bool
			e  error
		)
		if local {
			ex, e = mp.svc.applyRemove(ctx, mp.name, key, true)
		} else {
			ex, e = mp.svc.sendRemove(ctx, addr, mp.name, key, true)
		}
		if e == nil {
			existed, done = ex, true
			return true
		}
		err = e
		return false
	})
	if done {
		return existed, nil
	}
	return false, err
}
