package imap

import (
	"context"
	"time"

	"github.com/lodgvideon/medusa/metrics"
	"github.com/lodgvideon/medusa/partition"
)

// Map is a handle to a named distributed map. Operations route to the partition
// owner; if the owner is unreachable they fall back to the backup, so reads and
// writes survive a single node failure even before the failure is detected.
type Map struct {
	svc  *Service
	name string
}

// route resolves the owner and backup for key. ownerLocal/backupLocal report
// whether this node holds that role (so we can skip the network). A node's own
// membership always contains at least itself, so the owner is never empty.
func (mp *Map) route(key []byte) (p int, owner, backup string, ownerLocal, backupLocal, hasBackup bool) {
	p = partition.For(key)
	tbl := mp.svc.mem.Table()
	ownerID := tbl.OwnerOf(p)
	ownerLocal = ownerID == mp.svc.self
	if !ownerLocal {
		owner, _ = mp.svc.mem.AddrOf(ownerID)
	}
	if backupID, ok := tbl.BackupOf(p); ok {
		hasBackup = true
		backupLocal = backupID == mp.svc.self
		if !backupLocal {
			backup, _ = mp.svc.mem.AddrOf(backupID)
		}
	}
	return p, owner, backup, ownerLocal, backupLocal, hasBackup
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
	_, owner, backup, ownerLocal, backupLocal, hasBackup := mp.route(key)

	if ownerLocal {
		_, err := mp.svc.applyPut(ctx, mp.name, key, value, ttlMs, false)
		return err
	}

	err := mp.svc.sendPut(ctx, owner, mp.name, key, value, ttlMs, false)
	if err == nil {
		return nil
	}
	// Owner unreachable: fall back to the backup so the write still lands. Route
	// through applyPut (as a backup write) so it is logged to the WAL too.
	if backupLocal {
		_, berr := mp.svc.applyPut(ctx, mp.name, key, value, ttlMs, true)
		return berr
	}
	if hasBackup {
		return mp.svc.sendPut(ctx, backup, mp.name, key, value, ttlMs, true)
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
	p, owner, backup, ownerLocal, backupLocal, hasBackup := mp.route(key)

	// Try the owner first.
	if ownerLocal {
		if v, ok := mp.svc.store.get(p, mp.name, key); ok {
			return v, ok, nil
		}
	} else if v, ok, err := mp.svc.sendGet(ctx, owner, mp.name, key); err == nil && ok {
		return v, ok, nil
	}

	// The owner missed or was unreachable. During a rebalance the entry can
	// still live on the backup (an old holder whose data the new owner has not
	// received yet), so consult the backup before reporting the key absent.
	if !hasBackup {
		return nil, false, nil
	}
	if backupLocal {
		v, ok := mp.svc.store.get(p, mp.name, key)
		return v, ok, nil
	}
	return mp.svc.sendGet(ctx, backup, mp.name, key)
}

// Execute runs the named server-side processor against key atomically on the
// partition owner and returns its result. Processors provide lock-free,
// single-round-trip read-modify-write: "incr", for example, is an atomic
// distributed counter with no lost updates under concurrency.
func (mp *Map) Execute(ctx context.Context, key []byte, processor string, arg []byte) ([]byte, error) {
	metrics.ExecuteOps.Add(1)
	_, owner, backup, ownerLocal, backupLocal, hasBackup := mp.route(key)

	if ownerLocal {
		return mp.svc.applyExecute(ctx, mp.name, key, processor, arg)
	}
	out, err := mp.svc.sendExecute(ctx, owner, mp.name, key, processor, arg)
	if err == nil {
		return out, nil
	}
	// Owner unreachable: execute on the backup (it becomes owner after eviction).
	if backupLocal {
		return mp.svc.applyExecute(ctx, mp.name, key, processor, arg)
	}
	if hasBackup {
		return mp.svc.sendExecute(ctx, backup, mp.name, key, processor, arg)
	}
	return nil, err
}

// Remove deletes key, returning whether it existed.
func (mp *Map) Remove(ctx context.Context, key []byte) (bool, error) {
	metrics.RemoveOps.Add(1)
	_, owner, backup, ownerLocal, backupLocal, hasBackup := mp.route(key)

	if ownerLocal {
		return mp.svc.applyRemove(ctx, mp.name, key, false)
	}

	existed, err := mp.svc.sendRemove(ctx, owner, mp.name, key, false)
	if err == nil {
		return existed, nil
	}
	if backupLocal {
		return mp.svc.applyRemove(ctx, mp.name, key, true)
	}
	if hasBackup {
		return mp.svc.sendRemove(ctx, backup, mp.name, key, true)
	}
	return false, err
}
