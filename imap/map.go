package imap

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/lodgvideon/medusa/metrics"
	"github.com/lodgvideon/medusa/partition"
)

// errEmptyHolder guards the lock API: an empty holder is the lock's "free"
// sentinel, so it can never identify an owner.
var errEmptyHolder = errors.New("imap: lock holder must be non-empty")

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

// PutIfAbsent atomically stores value under key only if the key is currently
// absent, returning whether it was stored. It runs as a processor on the owner
// (atomic read-modify-write under the shard lock) and the result is replicated,
// so it is a correct distributed primitive for locks and leader election: a
// caller that gets true holds the key.
//
// Like every Execute op it is at-least-once under owner failover: if the owner
// applies and replicates the write but the response is lost, the retry runs on a
// backup that now holds the key and returns false — a false negative (you won but
// were told you lost). For strict mutual exclusion, store a value unique to the
// caller and, on a false/errored result, Get the key: if it equals your value you
// hold the lock. For a fence token that makes a stale holder detectable, use the
// Lock/Unlock fenced-lock API instead (note its monotonicity caveat).
func (mp *Map) PutIfAbsent(ctx context.Context, key, value []byte) (bool, error) {
	out, err := mp.Execute(ctx, key, "putifabsent", value)
	if err != nil {
		return false, err
	}
	return len(out) == 1 && out[0] == 1, nil
}

// CompareAndSwap atomically sets key to newVal only if it currently exists with
// a value equal to expected, returning whether the swap happened. It is the
// optimistic-concurrency primitive (compare-and-set). To create an absent key,
// use PutIfAbsent.
//
// Re-applying the same swap is a no-op (once the value is newVal it no longer
// equals expected), so retries never double-apply; but like PutIfAbsent the
// reported result is at-least-once — a lost response then a backup retry can
// return false for a swap that did happen. Read the value back if the outcome
// must be known exactly.
func (mp *Map) CompareAndSwap(ctx context.Context, key, expected, newVal []byte) (bool, error) {
	out, err := mp.Execute(ctx, key, "cas", encodeCAS(expected, newVal))
	if err != nil {
		return false, err
	}
	return len(out) == 1 && out[0] == 1, nil
}

// Lock attempts to acquire a fenced lock named key on behalf of holder (a value
// unique to the caller, e.g. a node id or UUID). It returns a monotonically
// increasing fence token and true on success, or 0 and false if the lock is held
// by a different holder. The token lets a holder prove ownership to downstream
// services so a stale holder (one paused past its turn) is detected.
//
// Acquiring a lock you already hold returns your existing token (idempotent), so
// a retry after an ambiguous/owner-failover call is safe — it returns the token
// rather than a false negative. Manage a lock key only through Lock/Unlock — not
// Put/Get/Remove.
//
// Monotonicity caveat: the fence is strictly increasing while one owner serves
// the key uncontended, but NOT guaranteed across (a) an ungraceful owner crash —
// a backup promoted after a crash may have missed the last acquire (best-effort
// replication) and reissue a live token; or (b) a partition migration — an
// acquire routed to the old owner on a stale table during the snapshot→handoff
// window is not propagated to the new owner, which then reissues the same token.
// Both stem from the AP, single-owner, best-effort-replication model; strict
// fencing needs synchronous/consensus replication or a quiescent handoff (see the
// roadmap). holder is a cooperative identity, not authenticated: any caller that
// presents a holder's id can release or re-enter that lock.
func (mp *Map) Lock(ctx context.Context, key, holder []byte) (token uint64, acquired bool, err error) {
	if len(holder) == 0 {
		return 0, false, errEmptyHolder
	}
	out, err := mp.Execute(ctx, key, "lockacquire", holder)
	if err != nil {
		return 0, false, err
	}
	if len(out) != 8 {
		return 0, false, nil // held by another holder
	}
	return binary.BigEndian.Uint64(out), true, nil
}

// Unlock releases a fenced lock held by holder, returning whether it was released
// (false if the caller did not hold it). The fence token is retained so the next
// acquire is strictly greater.
func (mp *Map) Unlock(ctx context.Context, key, holder []byte) (bool, error) {
	if len(holder) == 0 {
		return false, errEmptyHolder
	}
	out, err := mp.Execute(ctx, key, "lockrelease", holder)
	if err != nil {
		return false, err
	}
	return len(out) == 1 && out[0] == 1, nil
}

// Size returns the total number of live entries in the map across the whole
// cluster. It is a scatter-gather: every member counts the entries it owns and
// the counts are summed, so each entry is counted exactly once (a backup copy is
// never counted). The result is approximate during a rebalance, when ownership is
// briefly in flux. If a member is unreachable its share is omitted and the error
// is non-nil — the returned count is then a lower bound over the reachable members.
func (mp *Map) Size(ctx context.Context) (uint64, error) {
	var (
		total    uint64
		firstErr error
	)
	for _, m := range mp.svc.mem.Members() {
		if m.ID == mp.svc.self {
			total += uint64(mp.svc.localMapSize(mp.name))
			continue
		}
		c, err := mp.svc.sendSize(ctx, m.Addr, mp.name)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		total += c
	}
	return total, firstErr
}

// Clear removes every entry in the map across the whole cluster. It is a
// broadcast: each member drops all the copies it holds for the map (owner and
// backup), so a healthy cluster ends fully empty. If a member is unreachable its
// entries are not cleared and the error is non-nil — the clear is then partial,
// and (as with any delete in this AP design) a leftover copy could briefly be
// read via backup fallback until anti-entropy and a re-clear reconcile it.
func (mp *Map) Clear(ctx context.Context) error {
	var firstErr error
	for _, m := range mp.svc.mem.Members() {
		if m.ID == mp.svc.self {
			if _, err := mp.svc.localClearMap(mp.name); err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err := mp.svc.sendClear(ctx, m.Addr, mp.name); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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
