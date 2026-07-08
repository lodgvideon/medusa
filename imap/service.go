// Package imap implements a distributed map (a Hazelcast-style IMap): a named
// key/value store whose entries are partitioned across the cluster. Any node
// can serve any key by routing to the partition owner; writes are replicated
// to one backup so a single node failure does not lose data.
package imap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lodgvideon/medusa/cluster"
	"github.com/lodgvideon/medusa/codec"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/metrics"
	"github.com/lodgvideon/medusa/partition"
	"github.com/lodgvideon/medusa/transport"
)

// Service hosts this node's share of every distributed map and answers map
// RPCs from peers. Obtain per-map handles with Map.
type Service struct {
	self    string
	mem     *cluster.Membership
	tr      transport.Transport
	store   *store
	wal     *wal            // nil when durability logging is disabled (no DataDir)
	events  *listeners      // injected entry-event handlers; dormant until one is added
	loaders *loaderRegistry // per-map MapLoader/MapStore for read/write-through
}

// NewService creates a map service bound to a membership and transport.
func NewService(mem *cluster.Membership, tr transport.Transport) *Service {
	return &Service{self: mem.Self().ID, mem: mem, tr: tr, store: newStore(), events: newListeners(), loaders: newLoaderRegistry()}
}

// SetMapLoader configures a read-through loader for the named map: a Get that
// misses loads the entry from it and caches it. SetMapStore additionally enables
// write-through (Put) and delete-through (Remove). Register the same loader on
// every node — read/write-through runs on whichever node owns the key. Configure
// at startup, before serving traffic. See MapLoader/MapStore.
func (s *Service) SetMapLoader(name string, l MapLoader) { s.loaders.set(name, l) }

// SetMapStore configures a read/write-through backing store for the named map.
func (s *Service) SetMapStore(name string, ms MapStore) { s.loaders.set(name, ms) }

// AddEntryListener registers fn to receive entry events for mutations this node
// owns. The first listener starts the background dispatcher; delivery is
// asynchronous and best-effort (see EntryListener). It is the injection seam for
// integrating with external systems.
func (s *Service) AddEntryListener(fn EntryListener) { s.events.add(fn) }

// Close releases service-level background resources (the entry-event dispatcher).
// The WAL is closed separately via CloseWAL.
func (s *Service) Close() { s.events.close() }

// Map returns a handle to the named distributed map.
func (s *Service) Map(name string) *Map { return &Map{svc: s, name: name} }

// logged runs a store mutation and its WAL append as one unit. When a WAL is
// enabled it holds w.mu across BOTH, so the store-apply order equals the WAL
// order — two concurrent conflicting writes cannot land in the store in one
// order yet the WAL in the other (which would make crash recovery replay a
// different final state than was live). The lock order is w.mu → shard.mu, the
// same nesting Checkpoint uses (w.mu then the snapshot's shard RLocks), so the
// two cannot deadlock. record is the WAL append (via appendLocked); it is not
// called when the WAL is disabled. mutate must perform the store change.
//
// Apply-before-log within the unit is deliberate: it lets Checkpoint capture the
// snapshot and truncate the WAL under w.mu knowing every truncated record is
// already reflected — truncation can never drop a write.
func (s *Service) logged(mutate func(), record func() error) error {
	if s.wal == nil {
		mutate()
		return nil
	}
	s.wal.mu.Lock()
	defer s.wal.mu.Unlock()
	mutate()
	return record()
}

// dropMigratedLogged drops the entries this node handed off and records the
// deletions in the WAL as one atomic unit, mirroring logged(): the WAL lock is
// held across BOTH the store drop and its WAL records, so the store-apply order
// equals the WAL order across the hand-off. That closes two crash hazards a
// split drop-then-append would open:
//   - a concurrent put for a just-dropped key cannot slip its [PUT] into the WAL
//     between the drop and the [REMOVE] recorded here — which replay would apply
//     as [PUT][REMOVE], silently losing an acknowledged write;
//   - the drop and its record cannot straddle a crash with the key gone from the
//     store but absent from the WAL, which replay of the pre-drop snapshot would
//     resurrect for a partition this node no longer owns.
//
// dropMigrated returns only the entries actually removed (unchanged since the
// snapshot under the shard lock), so a write that raced the drop is neither
// wiped nor recorded as removed. A WAL append error stops the rest; the
// post-rebalance checkpoint is the durability backstop. The lock order is the
// same w.mu → shard.mu nesting logged() and Checkpoint use, so it cannot deadlock.
func (s *Service) dropMigratedLogged(p int, entries []entry) {
	if s.wal == nil {
		s.store.dropMigrated(p, entries)
		return
	}
	s.wal.mu.Lock()
	defer s.wal.mu.Unlock()
	for _, e := range s.store.dropMigrated(p, entries) {
		if err := s.wal.appendRemoveLocked(e.mapName, []byte(e.key)); err != nil {
			break
		}
	}
}

// pruneToKeysetLogged drops the keys in partition p that the owner no longer
// holds (everything not in keep) and records each deletion in the WAL as one
// atomic unit, mirroring dropMigratedLogged: the WAL lock is held across BOTH
// the store prune and its WAL records, so the store-apply order equals the WAL
// order and a crash mid-prune cannot leave a key gone from the store but absent
// from the WAL (which replay of the pre-prune snapshot would resurrect). Like a
// backup write, it fires NO entry listener and does NOT delete-through to a
// MapStore: the owner already did both when it processed the original Remove;
// this is internal convergence, not a new user-visible deletion. It returns the
// number of keys pruned.
func (s *Service) pruneToKeysetLogged(p int, keep map[string]map[string]struct{}) int {
	if s.wal == nil {
		return len(s.store.pruneToKeyset(p, keep))
	}
	s.wal.mu.Lock()
	defer s.wal.mu.Unlock()
	dropped := s.store.pruneToKeyset(p, keep)
	for _, e := range dropped {
		if err := s.wal.appendRemoveLocked(e.mapName, []byte(e.key)); err != nil {
			break // WAL append failed; the next checkpoint is the durability backstop
		}
	}
	return len(dropped)
}

// applyPut writes locally as the partition owner and, unless this is itself a
// backup write, replicates to the partition's backup. It returns whether the
// key was newly created. The store mutation and its WAL record are applied
// atomically under the WAL lock (see logged), and fsynced before returning, so
// an acknowledged write survives an ungraceful crash and replay reconstructs the
// live order; a WAL error is surfaced rather than silently dropped.
func (s *Service) applyPut(ctx context.Context, name string, key, value []byte, ttlMs int64, isBackup bool) (bool, error) {
	p := routedPartition(name, key)
	expireAt := expireFromTTL(ttlMs)
	var created bool
	if err := s.logged(
		func() { created = s.store.put(p, name, key, value, expireAt) },
		func() error {
			return s.wal.appendLocked(walOpPut, &medusav1.SnapshotEntry{Map: name, Key: key, Value: value, ExpireAt: expireAt})
		},
	); err != nil {
		return created, err
	}
	if !isBackup {
		s.replicate(ctx, p, medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST, name, key, value, ttlMs)
		if !IsReservedMap(name) { // reserved-namespace mutations are not user-visible events
			if created {
				s.events.emit(EventCreated, name, key, value)
			} else {
				s.events.emit(EventUpdated, name, key, value)
			}
		}
		if ms := s.loaders.store(name); ms != nil {
			// Write-through: persist to the backing store synchronously. The value
			// is already in memory + WAL and replicated, so a store failure is
			// surfaced for the caller to retry while the grid stays internally
			// consistent (the in-memory write is kept regardless of the external one).
			if err := ms.Store(key, value); err != nil {
				return created, err
			}
		}
	}
	return created, nil
}

// getThrough reads key locally with read-through: on a miss, if a loader is
// configured for the map, it loads the entry from the external source, caches it
// (cacheLoaded never clobbers a write that raced the load), and returns it. It
// runs on the partition OWNER — where a key is cached — so a loaded value is not
// replicated (the external store is the source of truth, and replicating would
// re-trigger write-through). A loader error is surfaced.
func (s *Service) getThrough(name string, key []byte) ([]byte, bool, error) {
	p := routedPartition(name, key)
	if v, ok := s.store.get(p, name, key); ok {
		return v, true, nil
	}
	loader := s.loaders.loader(name)
	if loader == nil {
		return nil, false, nil
	}
	// Read-through only on the partition OWNER. The GET handler is also reached via
	// Map.Get's backup-fallback (a query routed to a backup when the owner is
	// unreachable); a non-owner must NOT load — it would cache an orphaned copy the
	// owner never learns about and anti-entropy (owner→backup) never heals.
	if s.mem.Table().OwnerOf(p) != s.self {
		return nil, false, nil
	}
	v, found, err := loader.Load(key)
	if err != nil || !found {
		return nil, found, err
	}
	return s.store.cacheLoaded(p, name, key, v), true, nil
}

// expireFromTTL turns a relative TTL in milliseconds into an absolute expiry
// time (unix nanoseconds); a non-positive TTL means "never expires".
func expireFromTTL(ttlMs int64) int64 {
	if ttlMs <= 0 {
		return 0
	}
	return nowNano() + ttlMs*int64(time.Millisecond)
}

// applyRemove deletes locally and, unless a backup write, replicates the delete.
// Like applyPut it records the deletion in the WAL before returning.
func (s *Service) applyRemove(ctx context.Context, name string, key []byte, isBackup bool) (bool, error) {
	p := routedPartition(name, key)
	var existed bool
	if err := s.logged(
		func() { existed = s.store.remove(p, name, key) },
		func() error {
			return s.wal.appendLocked(walOpRemove, &medusav1.SnapshotEntry{Map: name, Key: key})
		},
	); err != nil {
		return existed, err
	}
	if !isBackup {
		s.replicate(ctx, p, medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST, name, key, nil, 0)
		if !IsReservedMap(name) && existed {
			s.events.emit(EventRemoved, name, key, nil)
		}
		if ms := s.loaders.store(name); ms != nil {
			// Delete-through fires even when the grid held no cached entry (existed
			// may be false): a key can be evicted from the cache yet still live in
			// the backing store, and Remove must delete it there too — gating on
			// existed would make an evicted-but-persisted key undeletable. Delete is
			// contractually idempotent, so an absent-key delete is a harmless no-op.
			if err := ms.Delete(key); err != nil {
				return existed, err
			}
		}
	}
	return existed, nil
}

// applyEvict drops key from the in-memory store and replicates the drop to the
// backups, but — unlike applyRemove — does NOT delete through to a MapStore and
// does NOT fire an entry event: it sheds the cached copy so the next read reloads
// through the MapLoader. The store mutation and its WAL record are one unit
// (logged), like every owner write. It is owner-only (Map.Evict routes here).
func (s *Service) applyEvict(ctx context.Context, name string, key []byte) (bool, error) {
	p := routedPartition(name, key)
	var existed bool
	if err := s.logged(
		func() { existed = s.store.remove(p, name, key) },
		func() error {
			return s.wal.appendLocked(walOpRemove, &medusav1.SnapshotEntry{Map: name, Key: key})
		},
	); err != nil {
		return existed, err
	}
	// Drop the backups' copies too (a plain backup remove — no delete-through there).
	s.replicate(ctx, p, medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST, name, key, nil, 0)
	return existed, nil
}

// replicate forwards a write to every backup member of the partition,
// best-effort. With a replication factor of N this loops over all N backups so
// the data survives up to N simultaneous holder failures. The loop is
// allocation-free: backups are read by index, not collected into a slice.
func (s *Service) replicate(ctx context.Context, p int, op medusav1.MessageType, name string, key, value []byte, ttlMs int64) {
	tbl := s.mem.Table()
	for i, n := 0, tbl.NumBackups(p); i < n; i++ {
		backup, ok := tbl.BackupAt(p, i)
		if !ok || backup == s.self {
			continue
		}
		addr, ok := s.mem.AddrOf(backup)
		if !ok {
			continue
		}
		switch op {
		case medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST:
			_ = s.sendPut(ctx, addr, name, key, value, ttlMs, true)
		case medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST:
			_, _ = s.sendRemove(ctx, addr, name, key, true)
		}
	}
}

func (s *Service) sendPut(ctx context.Context, addr, name string, key, value []byte, ttlMs int64, backup bool) error {
	reqBuf := codec.GetBuf()
	defer codec.PutBuf(reqBuf)
	req := medusav1.PutRequest{Map: name, Key: key, Value: value, Backup: backup, TtlMs: ttlMs}
	rb, err := codec.Marshal((*reqBuf)[:0], &req)
	if err != nil {
		return err
	}
	*reqBuf = rb
	respType, _, err := s.tr.Send(ctx, addr, medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST, rb, nil)
	if err != nil {
		return err
	}
	if respType != medusav1.MessageType_MESSAGE_TYPE_PUT_RESPONSE {
		return fmt.Errorf("imap: unexpected put response type %v", respType)
	}
	return nil
}

func (s *Service) sendGet(ctx context.Context, addr, name string, key []byte) ([]byte, bool, error) {
	reqBuf := codec.GetBuf()
	dstBuf := codec.GetBuf()
	defer codec.PutBuf(reqBuf)
	defer codec.PutBuf(dstBuf)

	req := medusav1.GetRequest{Map: name, Key: key}
	rb, err := codec.Marshal((*reqBuf)[:0], &req)
	if err != nil {
		return nil, false, err
	}
	*reqBuf = rb

	respType, resp, err := s.tr.Send(ctx, addr, medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST, rb, (*dstBuf)[:0])
	*dstBuf = resp
	if err != nil {
		return nil, false, err
	}
	if respType != medusav1.MessageType_MESSAGE_TYPE_GET_RESPONSE {
		return nil, false, fmt.Errorf("imap: unexpected get response type %v", respType)
	}
	var gr medusav1.GetResponse
	if err := gr.UnmarshalVT(resp); err != nil {
		return nil, false, err
	}
	// gr.Value was allocated fresh by UnmarshalVT, so it is safe to return even
	// after dstBuf goes back to the pool.
	return gr.Value, gr.Found, nil
}

func (s *Service) sendRemove(ctx context.Context, addr, name string, key []byte, backup bool) (bool, error) {
	reqBuf := codec.GetBuf()
	dstBuf := codec.GetBuf()
	defer codec.PutBuf(reqBuf)
	defer codec.PutBuf(dstBuf)

	req := medusav1.RemoveRequest{Map: name, Key: key, Backup: backup}
	rb, err := codec.Marshal((*reqBuf)[:0], &req)
	if err != nil {
		return false, err
	}
	*reqBuf = rb

	respType, resp, err := s.tr.Send(ctx, addr, medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST, rb, (*dstBuf)[:0])
	*dstBuf = resp
	if err != nil {
		return false, err
	}
	if respType != medusav1.MessageType_MESSAGE_TYPE_REMOVE_RESPONSE {
		return false, fmt.Errorf("imap: unexpected remove response type %v", respType)
	}
	var rr medusav1.RemoveResponse
	if err := rr.UnmarshalVT(resp); err != nil {
		return false, err
	}
	return rr.Existed, nil
}

// sendEvict asks the owner to evict key (drop the cached copy without deleting it
// from any backing store).
func (s *Service) sendEvict(ctx context.Context, addr, name string, key []byte) (bool, error) {
	reqBuf := codec.GetBuf()
	dstBuf := codec.GetBuf()
	defer codec.PutBuf(reqBuf)
	defer codec.PutBuf(dstBuf)

	rb, err := codec.Marshal((*reqBuf)[:0], &medusav1.EvictRequest{Map: name, Key: key})
	if err != nil {
		return false, err
	}
	*reqBuf = rb

	respType, resp, err := s.tr.Send(ctx, addr, medusav1.MessageType_MESSAGE_TYPE_EVICT_REQUEST, rb, (*dstBuf)[:0])
	*dstBuf = resp
	if err != nil {
		return false, err
	}
	if respType != medusav1.MessageType_MESSAGE_TYPE_EVICT_RESPONSE {
		return false, fmt.Errorf("imap: unexpected evict response type %v", respType)
	}
	var er medusav1.EvictResponse
	if err := er.UnmarshalVT(resp); err != nil {
		return false, err
	}
	return er.Existed, nil
}

// sendDigest asks a backup whether its content digest for partition p matches
// the owner's. The caller treats any error (or unexpected response) as "not in
// sync" and falls back to pushing, so a digest probe never blocks reconciliation.
func (s *Service) sendDigest(ctx context.Context, addr string, p int, digest uint64) (bool, error) {
	reqBuf := codec.GetBuf()
	dstBuf := codec.GetBuf()
	defer codec.PutBuf(reqBuf)
	defer codec.PutBuf(dstBuf)

	req := medusav1.DigestRequest{Partition: uint32(p), Digest: digest}
	rb, err := codec.Marshal((*reqBuf)[:0], &req)
	if err != nil {
		return false, err
	}
	*reqBuf = rb

	respType, resp, err := s.tr.Send(ctx, addr, medusav1.MessageType_MESSAGE_TYPE_DIGEST_REQUEST, rb, (*dstBuf)[:0])
	*dstBuf = resp
	if err != nil {
		return false, err
	}
	if respType != medusav1.MessageType_MESSAGE_TYPE_DIGEST_RESPONSE {
		return false, fmt.Errorf("imap: unexpected digest response type %v", respType)
	}
	var dr medusav1.DigestResponse
	if err := dr.UnmarshalVT(resp); err != nil {
		return false, err
	}
	return dr.Match, nil
}

// sendReconcile sends a backup the owner's authoritative key set for partition p
// and returns how many zombie keys the backup pruned (keys it held that the
// owner no longer does). Any transport error is returned; the caller treats it
// as best-effort (the next anti-entropy tick retries), so a prune never blocks
// the maintenance loop.
func (s *Service) sendReconcile(ctx context.Context, addr string, p int, keys []*medusav1.KeyRef) (uint64, error) {
	reqBuf := codec.GetBuf()
	dstBuf := codec.GetBuf()
	defer codec.PutBuf(reqBuf)
	defer codec.PutBuf(dstBuf)

	req := medusav1.ReconcileRequest{Partition: uint32(p), Keys: keys}
	rb, err := codec.Marshal((*reqBuf)[:0], &req)
	if err != nil {
		return 0, err
	}
	*reqBuf = rb

	respType, resp, err := s.tr.Send(ctx, addr, medusav1.MessageType_MESSAGE_TYPE_RECONCILE_REQUEST, rb, (*dstBuf)[:0])
	*dstBuf = resp
	if err != nil {
		return 0, err
	}
	if respType != medusav1.MessageType_MESSAGE_TYPE_RECONCILE_RESPONSE {
		return 0, fmt.Errorf("imap: unexpected reconcile response type %v", respType)
	}
	var rr medusav1.ReconcileResponse
	if err := rr.UnmarshalVT(resp); err != nil {
		return 0, err
	}
	return rr.Pruned, nil
}

// applyExecute runs a named processor against key atomically on this (owner)
// node and replicates the resulting state to the backup. It returns the
// processor's result.
func (s *Service) applyExecute(ctx context.Context, name string, key []byte, procName string, arg []byte) ([]byte, error) {
	// Queue operations are transactional (multi-key: metadata + a segment) and are
	// intercepted here rather than run as single-key processors. The segment
	// namespace accepts no direct Executes at all.
	if name == queueMap {
		return s.applyQueueExecute(ctx, string(key), procName, arg)
	}
	if name == queueSegMap {
		return nil, errReservedMap
	}
	proc, ok := lookupProcessor(procName)
	if !ok {
		return nil, fmt.Errorf("imap: unknown processor %q", procName)
	}
	p := partition.For(key)
	var newVal, out []byte
	var action Action
	var expireAt int64
	var existedBefore bool // whether the key was live before the processor ran (Created vs Updated)
	// The store update (which runs the processor under the shard lock) and the
	// resulting WAL record are applied atomically under the WAL lock, so a
	// concurrent conflicting write cannot interleave their store/WAL orders.
	if err := s.logged(
		func() {
			action, expireAt = s.store.update(p, name, key, func(cur []byte, exists bool) ([]byte, Action) {
				existedBefore = exists
				var a Action
				newVal, a, out = proc(cur, exists, arg)
				return newVal, a
			})
		},
		func() error {
			switch action {
			case Set:
				return s.wal.appendLocked(walOpPut, &medusav1.SnapshotEntry{Map: name, Key: key, Value: newVal, ExpireAt: expireAt})
			case Delete:
				return s.wal.appendLocked(walOpRemove, &medusav1.SnapshotEntry{Map: name, Key: key})
			}
			return nil
		},
	); err != nil {
		return out, err
	}
	switch action {
	case Set:
		s.replicate(ctx, p, medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST, name, key, newVal, remainingTTLms(expireAt, nowNano()))
		// Suppress listener events for the reserved queue namespace: an offer/poll is
		// internal infrastructure, not a user-visible map mutation.
		if !IsReservedMap(name) {
			if existedBefore {
				s.events.emit(EventUpdated, name, key, newVal)
			} else {
				s.events.emit(EventCreated, name, key, newVal)
			}
		}
		if ms := s.loaders.store(name); ms != nil { // write-through, like applyPut
			if err := ms.Store(key, newVal); err != nil {
				return out, err
			}
		}
	case Delete:
		s.replicate(ctx, p, medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST, name, key, nil, 0)
		if !IsReservedMap(name) && existedBefore {
			s.events.emit(EventRemoved, name, key, nil)
		}
		if ms := s.loaders.store(name); ms != nil { // delete-through, like applyRemove
			if err := ms.Delete(key); err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

// applyQueueExecute runs one queue operation as a TRANSACTION on this (owner)
// node: queueOp mutates the queue's co-located entries — metadata plus the
// head/tail segment — inside a single store.updateMulti critical section (the
// shard lock), itself inside the WAL lock (logged), so the store changes and
// their WAL records are one atomic unit exactly like every other owner write.
// The applied mutations are then replicated to the backups entry-by-entry
// (best-effort, like all replication). Queue ops fire no entry listeners and
// never write-through — the reserved namespaces are internal infrastructure.
func (s *Service) applyQueueExecute(ctx context.Context, qname, op string, arg []byte) ([]byte, error) {
	p := partition.For([]byte(qname)) // queue state is partition-affine on the name
	var (
		out  []byte
		muts []mutation
		oerr error
	)
	if err := s.logged(
		func() {
			muts = s.store.updateMulti(p, func(get getFn, set setFn, del delFn) {
				out, oerr = queueOp(qname, op, arg, get, set, del)
			})
		},
		func() error {
			for _, m := range muts {
				if m.del {
					if err := s.wal.appendRemoveLocked(m.mapName, m.key); err != nil {
						return err
					}
				} else if err := s.wal.appendLocked(walOpPut, &medusav1.SnapshotEntry{Map: m.mapName, Key: m.key, Value: m.value}); err != nil {
					return err
				}
			}
			return nil
		},
	); err != nil {
		return out, err
	}
	if oerr != nil {
		return nil, oerr // unknown op: nothing was mutated
	}
	for _, m := range muts {
		if m.del {
			s.replicate(ctx, p, medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST, m.mapName, m.key, nil, 0)
		} else {
			s.replicate(ctx, p, medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST, m.mapName, m.key, m.value, 0)
		}
	}
	return out, nil
}

func (s *Service) sendExecute(ctx context.Context, addr, name string, key []byte, procName string, arg []byte) ([]byte, error) {
	reqBuf := codec.GetBuf()
	dstBuf := codec.GetBuf()
	defer codec.PutBuf(reqBuf)
	defer codec.PutBuf(dstBuf)

	req := medusav1.ExecuteRequest{Map: name, Key: key, Processor: procName, Arg: arg}
	rb, err := codec.Marshal((*reqBuf)[:0], &req)
	if err != nil {
		return nil, err
	}
	*reqBuf = rb

	respType, resp, err := s.tr.Send(ctx, addr, medusav1.MessageType_MESSAGE_TYPE_EXECUTE_REQUEST, rb, (*dstBuf)[:0])
	*dstBuf = resp
	if err != nil {
		return nil, err
	}
	if respType != medusav1.MessageType_MESSAGE_TYPE_EXECUTE_RESPONSE {
		return nil, fmt.Errorf("imap: unexpected execute response type %v", respType)
	}
	var er medusav1.ExecuteResponse
	if err := er.UnmarshalVT(resp); err != nil {
		return nil, err
	}
	return er.Result, nil
}

// Migrate moves locally-held entries to the holders dictated by table. For each
// partition this node has data for, it pushes the entries to the partition's
// new owner and backup, then drops them locally if this node is no longer a
// holder.
//
// It is best-effort and idempotent: redundant pushes carry the same value and
// are harmless, and a partition is dropped only after every required push
// succeeds — so a failed push leaves the data in place to be retried on the
// next membership change. Triggered when the partition table changes (a node
// joining or leaving), it is what makes ownership and data move together.
//
// It honours ctx: on cancellation or deadline it stops between partitions and
// returns false (an incomplete pass). A full pass returns true. The caller can
// time-box it so a slow or unreachable peer cannot freeze the maintenance loop,
// and retry on the next tick (the partial work done is idempotent) until a pass
// completes.
func (s *Service) Migrate(ctx context.Context, table *partition.Table) bool {
	for p := 0; p < partition.Count; p++ {
		if ctx.Err() != nil {
			return false // time-boxed or cancelled mid-pass; caller retries
		}
		// Queue metadata is pushed after its segments (orderQueueMetaLast), so the
		// receiver never serves a queue whose metadata references segments that
		// have not arrived yet.
		entries := orderQueueMetaLast(s.store.snapshotPartition(p))
		if len(entries) == 0 {
			continue
		}
		owner := table.OwnerOf(p)
		selfHolds := owner == s.self

		// Every holder this node must push to: the owner plus all backups,
		// minus itself. If this node is among them it keeps its copy.
		targets := make([]string, 0, 1+table.NumBackups(p))
		if owner != "" && owner != s.self {
			targets = append(targets, owner)
		}
		for i, n := 0, table.NumBackups(p); i < n; i++ {
			b, ok := table.BackupAt(p, i)
			if !ok {
				continue
			}
			if b == s.self {
				selfHolds = true
				continue
			}
			targets = append(targets, b)
		}

		pushedOK := true
		for _, id := range targets {
			addr, ok := s.mem.AddrOf(id)
			if !ok {
				pushedOK = false
				continue
			}
			now := nowNano()
			for _, e := range entries {
				ttlMs := remainingTTLms(e.expireAt, now)
				if e.expireAt != 0 && ttlMs <= 0 {
					continue // expired in flight; let it lapse
				}
				if err := s.sendPut(ctx, addr, e.mapName, []byte(e.key), e.value, ttlMs, true); err != nil {
					pushedOK = false
				}
			}
		}
		if !selfHolds && pushedOK {
			// Drop only the entries we actually migrated and that are unchanged —
			// never a blanket wipe — so a write that raced this migration survives,
			// and record the deletions in the WAL atomically with the drop so a
			// crash can neither lose a racing write nor resurrect handed-off data
			// (see dropMigratedLogged). The post-rebalance checkpoint folds these
			// remove records into the snapshot and is the backstop on WAL failure.
			s.dropMigratedLogged(p, entries)
		}
	}
	return true
}

// SyncBackups is active anti-entropy: it re-pushes the entries of the partitions
// this node OWNS, starting at partition `start` and covering `batch` of them, to
// their current backups — so a replica that missed a best-effort write (a
// transient blip during replicate, before the owner could evict it) converges to
// the owner's state. It returns how many entries were pushed and the partition
// index to resume from next time, so the caller can rotate through all
// partitions over successive maintenance ticks at a bounded per-tick cost.
//
// It is a slow background safety net beneath the synchronous-replication fast
// path and is idempotent (redundant pushes carry the same value). It heals both
// divergence directions:
//   - PUSH heals a backup that is MISSING or STALE for a key (re-sends the value);
//   - PRUNE heals a backup that KEPT a key after missing the owner's delete (a
//     "zombie") — the owner sends its authoritative key set for the partition and
//     the backup drops any key not in it. Without prune a missed delete lingered
//     until restart or migration and could resurrect on failover.
//
// Only owned partitions are reconciled, so the owner stays the single source of
// truth: its current content fully defines what a backup must hold (medusa is
// single-owner, so there are no conflicting cross-node writes to merge — making
// owner-authoritative keyset reconciliation simpler and stronger than per-entry
// versioning would be here).
//
// Each partition is digest-gated: the owner sends each backup its content digest
// and only pushes the data and prunes for backups whose digest differs (or that
// fail the probe). In steady state, when replicas already match, a pass costs one
// tiny digest RPC per backup and transfers nothing.
//
// Each entry is re-read from the owner's store (lookup) immediately before it is
// pushed, and skipped if the owner no longer holds it — so a key deleted since
// the snapshot is neither resurrected on a backup nor kept in the key set the
// prune is judged against. This leaves only the same narrow re-read→send race
// Migrate has, healed on the next tick, instead of the whole snapshot→push window.
func (s *Service) SyncBackups(ctx context.Context, table *partition.Table, start, batch int) (pushed, next int) {
	now := nowNano()
	p := ((start % partition.Count) + partition.Count) % partition.Count
	for k := 0; k < batch; k++ {
		if table.OwnerOf(p) == s.self {
			// Probe each backup with the owner's digest; push only to those that
			// report a mismatch (diverged) or fail the probe (treat as stale).
			ownerDigest := s.store.partitionDigest(p)
			var stale []string
			for i, n := 0, table.NumBackups(p); i < n; i++ {
				b, ok := table.BackupAt(p, i)
				if !ok || b == s.self {
					continue
				}
				addr, ok := s.mem.AddrOf(b)
				if !ok {
					continue
				}
				if match, err := s.sendDigest(ctx, addr, p, ownerDigest); err != nil || !match {
					stale = append(stale, addr)
				}
			}
			if len(stale) > 0 {
				// Build the owner's authoritative key set for the partition while
				// pushing values: a key still live at push time joins the set; a key
				// deleted/expired since the snapshot is excluded from BOTH the push and
				// the set, so the backup neither resurrects nor keeps it.
				snap := orderQueueMetaLast(s.store.snapshotPartition(p)) // segments before queue metadata
				keyset := make([]*medusav1.KeyRef, 0, len(snap))
				for _, e := range snap {
					key := []byte(e.key)
					// Re-read the owner's CURRENT state: a key deleted or changed
					// since the snapshot must not be resurrected/clobbered on a
					// backup. Push the fresh value so backups converge to the owner.
					data, expireAt, ok := s.store.lookup(p, e.mapName, key)
					if !ok {
						continue // deleted/expired since the snapshot — don't resurrect
					}
					ttlMs := remainingTTLms(expireAt, now)
					if expireAt != 0 && ttlMs <= 0 {
						continue // expired in flight; let it lapse
					}
					keyset = append(keyset, &medusav1.KeyRef{Map: e.mapName, Key: key})
					sent := false
					for _, addr := range stale {
						if err := s.sendPut(ctx, addr, e.mapName, key, data, ttlMs, true); err == nil {
							sent = true
						}
					}
					if sent {
						pushed++ // count the entry once, not once per backup
					}
				}
				// Prune the delete-side: tell each stale backup to drop any key in
				// this partition the owner no longer holds. Best-effort — a failed
				// reconcile is retried on the next tick.
				for _, addr := range stale {
					if n, err := s.sendReconcile(ctx, addr, p, keyset); err == nil && n > 0 {
						metrics.Pruned.Add(int64(n))
					}
				}
			}
		}
		p = (p + 1) % partition.Count
	}
	return pushed, p
}

// remainingTTLms returns the milliseconds left until expireAt (0 for an entry
// that never expires).
func remainingTTLms(expireAt, now int64) int64 {
	if expireAt == 0 {
		return 0
	}
	return (expireAt - now) / int64(time.Millisecond)
}

// LocalEntryCount returns the number of live entries this node currently stores.
func (s *Service) LocalEntryCount() int { return s.store.entryCount() }

// Evict enforces a soft per-node cap: if this node OWNS more than max live
// entries, it removes up to batch of them (a roughly random selection) to drain
// toward the cap, replicating each removal (isBackup=false) so backups stay
// consistent — a normal distributed delete. It returns the number removed. max
// <= 0 disables it.
//
// The cap is on OWNED entries, not the total this node holds: backup copies
// cannot be evicted (anti-entropy would re-push them from the owner), so capping
// the total would over-evict — deleting all owned data chasing a backup footprint
// it cannot shed. Since every node caps its owned share, per-node memory is still
// bounded to about max*(1+Backups).
func (s *Service) Evict(ctx context.Context, max, batch int) int {
	if max <= 0 {
		return 0
	}
	tbl := s.mem.Table()
	owned := func(p int) bool { return tbl.OwnerOf(p) == s.self }
	n := s.store.countOwned(owned)
	if n <= max {
		return 0
	}
	excess := n - max
	if excess > batch {
		excess = batch
	}
	victims := s.store.sampleOwned(owned, excess)
	for _, e := range victims {
		// Evict, not Remove: shed the in-memory copy (and the backups') but do NOT
		// delete-through to a configured MapStore — max-size eviction frees memory,
		// it must not destroy the backing-store record (a later Get reloads it). It
		// also fires no entry-listener event (eviction is not a logical delete).
		_, _ = s.applyEvict(ctx, e.mapName, []byte(e.key))
	}
	return len(victims)
}

// localMapSize counts the live entries in the named map that this node OWNS
// (entries in partitions it is the current owner of), so a cluster-wide sum
// counts each entry once and never a backup copy.
func (s *Service) localMapSize(name string) int {
	tbl := s.mem.Table()
	return s.store.countMap(name, func(p int) bool { return tbl.OwnerOf(p) == s.self })
}

// localAggregate reduces this node's OWNED entries for the named map with the
// named aggregator, returning that member's partial. It is the member side of the
// distributed map-reduce (the caller combines partials from every member). The
// reduce runs outside the shard locks: collectOwnedValues snapshots the owned
// values (which alias immutable stored slices) under brief read locks, then the
// aggregator folds them — so a slow custom aggregator never holds a shard lock.
func (s *Service) localAggregate(name, aggName string) ([]byte, error) {
	agg, ok := lookupAggregator(aggName)
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownAggregator, aggName)
	}
	tbl := s.mem.Table()
	values := s.store.collectOwnedValues(name, func(p int) bool { return tbl.OwnerOf(p) == s.self })
	return agg.Reduce(values), nil
}

// localClearMap removes all entries this node holds for the named map (owner and
// backup copies) and records the clear durably as ONE WAL record, so a crash
// before the next checkpoint replays the clear (not the pre-clear entries). It
// returns the number removed and surfaces a WAL error to the caller — like the
// single-key paths, the clear is not acknowledged as successful unless its WAL
// record is durable.
func (s *Service) localClearMap(name string) (int, error) {
	var removed []entry
	// clearMap and its single WAL record run atomically under the WAL lock, so a
	// concurrent Put to the same map cannot land between them and then be ordered
	// before the clear in the WAL (which crash recovery would replay as a lost put).
	err := s.logged(
		func() { removed = s.store.clearMap(name) },
		func() error {
			if len(removed) == 0 {
				return nil
			}
			return s.wal.appendLocked(walOpClear, &medusav1.SnapshotEntry{Map: name})
		},
	)
	return len(removed), err
}

// sendClear tells a peer to clear all its entries for the named map.
func (s *Service) sendClear(ctx context.Context, addr, name string) (uint64, error) {
	reqBuf := codec.GetBuf()
	dstBuf := codec.GetBuf()
	defer codec.PutBuf(reqBuf)
	defer codec.PutBuf(dstBuf)

	rb, err := codec.Marshal((*reqBuf)[:0], &medusav1.ClearRequest{Map: name})
	if err != nil {
		return 0, err
	}
	*reqBuf = rb

	respType, resp, err := s.tr.Send(ctx, addr, medusav1.MessageType_MESSAGE_TYPE_CLEAR_REQUEST, rb, (*dstBuf)[:0])
	*dstBuf = resp
	if err != nil {
		return 0, err
	}
	if respType != medusav1.MessageType_MESSAGE_TYPE_CLEAR_RESPONSE {
		return 0, fmt.Errorf("imap: unexpected clear response type %v", respType)
	}
	var cr medusav1.ClearResponse
	if err := cr.UnmarshalVT(resp); err != nil {
		return 0, err
	}
	return cr.Removed, nil
}

// sendSize asks a peer for the count of entries it owns for the named map.
func (s *Service) sendSize(ctx context.Context, addr, name string) (uint64, error) {
	reqBuf := codec.GetBuf()
	dstBuf := codec.GetBuf()
	defer codec.PutBuf(reqBuf)
	defer codec.PutBuf(dstBuf)

	rb, err := codec.Marshal((*reqBuf)[:0], &medusav1.SizeRequest{Map: name})
	if err != nil {
		return 0, err
	}
	*reqBuf = rb

	respType, resp, err := s.tr.Send(ctx, addr, medusav1.MessageType_MESSAGE_TYPE_SIZE_REQUEST, rb, (*dstBuf)[:0])
	*dstBuf = resp
	if err != nil {
		return 0, err
	}
	if respType != medusav1.MessageType_MESSAGE_TYPE_SIZE_RESPONSE {
		return 0, fmt.Errorf("imap: unexpected size response type %v", respType)
	}
	var sr medusav1.SizeResponse
	if err := sr.UnmarshalVT(resp); err != nil {
		return 0, err
	}
	return sr.Count, nil
}

// sendAggregate asks a peer to reduce the entries it owns for the named map with
// the named aggregator, returning that member's partial.
func (s *Service) sendAggregate(ctx context.Context, addr, name, aggName string) ([]byte, error) {
	reqBuf := codec.GetBuf()
	dstBuf := codec.GetBuf()
	defer codec.PutBuf(reqBuf)
	defer codec.PutBuf(dstBuf)

	rb, err := codec.Marshal((*reqBuf)[:0], &medusav1.AggregateRequest{Map: name, Aggregator: aggName})
	if err != nil {
		return nil, err
	}
	*reqBuf = rb

	respType, resp, err := s.tr.Send(ctx, addr, medusav1.MessageType_MESSAGE_TYPE_AGGREGATE_REQUEST, rb, (*dstBuf)[:0])
	*dstBuf = resp
	if err != nil {
		// The transport flattens a peer's handler error to a message string, losing
		// the wrapped sentinel. A peer that doesn't know the aggregator is a config
		// error, NOT an unreachable peer: re-wrap it so the caller fails fast (and
		// the HTTP layer returns 400) rather than silently dropping that peer's
		// share and reporting a wrong partial. Matched on the sentinel's own text.
		var re *transport.RemoteError
		if errors.As(err, &re) && strings.Contains(re.Message, ErrUnknownAggregator.Error()) {
			return nil, fmt.Errorf("%w (peer %s)", ErrUnknownAggregator, addr)
		}
		return nil, err
	}
	if respType != medusav1.MessageType_MESSAGE_TYPE_AGGREGATE_RESPONSE {
		return nil, fmt.Errorf("imap: unexpected aggregate response type %v", respType)
	}
	var ar medusav1.AggregateResponse
	if err := ar.UnmarshalVT(resp); err != nil {
		return nil, err
	}
	// ar.Partial was freshly allocated by UnmarshalVT, so it is safe to retain
	// after dstBuf returns to the pool.
	return ar.Partial, nil
}

// Snapshot serializes every live entry on this node for persistence.
func (s *Service) Snapshot() *medusav1.Snapshot {
	entries := s.store.snapshotAll()
	snap := &medusav1.Snapshot{Entries: make([]*medusav1.SnapshotEntry, 0, len(entries))}
	for _, e := range entries {
		snap.Entries = append(snap.Entries, &medusav1.SnapshotEntry{
			Map: e.mapName, Key: []byte(e.key), Value: e.value, ExpireAt: e.expireAt,
		})
	}
	return snap
}

// Restore loads a snapshot's entries into the store, skipping any that have
// already expired.
func (s *Service) Restore(snap *medusav1.Snapshot) {
	now := nowNano()
	entries := make([]entry, 0, len(snap.GetEntries()))
	for _, se := range snap.GetEntries() {
		if se.ExpireAt != 0 && se.ExpireAt <= now {
			continue
		}
		entries = append(entries, entry{
			mapName: se.Map, key: string(se.Key), value: se.Value, expireAt: se.ExpireAt,
		})
	}
	s.store.loadAll(entries)
}

// OpenWAL replays any existing write-ahead log at path into the store (on top
// of an already-restored snapshot), then opens it for appending so subsequent
// writes are logged. Call it after Restore. Replayed puts that have already
// expired are skipped.
func (s *Service) OpenWAL(path string) error {
	now := nowNano()
	valid, err := replayWAL(path,
		func(name string, key, value []byte, expireAt int64) {
			if expireAt != 0 && expireAt <= now {
				return // expired while we were down
			}
			if name == queueMap && len(value) != 24 && len(value) > 0 {
				s.store.loadAll(convertLegacyQueue(string(key), value)) // legacy queue record
				return
			}
			s.store.put(routedPartition(name, key), name, key, value, expireAt)
		},
		func(name string, key []byte) {
			s.store.remove(routedPartition(name, key), name, key)
		},
		func(name string) {
			s.store.clearMap(name)
		},
	)
	if err != nil {
		return err
	}
	// Chop any torn/corrupt tail so future appends stay contiguous; otherwise a
	// later replay would stop at the remnant and miss everything after it.
	if err := truncateTail(path, valid); err != nil {
		return err
	}
	w, err := openWAL(path)
	if err != nil {
		return err
	}
	s.wal = w
	return nil
}

// CloseWAL closes the write-ahead log if one is open.
func (s *Service) CloseWAL() error {
	if s.wal == nil {
		return nil
	}
	return s.wal.close()
}

// Checkpoint durably persists a snapshot and then truncates the write-ahead log,
// bounding replay time. write is invoked with a freshly captured snapshot and
// must persist it durably; only on its success is the WAL truncated. The whole
// operation holds the WAL lock, so it cannot race a concurrent append: every
// record being discarded is already reflected in the snapshot just written.
// With no WAL it simply persists the snapshot.
//
// The lock is held across write's disk I/O, so concurrent writes stall for the
// snapshot-persist latency. That is fine at this scale (snapshots are small and
// periodic) and is required for the truncation-safety invariant above; doing
// the file write outside the lock would let an append land between snapshot
// capture and truncate and then be truncated away — losing it. Decoupling the
// two safely needs WAL segment rotation (write to a fresh segment, persist the
// snapshot, drop the old segment), which is on the roadmap.
func (s *Service) Checkpoint(write func(*medusav1.Snapshot) error) error {
	if s.wal == nil {
		return write(s.Snapshot())
	}
	s.wal.mu.Lock()
	defer s.wal.mu.Unlock()
	if err := write(s.Snapshot()); err != nil {
		return err
	}
	return s.wal.truncateLocked()
}

// SweepExpired reclaims expired entries, returning the count removed.
func (s *Service) SweepExpired() int { return s.store.sweepExpired() }

// Handle answers inbound map RPCs. It satisfies transport.Handler so the node's
// dispatcher can route PUT/GET/REMOVE here. The GET path is allocation-light:
// the value is looked up without copying and marshalled straight into respBuf.
func (s *Service) Handle(reqType medusav1.MessageType, req, respBuf []byte) (medusav1.MessageType, []byte, error) {
	switch reqType {
	case medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST:
		var pr medusav1.PutRequest
		if err := pr.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		created, aerr := s.applyPut(context.Background(), pr.Map, pr.Key, pr.Value, pr.TtlMs, pr.Backup)
		if aerr != nil {
			return 0, respBuf, aerr
		}
		out, err := codec.Marshal(respBuf, &medusav1.PutResponse{Created: created})
		return medusav1.MessageType_MESSAGE_TYPE_PUT_RESPONSE, out, err

	case medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST:
		var gr medusav1.GetRequest
		if err := gr.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		v, ok, lerr := s.getThrough(gr.Map, gr.Key) // read-through on a miss
		if lerr != nil {
			return 0, respBuf, lerr
		}
		out, err := codec.Marshal(respBuf, &medusav1.GetResponse{Found: ok, Value: v})
		return medusav1.MessageType_MESSAGE_TYPE_GET_RESPONSE, out, err

	case medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST:
		var rr medusav1.RemoveRequest
		if err := rr.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		existed, aerr := s.applyRemove(context.Background(), rr.Map, rr.Key, rr.Backup)
		if aerr != nil {
			return 0, respBuf, aerr
		}
		out, err := codec.Marshal(respBuf, &medusav1.RemoveResponse{Existed: existed})
		return medusav1.MessageType_MESSAGE_TYPE_REMOVE_RESPONSE, out, err

	case medusav1.MessageType_MESSAGE_TYPE_EXECUTE_REQUEST:
		var er medusav1.ExecuteRequest
		if err := er.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		out, err := s.applyExecute(context.Background(), er.Map, er.Key, er.Processor, er.Arg)
		if err != nil {
			return 0, respBuf, err
		}
		o, merr := codec.Marshal(respBuf, &medusav1.ExecuteResponse{Result: out})
		return medusav1.MessageType_MESSAGE_TYPE_EXECUTE_RESPONSE, o, merr

	case medusav1.MessageType_MESSAGE_TYPE_DIGEST_REQUEST:
		var dr medusav1.DigestRequest
		if err := dr.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		// Partition comes off the wire — bounds-check before indexing the shard
		// array so a malformed or incompatible peer cannot panic this node.
		if dr.Partition >= partition.Count {
			return 0, respBuf, fmt.Errorf("imap: digest request partition %d out of range", dr.Partition)
		}
		match := s.store.partitionDigest(int(dr.Partition)) == dr.Digest
		out, err := codec.Marshal(respBuf, &medusav1.DigestResponse{Match: match})
		return medusav1.MessageType_MESSAGE_TYPE_DIGEST_RESPONSE, out, err

	case medusav1.MessageType_MESSAGE_TYPE_SIZE_REQUEST:
		var sr medusav1.SizeRequest
		if err := sr.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		out, err := codec.Marshal(respBuf, &medusav1.SizeResponse{Count: uint64(s.localMapSize(sr.Map))})
		return medusav1.MessageType_MESSAGE_TYPE_SIZE_RESPONSE, out, err

	case medusav1.MessageType_MESSAGE_TYPE_CLEAR_REQUEST:
		var cr medusav1.ClearRequest
		if err := cr.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		removed, cerr := s.localClearMap(cr.Map)
		if cerr != nil {
			return 0, respBuf, cerr
		}
		out, err := codec.Marshal(respBuf, &medusav1.ClearResponse{Removed: uint64(removed)})
		return medusav1.MessageType_MESSAGE_TYPE_CLEAR_RESPONSE, out, err

	case medusav1.MessageType_MESSAGE_TYPE_AGGREGATE_REQUEST:
		var ar medusav1.AggregateRequest
		if err := ar.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		partial, aerr := s.localAggregate(ar.Map, ar.Aggregator)
		if aerr != nil {
			return 0, respBuf, aerr
		}
		out, err := codec.Marshal(respBuf, &medusav1.AggregateResponse{Partial: partial})
		return medusav1.MessageType_MESSAGE_TYPE_AGGREGATE_RESPONSE, out, err

	case medusav1.MessageType_MESSAGE_TYPE_EVICT_REQUEST:
		var er medusav1.EvictRequest
		if err := er.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		existed, eerr := s.applyEvict(context.Background(), er.Map, er.Key)
		if eerr != nil {
			return 0, respBuf, eerr
		}
		out, err := codec.Marshal(respBuf, &medusav1.EvictResponse{Existed: existed})
		return medusav1.MessageType_MESSAGE_TYPE_EVICT_RESPONSE, out, err

	case medusav1.MessageType_MESSAGE_TYPE_RECONCILE_REQUEST:
		var rr medusav1.ReconcileRequest
		if err := rr.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		if rr.Partition >= partition.Count {
			return 0, respBuf, fmt.Errorf("imap: reconcile request partition %d out of range", rr.Partition)
		}
		p := int(rr.Partition)
		// Owner-gate: only prune as a FOLLOWER. If this node believes it owns the
		// partition, it is the authority — a prune driven by a peer (e.g. a stale
		// ex-owner during a membership transition) must never delete the owner's
		// live data. Ignore it; the true owner reconciles us, not the reverse.
		if s.mem.Table().OwnerOf(p) == s.self {
			out, err := codec.Marshal(respBuf, &medusav1.ReconcileResponse{Pruned: 0})
			return medusav1.MessageType_MESSAGE_TYPE_RECONCILE_RESPONSE, out, err
		}
		keep := make(map[string]map[string]struct{}, len(rr.Keys))
		for _, kr := range rr.Keys {
			inner := keep[kr.Map]
			if inner == nil {
				inner = make(map[string]struct{})
				keep[kr.Map] = inner
			}
			inner[string(kr.Key)] = struct{}{}
		}
		pruned := s.pruneToKeysetLogged(p, keep)
		out, err := codec.Marshal(respBuf, &medusav1.ReconcileResponse{Pruned: uint64(pruned)})
		return medusav1.MessageType_MESSAGE_TYPE_RECONCILE_RESPONSE, out, err

	default:
		return 0, respBuf, fmt.Errorf("imap: unhandled message type %v", reqType)
	}
}
