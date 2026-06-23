// Package imap implements a distributed map (a Hazelcast-style IMap): a named
// key/value store whose entries are partitioned across the cluster. Any node
// can serve any key by routing to the partition owner; writes are replicated
// to one backup so a single node failure does not lose data.
package imap

import (
	"context"
	"fmt"
	"time"

	"github.com/lodgvideon/medusa/cluster"
	"github.com/lodgvideon/medusa/codec"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/partition"
	"github.com/lodgvideon/medusa/transport"
)

// Service hosts this node's share of every distributed map and answers map
// RPCs from peers. Obtain per-map handles with Map.
type Service struct {
	self  string
	mem   *cluster.Membership
	tr    transport.Transport
	store *store
	wal   *wal // nil when durability logging is disabled (no DataDir)
}

// NewService creates a map service bound to a membership and transport.
func NewService(mem *cluster.Membership, tr transport.Transport) *Service {
	return &Service{self: mem.Self().ID, mem: mem, tr: tr, store: newStore()}
}

// Map returns a handle to the named distributed map.
func (s *Service) Map(name string) *Map { return &Map{svc: s, name: name} }

// applyPut writes locally as the partition owner and, unless this is itself a
// backup write, replicates to the partition's backup. It returns whether the
// key was newly created. The mutation is recorded in the write-ahead log (and
// fsynced) before returning, so an acknowledged write survives an ungraceful
// crash; a WAL error is surfaced rather than silently dropped.
//
// Ordering note: the store is mutated before the WAL append, not after. Both
// finish before the operation is acknowledged, so durability of acknowledged
// writes holds (a crash before the fsync returns means the caller was never
// acked). Apply-before-log is deliberate: it lets Checkpoint capture the store
// snapshot and truncate the WAL under one lock and know every truncated record
// is already reflected in that snapshot — so truncation can never drop a write.
func (s *Service) applyPut(ctx context.Context, name string, key, value []byte, ttlMs int64, isBackup bool) (bool, error) {
	p := partition.For(key)
	expireAt := expireFromTTL(ttlMs)
	created := s.store.put(p, name, key, value, expireAt)
	if s.wal != nil {
		if err := s.wal.appendPut(name, key, value, expireAt); err != nil {
			return created, err
		}
	}
	if !isBackup {
		s.replicate(ctx, p, medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST, name, key, value, ttlMs)
	}
	return created, nil
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
	p := partition.For(key)
	existed := s.store.remove(p, name, key)
	if s.wal != nil {
		if err := s.wal.appendRemove(name, key); err != nil {
			return existed, err
		}
	}
	if !isBackup {
		s.replicate(ctx, p, medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST, name, key, nil, 0)
	}
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

// applyExecute runs a named processor against key atomically on this (owner)
// node and replicates the resulting state to the backup. It returns the
// processor's result.
func (s *Service) applyExecute(ctx context.Context, name string, key []byte, procName string, arg []byte) ([]byte, error) {
	proc, ok := lookupProcessor(procName)
	if !ok {
		return nil, fmt.Errorf("imap: unknown processor %q", procName)
	}
	p := partition.For(key)
	var newVal, out []byte
	action, expireAt := s.store.update(p, name, key, func(cur []byte, exists bool) ([]byte, Action) {
		var a Action
		newVal, a, out = proc(cur, exists, arg)
		return newVal, a
	})
	switch action {
	case Set:
		if s.wal != nil {
			if err := s.wal.appendPut(name, key, newVal, expireAt); err != nil {
				return out, err
			}
		}
		s.replicate(ctx, p, medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST, name, key, newVal, remainingTTLms(expireAt, nowNano()))
	case Delete:
		if s.wal != nil {
			if err := s.wal.appendRemove(name, key); err != nil {
				return out, err
			}
		}
		s.replicate(ctx, p, medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST, name, key, nil, 0)
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
func (s *Service) Migrate(ctx context.Context, table *partition.Table) {
	for p := 0; p < partition.Count; p++ {
		entries := s.store.snapshotPartition(p)
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
			// never a blanket wipe — so a write that raced this migration survives.
			s.store.dropMigrated(p, entries)
		}
	}
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
// path: idempotent (redundant pushes carry the same value), and push-only — it
// heals a backup that is MISSING or STALE for a key, but does not remove a key a
// backup kept after missing a delete (digest-based full reconciliation that also
// reconciles tombstones is the roadmap). Only owned partitions are pushed, so the
// owner stays the single source of truth.
//
// Each partition is digest-gated: the owner sends each backup its content digest
// and only pushes the data to backups whose digest differs (or that fail the
// probe). In steady state, when replicas already match, a pass costs one tiny
// digest RPC per backup and transfers nothing.
//
// Each entry is re-read from the owner's store (lookup) immediately before it is
// pushed, and skipped if the owner no longer holds it — so a key deleted since
// the snapshot is not resurrected on a backup. This leaves only the same narrow
// re-read→send race Migrate has, instead of the whole snapshot→push window.
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
				for _, e := range s.store.snapshotPartition(p) {
					// Re-read the owner's CURRENT state: a key deleted or changed
					// since the snapshot must not be resurrected/clobbered on a
					// backup. Push the fresh value so backups converge to the owner.
					data, expireAt, ok := s.store.lookup(p, e.mapName, []byte(e.key))
					if !ok {
						continue // deleted/expired since the snapshot — don't resurrect
					}
					ttlMs := remainingTTLms(expireAt, now)
					if expireAt != 0 && ttlMs <= 0 {
						continue // expired in flight; let it lapse
					}
					sent := false
					for _, addr := range stale {
						if err := s.sendPut(ctx, addr, e.mapName, []byte(e.key), data, ttlMs, true); err == nil {
							sent = true
						}
					}
					if sent {
						pushed++ // count the entry once, not once per backup
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
			s.store.put(partition.For(key), name, key, value, expireAt)
		},
		func(name string, key []byte) {
			s.store.remove(partition.For(key), name, key)
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
		v, ok := s.store.get(partition.For(gr.Key), gr.Map, gr.Key)
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
		match := s.store.partitionDigest(int(dr.Partition)) == dr.Digest
		out, err := codec.Marshal(respBuf, &medusav1.DigestResponse{Match: match})
		return medusav1.MessageType_MESSAGE_TYPE_DIGEST_RESPONSE, out, err

	default:
		return 0, respBuf, fmt.Errorf("imap: unhandled message type %v", reqType)
	}
}
