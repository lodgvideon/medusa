package imap

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/lodgvideon/medusa/partition"
)

// A distributed FIFO queue with TRANSACTIONAL (atomic multi-key) operations,
// built on the distributed map with partition affinity — the Hazelcast IQueue
// approach. Each queue is:
//
//   - one METADATA entry — [headSeg:8][tailSeg:8][count:8] — under the reserved
//     __queue namespace, keyed by the queue name;
//   - a chain of SEGMENT entries under the reserved __queueseg namespace, keyed
//     by [segId:8][name], each a packed [uint32 len][item]… stream capped at
//     maxSegBytes.
//
// ALL of a queue's entries are partition-AFFINE: they route by the QUEUE NAME
// (routedPartition), not their own key, so the metadata and every segment live in
// ONE partition — one owner, one shard lock. Every queue operation is a single
// EXECUTE on that owner which mutates the metadata and the head/tail segment in
// one critical section (store.updateMulti under the shard lock, inside the WAL
// lock). That single-owner atomicity is what makes the queue correct under
// concurrent producers and consumers: there is never a moment when an element
// exists but is unreachable (the strand race that sinks client-orchestrated
// multi-step designs), and FIFO order is total by construction.
//
// Versus a single packed value, segments bound the bytes copied per op (O(one
// segment), not O(whole queue)) and lift the ~64 MiB per-value cap — a queue is
// bounded by its owner's memory (it lives in one partition, like Hazelcast's
// IQueue). Ops remain at-least-once under owner failover (best-effort
// replication): a lost response + retry can redeliver or drop one element — carry
// an idempotency key where that matters; exactly-once needs consensus (out of
// scope, AP model).

// queueMap holds per-queue metadata; queueSegMap holds the segments. Both are
// reserved: the ordinary Map/HTTP mutation API rejects them (see IsReservedMap) so
// a client cannot corrupt a queue via Put/Remove/Clear, and max-size eviction
// never samples them.
const (
	queueMap    = "__queue"
	queueSegMap = "__queueseg"
)

// maxSegBytes caps a segment's packed size, bounding the bytes copied per offer or
// poll. An item larger than the cap still lands alone in its own segment, so no
// element is ever un-enqueuable.
const maxSegBytes = 64 * 1024

// IsReservedMap reports whether name is a namespace medusa reserves for its own
// data structures (the distributed queue's metadata and segments). Mutating a
// reserved map through the ordinary Map/HTTP API is rejected; use the dedicated
// API (e.g. Queue) instead.
func IsReservedMap(name string) bool { return name == queueMap || name == queueSegMap }

// routedPartition returns the partition an entry of the named map belongs to.
// Queue segments are partition-affine: they route by the queue name embedded in
// the key (its suffix past the 8-byte segment id), so a queue's whole state —
// metadata and segments — co-locates in one partition and its ops can be atomic
// on one owner. Every other entry (including the metadata, whose key IS the
// name) routes by its own key. Every path that stores an entry — owner writes,
// backup replication, WAL replay, snapshot restore, migration — routes through
// this, so an entry lands in the same partition everywhere.
func routedPartition(name string, key []byte) int {
	if name == queueSegMap && len(key) >= 8 { // >=: an empty queue name is an 8-byte key
		return partition.For(key[8:])
	}
	return partition.For(key)
}

// queueSegKey is the store key for segment segId of the named queue: [segId][name].
// segId leads (fixed 8 bytes) so the encoding is unambiguous for any queue name.
func queueSegKey(segId uint64, name string) []byte {
	b := make([]byte, 8+len(name))
	binary.BigEndian.PutUint64(b, segId)
	copy(b[8:], name)
	return b
}

// ---- metadata codec: [headSeg:8][tailSeg:8][count:8]; absent = empty queue ----

func decodeQueueMeta(b []byte) (head, tail, count uint64) {
	if len(b) < 24 {
		return 0, 0, 0
	}
	return binary.BigEndian.Uint64(b), binary.BigEndian.Uint64(b[8:]), binary.BigEndian.Uint64(b[16:])
}

func encodeQueueMeta(head, tail, count uint64) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint64(b, head)
	binary.BigEndian.PutUint64(b[8:], tail)
	binary.BigEndian.PutUint64(b[16:], count)
	return b
}

// ---- segment value: a packed [uint32 len][item]… stream ----

// queueCount returns the number of packed items in b.
func queueCount(b []byte) int {
	n := 0
	for len(b) >= 4 {
		l := binary.BigEndian.Uint32(b[:4])
		if len(b) < 4+int(l) {
			break // torn tail — stop (shouldn't happen for well-formed values)
		}
		b = b[4+int(l):]
		n++
	}
	return n
}

// queueHead splits the packed value into the first item and the remainder.
func queueHead(b []byte) (item, rest []byte, ok bool) {
	if len(b) < 4 {
		return nil, nil, false
	}
	n := binary.BigEndian.Uint32(b[:4])
	if len(b) < 4+int(n) {
		return nil, nil, false
	}
	return b[4 : 4+n], b[4+int(n):], true
}

// appendPacked appends one length-prefixed item to a packed stream.
func appendPacked(seg, item []byte) []byte {
	nv := make([]byte, 0, len(seg)+4+len(item))
	nv = append(nv, seg...)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(item)))
	nv = append(nv, hdr[:]...)
	return append(nv, item...)
}

// queueOp applies one queue operation to the queue's co-located state — the
// metadata plus the head/tail segment — through the get/set/del accessors of a
// store.updateMulti critical section. It runs on the queue's partition owner
// under the shard lock (and the WAL lock), so the whole operation is atomic:
// concurrent offers and polls serialize, and no element is ever stored without
// being reachable. Results follow the old wire conventions: offer/size return a
// big-endian int64; poll/peek return a 1-byte found flag followed by the item.
func queueOp(name, op string, arg []byte, get getFn, set setFn, del delFn) ([]byte, error) {
	metaKey := []byte(name)
	mv, _ := get(queueMap, metaKey)
	head, tail, count := decodeQueueMeta(mv)

	switch op {
	case "queueoffer":
		sk := queueSegKey(tail, name)
		var seg []byte
		if count > 0 {
			seg, _ = get(queueSegMap, sk)
		}
		// count == 0: any content at the tail id is stale — e.g. a promoted
		// backup's zombie segment from a missed delete — so start fresh and
		// overwrite it rather than redelivering drained elements in bulk.
		if len(seg) != 0 && len(seg)+4+len(arg) > maxSegBytes {
			tail++ // segment full: roll (an oversized item lands alone)
			sk = queueSegKey(tail, name)
			seg, _ = get(queueSegMap, sk) // append to a crash-orphaned segment, never clobber it
		}
		set(queueSegMap, sk, appendPacked(seg, arg))
		count++
		set(queueMap, metaKey, encodeQueueMeta(head, tail, count))
		return putI64(int64(count)), nil

	case "queuepoll":
		// NEVER mutate on a path that did not consume an element. In particular,
		// count > 0 with the head segment absent or torn means the co-located
		// state is INCOMPLETE here (a migration or anti-entropy fill still in
		// flight) — report empty transiently and let the state converge;
		// advancing head or deleting the metadata on such a view is how
		// acknowledged elements get destroyed.
		if mv == nil || count == 0 {
			return []byte{0}, nil
		}
		sk := queueSegKey(head, name)
		seg, ok := get(queueSegMap, sk)
		if !ok {
			return []byte{0}, nil // incomplete state: transient empty, no mutation
		}
		item, rest, k := queueHead(seg)
		if !k {
			return []byte{0}, nil // torn value: surface as empty-with-size, never destroy
		}
		out := append(make([]byte, 0, 1+len(item)), 1)
		out = append(out, item...) // copy: item aliases the store, which we mutate below
		count--
		if len(rest) == 0 {
			del(queueSegMap, sk)
			if head < tail {
				head++ // advance atomically with the drain of its own segment
			}
		} else {
			set(queueSegMap, sk, rest)
		}
		// The metadata persists even when the queue drains (count 0): segment ids
		// stay monotonic for the queue's lifetime, so a stale segment at an old id
		// (a backup that missed a delete) can never merge into a live queue.
		set(queueMap, metaKey, encodeQueueMeta(head, tail, count))
		return out, nil

	case "queuepeek":
		if mv == nil || count == 0 {
			return []byte{0}, nil
		}
		if seg, ok := get(queueSegMap, queueSegKey(head, name)); ok {
			if item, _, k := queueHead(seg); k {
				out := append(make([]byte, 0, 1+len(item)), 1)
				return append(out, item...), nil // copy — read-only op, no mutation
			}
		}
		return []byte{0}, nil // incomplete/torn: transient empty (see queuepoll)

	case "queuesize":
		return putI64(int64(count)), nil

	default:
		return nil, fmt.Errorf("imap: unknown queue operation %q", op)
	}
}

// Queue is a handle to a named distributed FIFO queue. Obtain one with
// Service.Queue (or Node.Queue). It is safe for concurrent use: every operation
// is a single atomic Execute on the owner of the queue's partition, so order is
// a total global FIFO even under concurrent producers and consumers.
//
// At-least-once caveat (inherited from Execute on owner failover): if the owner
// applies an op and replicates it but its response is lost, the client's retry
// runs on the promoted backup and applies AGAIN — a duplicated Offer, or a Poll
// that drops a second element. Use it where at-least-once delivery is acceptable,
// or carry an idempotency key in the item; exactly-once would need consensus
// replication (out of scope, AP model).
type Queue struct {
	svc  *Service
	name string
}

// Queue returns a handle to the named distributed queue.
func (s *Service) Queue(name string) *Queue { return &Queue{svc: s, name: name} }

// Offer appends value to the tail and returns the queue's new size.
func (q *Queue) Offer(ctx context.Context, value []byte) (int64, error) {
	out, err := q.svc.Map(queueMap).Execute(ctx, []byte(q.name), "queueoffer", value)
	if err != nil {
		return 0, err
	}
	return decodeI64(out), nil
}

// Poll removes and returns the head item; ok is false when the queue is empty.
func (q *Queue) Poll(ctx context.Context) ([]byte, bool, error) {
	out, err := q.svc.Map(queueMap).Execute(ctx, []byte(q.name), "queuepoll", nil)
	if err != nil {
		return nil, false, err
	}
	v, ok := decodeQueueItem(out)
	return v, ok, nil
}

// Peek returns the head item without removing it; ok is false when empty.
func (q *Queue) Peek(ctx context.Context) ([]byte, bool, error) {
	out, err := q.svc.Map(queueMap).Execute(ctx, []byte(q.name), "queuepeek", nil)
	if err != nil {
		return nil, false, err
	}
	v, ok := decodeQueueItem(out)
	return v, ok, nil
}

// Size returns the number of items currently in the queue (O(1) — tracked in the
// queue's metadata).
func (q *Queue) Size(ctx context.Context) (int64, error) {
	out, err := q.svc.Map(queueMap).Execute(ctx, []byte(q.name), "queuesize", nil)
	if err != nil {
		return 0, err
	}
	return decodeI64(out), nil
}

func decodeI64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

// decodeQueueItem splits poll/peek's found-flag-prefixed result. A missing flag
// or a zero flag means the queue was empty.
func decodeQueueItem(out []byte) ([]byte, bool) {
	if len(out) == 0 || out[0] == 0 {
		return nil, false
	}
	return out[1:], true
}

// convertLegacyQueue upgrades a pre-segmentation queue value — the whole queue as
// ONE packed [u32 len][item]… stream under __queue — into the current
// metadata+segments representation, preserving FIFO order. The old format is
// recognized by length: current metadata is exactly 24 bytes; any other non-empty
// __queue value on restore is legacy. (A legacy queue whose packed size happened
// to be exactly 24 bytes would be misread — a corner accepted and documented.)
func convertLegacyQueue(name string, packed []byte) []entry {
	var out []entry
	var tail, count uint64
	var seg []byte
	flush := func() {
		if len(seg) > 0 {
			out = append(out, entry{mapName: queueSegMap, key: string(queueSegKey(tail, name)), value: seg})
		}
	}
	for {
		item, rest, ok := queueHead(packed)
		if !ok {
			break
		}
		if len(seg) != 0 && len(seg)+4+len(item) > maxSegBytes {
			flush()
			tail++
			seg = nil
		}
		seg = appendPacked(seg, item)
		count++
		packed = rest
	}
	flush()
	if count == 0 {
		return nil
	}
	out = append(out, entry{mapName: queueMap, key: name, value: encodeQueueMeta(0, tail, count)})
	return out
}

// orderQueueMetaLast reorders a partition snapshot so queue METADATA entries come
// after everything else (their segments in particular). Migration and anti-entropy
// push entries one at a time; sending a queue's metadata last means a receiver
// never holds metadata that references segments which have not arrived — the
// incomplete view a concurrent poll would (harmlessly, but visibly) read as empty.
func orderQueueMetaLast(entries []entry) []entry {
	ordered := make([]entry, 0, len(entries))
	var metas []entry
	for _, e := range entries {
		if e.mapName == queueMap {
			metas = append(metas, e)
			continue
		}
		ordered = append(ordered, e)
	}
	return append(ordered, metas...)
}
