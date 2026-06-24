package imap

import (
	"context"
	"encoding/binary"
)

// A distributed FIFO queue, built ON TOP of the distributed map rather than as a
// parallel subsystem: the whole queue is one map value under a reserved namespace,
// keyed by the queue name, and offer/poll/peek/size are EntryProcessors that
// read-modify-write that value atomically on its owner. This inherits everything
// the map already provides — single-owner global FIFO order, owner routing,
// backup replication and failover, WAL durability, snapshots, and partition
// migration — with no new wire types or storage. The value is a packed stream of
// [uint32 big-endian item length][item bytes] records; offer appends, poll pops
// the head, peek reads it, size counts.
//
// Trade-off (v1): each op serialises/copies the whole value (O(n)), and a queue's
// total packed size is bounded by the per-value transport limit (~64 MiB). A
// per-item representation is a future refinement; this is the minimal correct
// distributed queue that reuses the map's durability and replication machinery.

// queueMap is the reserved map namespace that holds every queue, keyed by queue
// name. Avoid using it as an ordinary map name.
const queueMap = "__queue"

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

// queueOfferProc appends arg as a new tail item and returns the new size (a
// big-endian int64).
func queueOfferProc(cur []byte, _ bool, arg []byte) ([]byte, Action, []byte) {
	nv := make([]byte, 0, len(cur)+4+len(arg))
	nv = append(nv, cur...)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(arg)))
	nv = append(nv, hdr[:]...)
	nv = append(nv, arg...)
	return nv, Set, putI64(int64(queueCount(nv)))
}

// queuePollProc removes and returns the head item. The out form is a 1-byte
// found flag (1/0) followed by the item, so an empty queue is distinguishable
// from an empty-but-present item. It Deletes the key when the queue becomes empty.
func queuePollProc(cur []byte, _ bool, _ []byte) ([]byte, Action, []byte) {
	item, rest, ok := queueHead(cur)
	if !ok {
		return nil, Keep, []byte{0}
	}
	out := append(make([]byte, 0, 1+len(item)), 1)
	out = append(out, item...)
	if len(rest) == 0 {
		return nil, Delete, out
	}
	return rest, Set, out
}

// queuePeekProc returns the head item without removing it (Keep), in the same
// found-flag form as poll.
func queuePeekProc(cur []byte, _ bool, _ []byte) ([]byte, Action, []byte) {
	item, _, ok := queueHead(cur)
	if !ok {
		return nil, Keep, []byte{0}
	}
	out := append(make([]byte, 0, 1+len(item)), 1)
	out = append(out, item...)
	return nil, Keep, out // Keep: no mutation; head returned via out
}

// queueSizeProc returns the item count as a big-endian int64 (Keep — read-only).
func queueSizeProc(cur []byte, _ bool, _ []byte) ([]byte, Action, []byte) {
	return nil, Keep, putI64(int64(queueCount(cur)))
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

// Queue is a handle to a named distributed FIFO queue. Obtain one with
// Service.Queue (or Node.Queue). It is safe for concurrent use; operations route
// to the queue's owner, so order is global.
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

// Size returns the number of items currently in the queue.
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
