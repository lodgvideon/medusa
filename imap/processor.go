package imap

import (
	"bytes"
	"encoding/binary"
	"sync"
)

// Action is what a Processor decides to do with an entry.
type Action uint8

const (
	Keep   Action = iota // leave the entry unchanged
	Set                  // store newVal
	Delete               // remove the entry
)

// Processor computes a new entry state and a result from the current value of a
// key. exists reports whether a live value is present (cur is nil otherwise).
// It runs atomically on the partition owner under the entry's shard lock, so
// concurrent processors on the same key are serialized — no lost updates.
//
// It returns the new value (when action is Set), the action, and an out value
// returned to the caller. Processors must be deterministic and side-effect
// free: only their resulting state is replicated to the backup, not the call.
type Processor func(cur []byte, exists bool, arg []byte) (newVal []byte, action Action, out []byte)

var (
	procMu     sync.RWMutex
	processors = map[string]Processor{
		"incr":        incrProc,
		"append":      appendProc,
		"getset":      getsetProc,
		"delete":      deleteProc,
		"putifabsent": putIfAbsentProc,
		"cas":         casProc,
	}
)

// RegisterProcessor adds or replaces a named processor. Call it before serving.
func RegisterProcessor(name string, p Processor) {
	procMu.Lock()
	processors[name] = p
	procMu.Unlock()
}

func lookupProcessor(name string) (Processor, bool) {
	procMu.RLock()
	p, ok := processors[name]
	procMu.RUnlock()
	return p, ok
}

// incrProc treats the value and arg as big-endian int64 and adds them. A
// missing entry counts as 0. The result is the new counter value. This is the
// canonical atomic distributed counter.
func incrProc(cur []byte, _ bool, arg []byte) ([]byte, Action, []byte) {
	n := readInt64(cur) + readInt64(arg)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf, Set, buf
}

// appendProc appends arg to the current value and returns the new value.
func appendProc(cur []byte, _ bool, arg []byte) ([]byte, Action, []byte) {
	out := make([]byte, 0, len(cur)+len(arg))
	out = append(out, cur...)
	out = append(out, arg...)
	return out, Set, out
}

// getsetProc stores arg and returns the previous value (nil if absent).
func getsetProc(cur []byte, exists bool, arg []byte) ([]byte, Action, []byte) {
	var old []byte
	if exists {
		old = append([]byte(nil), cur...)
	}
	return append([]byte(nil), arg...), Set, old
}

// deleteProc removes the entry, returning the previous value.
func deleteProc(cur []byte, exists bool, _ []byte) ([]byte, Action, []byte) {
	if !exists {
		return nil, Keep, nil
	}
	return nil, Delete, append([]byte(nil), cur...)
}

// putIfAbsentProc stores arg only if the key is currently absent. It returns a
// single byte: 1 if the value was stored, 0 if the key already existed. This is
// the atomic distributed putIfAbsent — a building block for distributed locks
// (a caller that gets 1 "holds" the lock key) and leader election.
func putIfAbsentProc(cur []byte, exists bool, arg []byte) ([]byte, Action, []byte) {
	if exists {
		return nil, Keep, []byte{0}
	}
	return append([]byte(nil), arg...), Set, []byte{1}
}

// casProc is an atomic compare-and-swap: arg packs (expected, new) — see
// encodeCAS — and the entry is set to new only if it currently exists with a
// value equal to expected. It returns 1 if the swap happened, 0 otherwise. This
// is the optimistic-concurrency / compare-and-set primitive. (To go from absent
// to present, use putifabsent instead.)
func casProc(cur []byte, exists bool, arg []byte) ([]byte, Action, []byte) {
	expected, newVal, ok := splitCAS(arg)
	if !ok || !exists || !bytes.Equal(cur, expected) {
		return nil, Keep, []byte{0}
	}
	return append([]byte(nil), newVal...), Set, []byte{1}
}

// encodeCAS packs an (expected, new) pair into a single "cas" processor arg as
// [big-endian uint32 len(expected)][expected][new].
func encodeCAS(expected, newVal []byte) []byte {
	buf := make([]byte, 4+len(expected)+len(newVal))
	binary.BigEndian.PutUint32(buf, uint32(len(expected)))
	copy(buf[4:], expected)
	copy(buf[4+len(expected):], newVal)
	return buf
}

// splitCAS is the inverse of encodeCAS. ok is false for a malformed arg.
func splitCAS(arg []byte) (expected, newVal []byte, ok bool) {
	if len(arg) < 4 {
		return nil, nil, false
	}
	n := binary.BigEndian.Uint32(arg[:4])
	if uint64(n) > uint64(len(arg)-4) {
		return nil, nil, false
	}
	return arg[4 : 4+n], arg[4+n:], true
}

func readInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}
