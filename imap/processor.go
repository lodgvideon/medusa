package imap

import (
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
		"incr":   incrProc,
		"append": appendProc,
		"getset": getsetProc,
		"delete": deleteProc,
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

func readInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}
