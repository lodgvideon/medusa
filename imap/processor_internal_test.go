package imap

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/lodgvideon/medusa/partition"
)

// TestPutIfAbsentProc covers the absent→store (1) and present→keep (0) cases.
func TestPutIfAbsentProc(t *testing.T) {
	nv, a, out := putIfAbsentProc(nil, false, []byte("v"))
	if a != Set || string(nv) != "v" || len(out) != 1 || out[0] != 1 {
		t.Fatalf("absent: nv=%q action=%v out=%v; want set,\"v\",[1]", nv, a, out)
	}
	_, a, out = putIfAbsentProc([]byte("x"), true, []byte("v"))
	if a != Keep || len(out) != 1 || out[0] != 0 {
		t.Fatalf("present: action=%v out=%v; want keep,[0]", a, out)
	}
}

// TestCASProc covers compare-and-swap: match swaps, mismatch/absent/malformed
// leave the entry unchanged, and the arg encoding round-trips binary values.
func TestCASProc(t *testing.T) {
	arg := encodeCAS([]byte("old"), []byte("new"))

	nv, a, out := casProc([]byte("old"), true, arg)
	if a != Set || string(nv) != "new" || out[0] != 1 {
		t.Fatalf("match: nv=%q action=%v out=%v; want set,\"new\",[1]", nv, a, out)
	}
	if _, a, out = casProc([]byte("other"), true, arg); a != Keep || out[0] != 0 {
		t.Fatalf("mismatch: action=%v out=%v; want keep,[0]", a, out)
	}
	if _, a, out = casProc(nil, false, arg); a != Keep || out[0] != 0 {
		t.Fatalf("absent: action=%v out=%v; want keep,[0]", a, out)
	}
	if _, a, out = casProc([]byte("old"), true, []byte{0x00}); a != Keep || out[0] != 0 {
		t.Fatalf("malformed arg: action=%v out=%v; want keep,[0]", a, out)
	}
	exp, nw, ok := splitCAS(encodeCAS([]byte("a\x00b"), []byte("c\x00d")))
	if !ok || string(exp) != "a\x00b" || string(nw) != "c\x00d" {
		t.Fatalf("encode/split round-trip: exp=%q new=%q ok=%v", exp, nw, ok)
	}
}

// TestLockProcs covers the fenced-lock processors: acquire, contention,
// re-entrant acquire (same token), holder-only release, fence retention, and the
// fence bumping strictly on the next acquire.
func TestLockProcs(t *testing.T) {
	h1, h2 := []byte("n1"), []byte("n2")

	nv, a, out := lockAcquireProc(nil, false, h1)
	if a != Set || len(out) != 8 || binary.BigEndian.Uint64(out) != 1 {
		t.Fatalf("first acquire: action=%v out=%v; want set, fence 1", a, out)
	}
	state := nv

	if _, a, out = lockAcquireProc(state, true, h2); a != Keep || len(out) != 0 {
		t.Fatalf("contended acquire: action=%v out=%v; want keep, empty", a, out)
	}
	if _, a, out = lockAcquireProc(state, true, h1); a != Keep || binary.BigEndian.Uint64(out) != 1 {
		t.Fatalf("re-entrant acquire: action=%v out=%v; want keep, fence 1", a, out)
	}
	if _, a, out = lockReleaseProc(state, true, h2); a != Keep || out[0] != 0 {
		t.Fatalf("non-holder release: action=%v out=%v; want keep, [0]", a, out)
	}

	nv, a, out = lockReleaseProc(state, true, h1)
	if a != Set || out[0] != 1 {
		t.Fatalf("release: action=%v out=%v; want set, [1]", a, out)
	}
	if f, holder := decodeLock(nv); f != 1 || len(holder) != 0 {
		t.Fatalf("released state: fence=%d holder=%q; want 1, empty", f, holder)
	}
	if _, a, out = lockAcquireProc(nv, true, h2); a != Set || binary.BigEndian.Uint64(out) != 2 {
		t.Fatalf("re-acquire after release: action=%v out=%v; want set, fence 2", a, out)
	}

	// An empty holder must be refused — storing it would mark the lock free and
	// let anyone "acquire" it.
	if newVal, a, out := lockAcquireProc(nil, false, nil); a != Keep || out != nil || newVal != nil {
		t.Fatalf("empty-holder acquire: newVal=%v action=%v out=%v; want keep, nil, nil", newVal, a, out)
	}
}

// TestRegisterAndExecuteCustomProcessor covers custom processor registration and
// the execute path end to end, including its write-ahead-log records for both a
// Set (the custom processor) and a Delete (the built-in "delete"), plus the
// unknown-processor error and delete-on-missing no-op.
func TestRegisterAndExecuteCustomProcessor(t *testing.T) {
	RegisterProcessor("double", func(cur []byte, _ bool, _ []byte) ([]byte, Action, []byte) {
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, uint64(readInt64(cur)*2))
		return out, Set, out
	})

	s := svcWith(&fakeTransport{})
	if err := s.OpenWAL(filepath.Join(t.TempDir(), "wal.log")); err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer s.CloseWAL()
	ctx := context.Background()

	five := make([]byte, 8)
	binary.BigEndian.PutUint64(five, 5)
	if _, err := s.applyPut(ctx, "m", []byte("k"), five, 0, false); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	out, err := s.applyExecute(ctx, "m", []byte("k"), "double", nil)
	if err != nil {
		t.Fatalf("execute double: %v", err)
	}
	if readInt64(out) != 10 {
		t.Fatalf("double(5) = %d, want 10", readInt64(out))
	}

	if _, err := s.applyExecute(ctx, "m", []byte("k"), "nope", nil); err == nil {
		t.Error("execute of an unknown processor should error")
	}

	// "delete" removes an existing key (Delete action → WAL remove record)...
	if _, err := s.applyExecute(ctx, "m", []byte("k"), "delete", nil); err != nil {
		t.Fatalf("execute delete: %v", err)
	}
	if _, ok := s.store.get(partition.For([]byte("k")), "m", []byte("k")); ok {
		t.Error("delete processor should have removed the key")
	}
	// ...and is a no-op on an already-absent key (Keep action).
	if out, err := s.applyExecute(ctx, "m", []byte("k"), "delete", nil); err != nil || out != nil {
		t.Errorf("delete on missing key = %q,%v, want nil,nil", out, err)
	}
}
