package imap

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/lodgvideon/medusa/partition"
)

func i64(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

func TestProcessors(t *testing.T) {
	// incr: missing entry starts at 0.
	v, a, out := incrProc(nil, false, i64(5))
	if a != Set || readInt64(v) != 5 || readInt64(out) != 5 {
		t.Fatalf("incr from absent = %d/%v/%d", readInt64(v), a, readInt64(out))
	}
	v, _, _ = incrProc(v, true, i64(3))
	if readInt64(v) != 8 {
		t.Fatalf("incr 5+3 = %d, want 8", readInt64(v))
	}

	if v, _, _ := appendProc([]byte("ab"), true, []byte("cd")); string(v) != "abcd" {
		t.Fatalf("append = %q, want abcd", v)
	}

	nv, _, old := getsetProc([]byte("old"), true, []byte("new"))
	if string(nv) != "new" || string(old) != "old" {
		t.Fatalf("getset = %q/%q, want new/old", nv, old)
	}

	if _, act, prev := deleteProc([]byte("x"), true, nil); act != Delete || string(prev) != "x" {
		t.Fatalf("delete = %v/%q, want Delete/x", act, prev)
	}
}

// TestStoreUpdateAtomic proves the shard lock serializes read-modify-write, so a
// concurrent counter increments exactly — no lost updates.
func TestStoreUpdateAtomic(t *testing.T) {
	s := newStore()
	p := partition.For([]byte("c"))
	const goroutines, perG = 50, 100

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				s.update(p, "m", []byte("c"), func(cur []byte, _ bool) ([]byte, Action) {
					return i64(readInt64(cur) + 1), Set
				})
			}
		}()
	}
	wg.Wait()

	v, _ := s.get(p, "m", []byte("c"))
	if got := readInt64(v); got != goroutines*perG {
		t.Fatalf("counter = %d, want %d", got, goroutines*perG)
	}
}
