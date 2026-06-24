package imap

import (
	"context"
	"testing"
)

// TestGetZeroAllocWithFeatures locks the central zero-alloc guarantee against the
// features layered onto the map: a local Get hit must stay allocation-free even
// when an entry listener and a MapLoader are configured. Get neither emits events
// (only writes do) nor consults the loader on a hit (read-through fires only on a
// miss), so the read hot path is decoupled from both — this guards that it stays so.
func TestGetZeroAllocWithFeatures(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	s.AddEntryListener(func(EntryEvent) {})
	s.SetMapLoader("m", readOnlyLoader{val: []byte("x")})
	ctx := context.Background()

	key := []byte("hot-key")
	if err := s.Map("m").Put(ctx, key, []byte("a-cache-value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, _, err := s.Map("m").Get(ctx, key); err != nil { // warm
		t.Fatalf("get: %v", err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		_, _, _ = s.Map("m").Get(ctx, key)
	})
	if allocs != 0 {
		t.Fatalf("local Get with a listener+loader configured = %v allocs/op, want 0", allocs)
	}
}

// TestPutAllocsBounded guards the write path: an owner Put copies the key (into
// the map key) and the value (so the store owns it) — two unavoidable allocations.
// With no backups or listeners, replication and event emission add none; this
// fails if some future change silently allocates on every write.
func TestPutAllocsBounded(t *testing.T) {
	s := svcWith(&fakeTransport{}) // solo node: no backups, no listener
	defer s.Close()
	ctx := context.Background()
	key, val := []byte("k"), []byte("a-value")

	allocs := testing.AllocsPerRun(1000, func() {
		_, _ = s.applyPut(ctx, "m", key, val, 0, false)
	})
	if allocs > 2 {
		t.Fatalf("applyPut = %v allocs/op, want <= 2 (only the key + value copy)", allocs)
	}
}
