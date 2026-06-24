package imap

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestEvictDoesNotDeleteThrough is the regression guard for the critical holistic
// finding: max-size eviction called applyRemove, which (after load/evict) delete-
// throughs to a MapStore — so shedding a cache entry for memory would destroy the
// backing-store record. Eviction must use applyEvict: drop the copy, never delete
// through.
func TestEvictDoesNotDeleteThrough(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	backing := &fakeBackingStore{data: map[string][]byte{}}
	s.SetMapStore("m", backing)
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		if err := s.Map("m").Put(ctx, []byte{byte(i)}, []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if _, st, _ := backing.count(); st != 50 {
		t.Fatalf("write-through stores = %d, want 50", st)
	}
	s.Evict(ctx, 5, 1000) // evict down to 5
	if _, _, d := backing.count(); d != 0 {
		t.Fatalf("eviction delete-through'd %d entries to the backing store; want 0", d)
	}
}

// TestExecuteWriteThrough is the regression guard for the finding that
// applyExecute bypassed the MapStore: a processor Set must write-through and a
// processor Delete must delete-through, like Put/Remove.
func TestExecuteWriteThrough(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	backing := &fakeBackingStore{data: map[string][]byte{}}
	s.SetMapStore("m", backing)
	ctx := context.Background()

	if _, err := s.Map("m").Execute(ctx, []byte("k"), "append", []byte("hello")); err != nil {
		t.Fatalf("execute append: %v", err)
	}
	if _, st, _ := backing.count(); st != 1 || string(backing.data["k"]) != "hello" {
		t.Fatalf("execute Set write-through: stores=%d data=%q", st, backing.data["k"])
	}
	if _, err := s.Map("m").Execute(ctx, []byte("k"), "delete", nil); err != nil {
		t.Fatalf("execute delete: %v", err)
	}
	if _, _, d := backing.count(); d != 1 {
		t.Fatalf("execute Delete delete-through: deletes=%d, want 1", d)
	}
}

// TestReservedMapRejectsMutations is the regression guard for the critical
// finding that the ordinary map API could wipe or corrupt queues: mutating the
// reserved namespace is rejected, and the queue survives the rejected mutations.
func TestReservedMapRejectsMutations(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Queue("q").Offer(ctx, []byte("item")); err != nil {
		t.Fatalf("offer: %v", err)
	}

	rm := s.Map(queueMap)
	if err := rm.Put(ctx, []byte("q"), []byte("corrupt")); !errors.Is(err, errReservedMap) {
		t.Fatalf("Put on reserved map = %v, want errReservedMap", err)
	}
	if _, err := rm.Remove(ctx, []byte("q")); !errors.Is(err, errReservedMap) {
		t.Fatalf("Remove on reserved map = %v, want errReservedMap", err)
	}
	if err := rm.Clear(ctx); !errors.Is(err, errReservedMap) {
		t.Fatalf("Clear on reserved map = %v, want errReservedMap", err)
	}
	if _, err := rm.Evict(ctx, []byte("q")); !errors.Is(err, errReservedMap) {
		t.Fatalf("Evict on reserved map = %v, want errReservedMap", err)
	}
	if n, _ := s.Queue("q").Size(ctx); n != 1 {
		t.Fatalf("queue size after rejected mutations = %d, want 1 (queue must be intact)", n)
	}
}

// TestQueueOpsDoNotFireListeners is the regression guard for the finding that
// queue operations (which run via applyExecute on the reserved map) surfaced as
// entry events to global listeners — leaking internal infrastructure.
func TestQueueOpsDoNotFireListeners(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	got := make(chan EntryEvent, 8)
	s.AddEntryListener(func(e EntryEvent) { got <- e })
	ctx := context.Background()

	if _, err := s.Queue("q").Offer(ctx, []byte("x")); err != nil {
		t.Fatalf("offer: %v", err)
	}
	if _, _, err := s.Queue("q").Poll(ctx); err != nil {
		t.Fatalf("poll: %v", err)
	}
	select {
	case e := <-got:
		t.Fatalf("a queue op leaked an entry event to a listener: %+v", e)
	case <-time.After(200 * time.Millisecond):
	}
	// A real map mutation still fires — listeners aren't globally broken.
	if err := s.Map("real").Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	select {
	case e := <-got:
		if e.Map != "real" || e.Type != EventCreated {
			t.Fatalf("event = {%v %q}, want {created real}", e.Type, e.Map)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a real map Put did not fire an event")
	}
}
