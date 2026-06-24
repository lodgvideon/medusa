package imap

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/medusa/metrics"
)

// TestEntryListenerReceivesEvents proves a registered listener gets a create,
// update, and remove event in order, with the right map/key/value.
func TestEntryListenerReceivesEvents(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	got := make(chan EntryEvent, 8)
	s.AddEntryListener(func(e EntryEvent) { got <- e })
	ctx := context.Background()

	if _, err := s.applyPut(ctx, "m", []byte("k"), []byte("v1"), 0, false); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.applyPut(ctx, "m", []byte("k"), []byte("v2"), 0, false); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := s.applyRemove(ctx, "m", []byte("k"), false); err != nil {
		t.Fatalf("remove: %v", err)
	}

	want := []struct {
		typ EventType
		val string
	}{{EventCreated, "v1"}, {EventUpdated, "v2"}, {EventRemoved, ""}}
	for i, w := range want {
		select {
		case e := <-got:
			if e.Type != w.typ || e.Map != "m" || string(e.Key) != "k" || string(e.Value) != w.val {
				t.Fatalf("event %d = {%v map=%q key=%q val=%q}; want {%v val=%q}", i, e.Type, e.Map, e.Key, e.Value, w.typ, w.val)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d (%v)", i, w.typ)
		}
	}
}

// TestEntryListenerExecuteCreateVsUpdate is the regression guard for the review
// finding that applyExecute always emitted EventUpdated: a processor that creates
// a new key must emit EventCreated, an overwrite EventUpdated, a delete EventRemoved.
func TestEntryListenerExecuteCreateVsUpdate(t *testing.T) {
	RegisterProcessor("lsnr_set", func(cur []byte, exists bool, arg []byte) ([]byte, Action, []byte) {
		return []byte("x"), Set, nil
	})
	s := svcWith(&fakeTransport{})
	defer s.Close()
	got := make(chan EntryEvent, 8)
	s.AddEntryListener(func(e EntryEvent) { got <- e })
	ctx := context.Background()

	if _, err := s.applyExecute(ctx, "m", []byte("k"), "lsnr_set", nil); err != nil { // create
		t.Fatalf("execute create: %v", err)
	}
	if _, err := s.applyExecute(ctx, "m", []byte("k"), "lsnr_set", nil); err != nil { // overwrite
		t.Fatalf("execute update: %v", err)
	}
	if _, err := s.applyExecute(ctx, "m", []byte("k"), "delete", nil); err != nil { // delete
		t.Fatalf("execute delete: %v", err)
	}

	for i, wt := range []EventType{EventCreated, EventUpdated, EventRemoved} {
		select {
		case e := <-got:
			if e.Type != wt {
				t.Fatalf("execute event %d = %v, want %v", i, e.Type, wt)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for execute event %d (%v)", i, wt)
		}
	}
}

// TestEntryListenerOnlyOwnerEmits proves a backup write (isBackup=true) does not
// emit — events fire once, on the owner, not for replica copies.
func TestEntryListenerOnlyOwnerEmits(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	got := make(chan EntryEvent, 4)
	s.AddEntryListener(func(e EntryEvent) { got <- e })

	if _, err := s.applyPut(context.Background(), "m", []byte("k"), []byte("v"), 0, true); err != nil {
		t.Fatalf("backup put: %v", err)
	}
	select {
	case e := <-got:
		t.Fatalf("a backup write must not emit an event, got %v", e)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestEmitZeroAllocWithoutListener proves the write hot path pays nothing when no
// listener is registered: emit is a single atomic load, allocation-free.
func TestEmitZeroAllocWithoutListener(t *testing.T) {
	s := svcWith(&fakeTransport{})
	key, val := []byte("k"), []byte("v")
	allocs := testing.AllocsPerRun(1000, func() {
		s.events.emit(EventUpdated, "m", key, val)
	})
	if allocs != 0 {
		t.Fatalf("emit with no listener = %.1f allocs/op, want 0", allocs)
	}
}

// TestEntryListenerDropsWhenQueueFull proves a slow listener never blocks the
// writer: once the bounded queue fills, events are dropped (and counted) instead.
func TestEntryListenerDropsWhenQueueFull(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	metrics.EventsDropped.Store(0)

	block := make(chan struct{})
	var once sync.Once
	s.AddEntryListener(func(EntryEvent) {
		once.Do(func() { <-block }) // the first delivery wedges the dispatcher
	})
	defer close(block)

	for i := 0; i < listenerQueue+200; i++ {
		s.events.emit(EventUpdated, "m", []byte("k"), []byte("v")) // must never block
	}
	if d := metrics.EventsDropped.Load(); d == 0 {
		t.Fatal("expected events to be dropped once the queue filled")
	}
}

// TestEventTypeString covers the human-readable labels used in logs.
func TestEventTypeString(t *testing.T) {
	for _, c := range []struct {
		t    EventType
		want string
	}{{EventCreated, "created"}, {EventUpdated, "updated"}, {EventRemoved, "removed"}, {EventType(99), "unknown"}} {
		if got := c.t.String(); got != c.want {
			t.Errorf("EventType(%d).String() = %q, want %q", c.t, got, c.want)
		}
	}
}

// TestServiceCloseDisablesEmit proves Close stops the dispatcher and turns emit
// into a no-op (the gate is cleared), so a late mutation neither delivers nor panics.
func TestServiceCloseDisablesEmit(t *testing.T) {
	s := svcWith(&fakeTransport{})
	s.AddEntryListener(func(EntryEvent) {})
	s.Close()

	before := metrics.EventsEmitted.Load()
	s.events.emit(EventUpdated, "m", []byte("k"), []byte("v"))
	if metrics.EventsEmitted.Load() != before {
		t.Fatal("emit after Close should be a no-op")
	}
}
