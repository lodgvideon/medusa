package imap_test

import (
	"context"
	"testing"
)

// TestQueueDistributed proves the distributed FIFO queue: offer from one node,
// observe size/peek/poll from others — all route to the queue's single owner, so
// order is global. Draining empties it.
func TestQueueDistributed(t *testing.T) {
	f := newFixture()
	a, b, c := f.cluster3(t)
	ctx := context.Background()

	for _, v := range []string{"1", "2", "3"} {
		if _, err := a.svc.Queue("tasks").Offer(ctx, []byte(v)); err != nil {
			t.Fatalf("offer %s: %v", v, err)
		}
	}
	if n, err := b.svc.Queue("tasks").Size(ctx); err != nil || n != 3 {
		t.Fatalf("size from b = %d,%v; want 3", n, err)
	}
	if v, ok, err := c.svc.Queue("tasks").Peek(ctx); err != nil || !ok || string(v) != "1" {
		t.Fatalf("peek from c = %q,%v,%v; want 1,true,nil", v, ok, err)
	}
	for _, want := range []string{"1", "2", "3"} {
		v, ok, err := b.svc.Queue("tasks").Poll(ctx)
		if err != nil || !ok || string(v) != want {
			t.Fatalf("poll from b = %q,%v,%v; want %s", v, ok, err, want)
		}
	}
	if _, ok, _ := a.svc.Queue("tasks").Poll(ctx); ok {
		t.Fatal("poll on a drained queue should report not-found")
	}
	if n, _ := a.svc.Queue("tasks").Size(ctx); n != 0 {
		t.Fatalf("size after drain = %d, want 0", n)
	}
}
