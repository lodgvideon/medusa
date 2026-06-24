package imap

import (
	"context"
	"errors"
	"testing"

	"github.com/lodgvideon/medusa/cluster"
	"github.com/lodgvideon/medusa/codec"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/partition"
)

func TestQueueProcessorsFIFO(t *testing.T) {
	var q []byte // the packed queue value; nil = empty

	for _, v := range []string{"a", "b", "c"} {
		nv, act, _ := queueOfferProc(q, q != nil, []byte(v))
		if act != Set {
			t.Fatalf("offer %q action = %v, want Set", v, act)
		}
		q = nv
	}
	if got := queueCount(q); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
	if _, _, out := queueSizeProc(q, true, nil); decodeI64(out) != 3 {
		t.Fatalf("size proc = %d, want 3", decodeI64(out))
	}

	// Peek returns the head without mutating.
	if _, act, out := queuePeekProc(q, true, nil); act != Keep {
		t.Fatalf("peek action = %v, want Keep", act)
	} else if v, ok := decodeQueueItem(out); !ok || string(v) != "a" {
		t.Fatalf("peek = %q,%v, want a,true", v, ok)
	}

	// Poll drains in FIFO order; the last poll Deletes the now-empty key.
	for _, want := range []string{"a", "b", "c"} {
		nv, act, out := queuePollProc(q, true, nil)
		v, ok := decodeQueueItem(out)
		if !ok || string(v) != want {
			t.Fatalf("poll = %q,%v, want %s", v, ok, want)
		}
		if want == "c" {
			if act != Delete {
				t.Fatalf("final poll action = %v, want Delete", act)
			}
		} else if act != Set {
			t.Fatalf("poll action = %v, want Set", act)
		}
		q = nv
	}

	// Polling/peeking an empty queue reports not-found.
	if _, act, out := queuePollProc(q, false, nil); act != Keep {
		t.Fatalf("empty poll action = %v, want Keep", act)
	} else if _, ok := decodeQueueItem(out); ok {
		t.Fatal("poll on empty queue should report not-found")
	}
	if _, _, out := queuePeekProc(q, false, nil); func() bool { _, ok := decodeQueueItem(out); return ok }() {
		t.Fatal("peek on empty queue should report not-found")
	}
	if _, _, out := queueSizeProc(q, false, nil); decodeI64(out) != 0 {
		t.Fatalf("size of empty queue = %d, want 0", decodeI64(out))
	}
}

// TestQueueParsingTolerantOfTorn proves the packed-value parser stops cleanly on
// a short or torn value rather than panicking (defensive against a corrupt value).
func TestQueueParsingTorn(t *testing.T) {
	torn := []byte{0, 0, 0, 5, 'a', 'b'} // header claims 5 bytes, only 2 present
	if _, _, ok := queueHead(torn); ok {
		t.Fatal("queueHead on a torn value should report not-ok")
	}
	if got := queueCount(torn); got != 0 {
		t.Fatalf("queueCount(torn) = %d, want 0", got)
	}
	short := []byte{1, 2} // fewer than 4 header bytes
	if _, _, ok := queueHead(short); ok {
		t.Fatal("queueHead on a short value should report not-ok")
	}
	if got := queueCount(short); got != 0 {
		t.Fatalf("queueCount(short) = %d, want 0", got)
	}
	// A valid item followed by a torn tail: count the good one, stop at the tear.
	mixed := append([]byte{0, 0, 0, 1, 'x'}, 0, 0, 0, 9, 'y')
	if got := queueCount(mixed); got != 1 {
		t.Fatalf("queueCount(valid+torn) = %d, want 1", got)
	}
}

// TestQueueHandleSurfacesError proves every queue op propagates a transport error
// from a remote owner (rather than swallowing it). A two-member view routes a
// queue owned by the other member through a failing transport.
func TestQueueHandleSurfacesError(t *testing.T) {
	boom := errors.New("transport down")
	tr := &fakeTransport{err: boom}
	mem := cluster.New(cluster.Member{ID: "self", Addr: "self"}, tr, 1)
	// Two other members so some partition has self as neither owner nor backup —
	// then every Execute holder is remote and the failing transport surfaces.
	mlb, err := codec.Marshal(nil, &medusav1.MemberList{Members: []*medusav1.Member{
		{Id: "other1", Addr: "other1"}, {Id: "other2", Addr: "other2"},
	}})
	if err != nil {
		t.Fatalf("marshal member list: %v", err)
	}
	if _, _, err := mem.Handle(medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST, mlb, nil); err != nil {
		t.Fatalf("merge members: %v", err)
	}
	svc := NewService(mem, tr)
	name := keyFullyRemote(mem.Table(), "self")
	if name == nil {
		t.Fatal("setup: no queue with self as neither owner nor backup")
	}
	q := svc.Queue(string(name))
	ctx := context.Background()
	if _, err := q.Offer(ctx, []byte("v")); !errors.Is(err, boom) {
		t.Fatalf("Offer err = %v, want boom", err)
	}
	if _, _, err := q.Poll(ctx); !errors.Is(err, boom) {
		t.Fatalf("Poll err = %v, want boom", err)
	}
	if _, _, err := q.Peek(ctx); !errors.Is(err, boom) {
		t.Fatalf("Peek err = %v, want boom", err)
	}
	if _, err := q.Size(ctx); !errors.Is(err, boom) {
		t.Fatalf("Size err = %v, want boom", err)
	}
}

// TestQueueOfferPreservesBinary proves items with arbitrary bytes (including the
// length-prefix bytes and NULs) round-trip — the packing is length-prefixed, not
// delimiter-based.
func TestQueueOfferPreservesBinary(t *testing.T) {
	var q []byte
	items := [][]byte{{0x00, 0x00, 0x00, 0x02}, {}, {0xff, 0x00, 0xff}}
	for _, it := range items {
		q, _, _ = queueOfferProc(q, q != nil, it)
	}
	for i, want := range items {
		nv, _, out := queuePollProc(q, true, nil)
		v, ok := decodeQueueItem(out)
		if !ok || string(v) != string(want) {
			t.Fatalf("item %d = %v,%v, want %v", i, v, ok, want)
		}
		q = nv
	}
}

// TestEvictSkipsQueues is the regression guard for the review's critical finding:
// max-size eviction sampled victims from every map including the reserved __queue
// namespace, so under memory pressure it could applyRemove a whole packed queue
// (losing all its items). Eviction must drain regular map entries and never touch
// a queue.
func TestEvictSkipsQueues(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	ctx := context.Background()

	for _, v := range []string{"a", "b", "c"} {
		if _, err := s.Queue("q").Offer(ctx, []byte(v)); err != nil {
			t.Fatalf("offer: %v", err)
		}
	}
	for i := 0; i < 50; i++ { // regular map entries, well over the cap
		s.store.put(partition.For([]byte{byte(i)}), "m", []byte{byte(i)}, []byte("v"), 0)
	}

	// Evict down to a small cap. It must shed map entries, never the queue.
	s.Evict(ctx, 5, 1000)

	if n, err := s.Queue("q").Size(ctx); err != nil || n != 3 {
		t.Fatalf("queue size after eviction = %d,%v; want 3 (queues must not be evicted)", n, err)
	}
}

// keyFullyRemote returns a key whose partition has self as neither owner nor any
// backup, so every Execute holder for it is remote.
func keyFullyRemote(tbl *partition.Table, self string) []byte {
	for i := 0; i < 8000; i++ {
		k := []byte{byte(i), byte(i >> 8)}
		p := partition.For(k)
		if tbl.OwnerOf(p) == self {
			continue
		}
		remote := true
		for j, n := 0, tbl.NumBackups(p); j < n; j++ {
			if b, ok := tbl.BackupAt(p, j); ok && b == self {
				remote = false
				break
			}
		}
		if remote {
			return k
		}
	}
	return nil
}
