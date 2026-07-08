package imap

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/medusa/cluster"
	"github.com/lodgvideon/medusa/codec"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/partition"
)

// TestQueueMetaCodec covers the [head][tail][count] metadata codec, including
// short/absent input (an empty queue) and a round-trip.
func TestQueueMetaCodec(t *testing.T) {
	if h, tl, c := decodeQueueMeta(nil); h != 0 || tl != 0 || c != 0 {
		t.Fatalf("decode(nil) = %d,%d,%d want zeros", h, tl, c)
	}
	if h, tl, c := decodeQueueMeta([]byte{1, 2, 3}); h != 0 || tl != 0 || c != 0 {
		t.Fatalf("decode(short) = %d,%d,%d want zeros", h, tl, c)
	}
	if h, tl, c := decodeQueueMeta(encodeQueueMeta(7, 9, 42)); h != 7 || tl != 9 || c != 42 {
		t.Fatalf("roundtrip = %d,%d,%d want 7,9,42", h, tl, c)
	}
}

// TestRoutedPartitionAffinity proves a queue's segments route to the QUEUE NAME's
// partition (co-location with the metadata), not their own key's — the property
// that makes queue ops atomic on one owner — and that ordinary maps still route
// by key.
func TestRoutedPartitionAffinity(t *testing.T) {
	name := "orders"
	want := partition.For([]byte(name))
	for _, seg := range []uint64{0, 1, 7, 1 << 40} {
		if got := routedPartition(queueSegMap, queueSegKey(seg, name)); got != want {
			t.Fatalf("segment %d routed to %d, want the name's partition %d", seg, got, want)
		}
	}
	if got := routedPartition(queueMap, []byte(name)); got != want {
		t.Fatalf("metadata routed to %d, want %d", got, want)
	}
	if got := routedPartition("m", []byte("k")); got != partition.For([]byte("k")) {
		t.Fatal("ordinary map keys must route by their own key")
	}
}

// TestQueueParsingTorn proves the segment parser stops cleanly on a short/torn
// value rather than panicking.
func TestQueueParsingTorn(t *testing.T) {
	torn := []byte{0, 0, 0, 5, 'a', 'b'}
	if _, _, ok := queueHead(torn); ok {
		t.Fatal("queueHead on a torn value should report not-ok")
	}
	if got := queueCount(torn); got != 0 {
		t.Fatalf("queueCount(torn) = %d, want 0", got)
	}
	mixed := append([]byte{0, 0, 0, 1, 'x'}, 0, 0, 0, 9, 'y')
	if got := queueCount(mixed); got != 1 {
		t.Fatalf("queueCount(valid+torn) = %d, want 1", got)
	}
}

// TestQueueOpUnknown proves an unknown queue operation errors without mutating.
func TestQueueOpUnknown(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	if _, err := s.Map(queueMap).Execute(context.Background(), []byte("q"), "nosuch", nil); err == nil {
		t.Fatal("unknown queue op must error")
	}
	if n, _ := s.Queue("q").Size(context.Background()); n != 0 {
		t.Fatalf("size after failed op = %d, want 0 (nothing mutated)", n)
	}
}

// TestExecuteOnSegmentNamespaceRejected proves direct Executes against the
// segment namespace are refused — segments are mutated only inside queue
// transactions.
func TestExecuteOnSegmentNamespaceRejected(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	if _, err := s.Map(queueSegMap).Execute(context.Background(), []byte("k"), "incr", nil); !errors.Is(err, errReservedMap) {
		t.Fatalf("Execute on %s = %v, want errReservedMap", queueSegMap, err)
	}
}

// TestQueueBinaryRoundTrip proves items with arbitrary bytes (NULs, length-prefix
// look-alikes, empty) round-trip through Offer/Poll.
func TestQueueBinaryRoundTrip(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	ctx := context.Background()
	q := s.Queue("bin")
	items := [][]byte{{0x00, 0x00, 0x00, 0x02}, {}, {0xff, 0x00, 0xff}}
	for _, it := range items {
		if _, err := q.Offer(ctx, it); err != nil {
			t.Fatalf("offer: %v", err)
		}
	}
	for i, want := range items {
		v, ok, err := q.Poll(ctx)
		if err != nil || !ok || string(v) != string(want) {
			t.Fatalf("poll %d = %v,%v,%v want %v", i, v, ok, err, want)
		}
	}
	if _, ok, _ := q.Poll(ctx); ok {
		t.Fatal("queue should be empty after draining")
	}
}

// TestQueueRollsSegmentsFIFO drives enough data through one queue to span many
// segments and proves strict global FIFO holds across segment rollovers, Size
// tracks the count, an oversized item (> one segment) still round-trips, and a
// drained queue reclaims its entries.
func TestQueueRollsSegmentsFIFO(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	ctx := context.Background()
	q := s.Queue("roll")

	const n = 300 // ~1 KiB each => several 64 KiB segments
	for i := 0; i < n; i++ {
		v := append([]byte(fmt.Sprintf("%08d:", i)), make([]byte, 1024)...)
		if _, err := q.Offer(ctx, v); err != nil {
			t.Fatalf("offer %d: %v", i, err)
		}
	}
	big := make([]byte, maxSegBytes+512) // oversized: must land alone in a segment
	big[0] = 'B'
	if _, err := q.Offer(ctx, big); err != nil {
		t.Fatalf("offer oversized: %v", err)
	}
	if sz, err := q.Size(ctx); err != nil || sz != n+1 {
		t.Fatalf("size = %d,%v want %d", sz, err, n+1)
	}
	if v, ok, err := q.Peek(ctx); err != nil || !ok || string(v[:9]) != "00000000:" {
		t.Fatalf("peek = %q,%v,%v want first element", v[:min(len(v), 9)], ok, err)
	}
	for i := 0; i < n; i++ {
		v, ok, err := q.Poll(ctx)
		if err != nil || !ok {
			t.Fatalf("poll %d = %v,%v", i, ok, err)
		}
		want := fmt.Sprintf("%08d:", i)
		if string(v[:len(want)]) != want {
			t.Fatalf("poll %d prefix = %q, want %q (FIFO must hold across rollovers)", i, v[:len(want)], want)
		}
	}
	v, ok, err := q.Poll(ctx)
	if err != nil || !ok || len(v) != len(big) || v[0] != 'B' {
		t.Fatalf("oversized poll = len %d,%v,%v want len %d", len(v), ok, err, len(big))
	}
	if _, ok, _ := q.Poll(ctx); ok {
		t.Fatal("queue not empty after draining")
	}
	// Fully drained: segments are reclaimed; the 24-byte metadata persists so
	// segment ids stay monotonic (a stale old-id segment can never merge back in).
	if got := s.store.countMap(queueSegMap, func(int) bool { return true }); got != 0 {
		t.Fatalf("drained queue left %d segment entries", got)
	}
	if got := s.store.countMap(queueMap, func(int) bool { return true }); got != 1 {
		t.Fatalf("drained queue kept %d metadata entries, want 1 (monotonic ids)", got)
	}
	// The queue remains fully usable after draining.
	if _, err := q.Offer(ctx, []byte("again")); err != nil {
		t.Fatalf("offer after drain: %v", err)
	}
	if v, ok, err := q.Poll(ctx); err != nil || !ok || string(v) != "again" {
		t.Fatalf("poll after drain = %q,%v,%v", v, ok, err)
	}
}

// TestQueuePollIncompleteStateNonDestructive is the regression guard for the
// review's critical finding: during a migration the metadata can arrive (or
// survive) while a segment is still in flight; a poll observing count > 0 with
// the head segment absent must report empty WITHOUT mutating — advancing head or
// deleting the metadata on that incomplete view permanently destroys elements.
func TestQueuePollIncompleteStateNonDestructive(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	ctx := context.Background()

	// Simulate the mid-migration view: metadata says one element lives in segment
	// 5, but that segment has not arrived.
	name := "mig"
	meta := encodeQueueMeta(5, 5, 1)
	p := routedPartition(queueMap, []byte(name))
	s.store.put(p, queueMap, []byte(name), meta, 0)

	if _, ok, err := s.Queue(name).Poll(ctx); err != nil || ok {
		t.Fatalf("poll on incomplete state = %v,%v; want empty, no error", ok, err)
	}
	if v, k := s.store.get(p, queueMap, []byte(name)); !k || string(v) != string(meta) {
		t.Fatal("poll on incomplete state mutated the metadata (head advance or delete)")
	}
	// The segment "arrives" (migration completes): the element is delivered.
	s.store.put(p, queueSegMap, queueSegKey(5, name), appendPacked(nil, []byte("x")), 0)
	if v, ok, err := s.Queue(name).Poll(ctx); err != nil || !ok || string(v) != "x" {
		t.Fatalf("poll after segment arrival = %q,%v,%v; want x", v, ok, err)
	}
}

// TestQueueWALReplayRestores proves a queue op's multi-entry transaction is
// WAL-logged and replayed: offers (spanning a segment roll) and a poll survive a
// crash+replay with FIFO intact.
func TestQueueWALReplayRestores(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "wal.log")
	s := svcWith(&fakeTransport{})
	if err := s.OpenWAL(walPath); err != nil {
		t.Fatalf("open wal: %v", err)
	}
	ctx := context.Background()
	q := s.Queue("dur")
	big := make([]byte, maxSegBytes-8) // forces the second offer to roll
	for _, v := range [][]byte{big, []byte("second"), []byte("third")} {
		if _, err := q.Offer(ctx, v); err != nil {
			t.Fatalf("offer: %v", err)
		}
	}
	if v, ok, _ := q.Poll(ctx); !ok || len(v) != len(big) {
		t.Fatal("first poll should return the big element")
	}
	if err := s.CloseWAL(); err != nil { // crash before any checkpoint
		t.Fatalf("close wal: %v", err)
	}

	s2 := svcWith(&fakeTransport{})
	if err := s2.OpenWAL(walPath); err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	defer s2.CloseWAL()
	q2 := s2.Queue("dur")
	if n, _ := q2.Size(ctx); n != 2 {
		t.Fatalf("size after replay = %d, want 2", n)
	}
	for _, want := range []string{"second", "third"} {
		v, ok, err := q2.Poll(ctx)
		if err != nil || !ok || string(v) != want {
			t.Fatalf("post-replay poll = %q,%v,%v; want %s", v, ok, err, want)
		}
	}
}

// TestLegacyQueueConversion proves a pre-segmentation queue value (the whole
// queue as one packed stream) is upgraded on restore, preserving FIFO order.
func TestLegacyQueueConversion(t *testing.T) {
	var packed []byte
	for _, v := range []string{"a", "b", "c"} {
		packed = appendPacked(packed, []byte(v))
	}
	s := svcWith(&fakeTransport{})
	defer s.Close()
	s.store.loadAll([]entry{{mapName: queueMap, key: "legacy", value: packed}})

	ctx := context.Background()
	q := s.Queue("legacy")
	if n, _ := q.Size(ctx); n != 3 {
		t.Fatalf("size after legacy conversion = %d, want 3", n)
	}
	for _, want := range []string{"a", "b", "c"} {
		v, ok, err := q.Poll(ctx)
		if err != nil || !ok || string(v) != want {
			t.Fatalf("poll = %q,%v,%v; want %s", v, ok, err, want)
		}
	}
}

// TestQueueConcurrentNoLoss is the regression guard against the strand races that
// sank the client-orchestrated designs: with producers and consumers running
// concurrently, EVERY offered element must be delivered exactly once (single
// node, so no failover redelivery) — none silently stranded, none duplicated.
func TestQueueConcurrentNoLoss(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	ctx := context.Background()
	q := s.Queue("cc")

	const producers, perProducer = 4, 250
	total := producers * perProducer

	var pwg sync.WaitGroup
	for p := 0; p < producers; p++ {
		pwg.Add(1)
		go func(p int) {
			defer pwg.Done()
			for i := 0; i < perProducer; i++ {
				if _, err := q.Offer(ctx, []byte(fmt.Sprintf("%d-%d", p, i))); err != nil {
					t.Errorf("offer: %v", err)
					return
				}
			}
		}(p)
	}

	var mu sync.Mutex
	got := make(map[string]int)
	consumed := 0
	stop := make(chan struct{})
	var cwg sync.WaitGroup
	for c := 0; c < 3; c++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, ok, err := q.Poll(ctx)
				if err != nil {
					t.Errorf("poll: %v", err)
					return
				}
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				mu.Lock()
				got[string(v)]++
				consumed++
				done := consumed >= total
				mu.Unlock()
				if done {
					return
				}
			}
		}()
	}

	pwg.Wait()
	deadline := time.After(30 * time.Second)
	for {
		mu.Lock()
		c := consumed
		mu.Unlock()
		if c >= total {
			break
		}
		select {
		case <-deadline:
			close(stop)
			cwg.Wait()
			t.Fatalf("consumed only %d/%d — elements were silently lost (strand)", c, total)
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(stop)
	cwg.Wait()

	if len(got) != total {
		t.Fatalf("distinct elements delivered = %d, want %d", len(got), total)
	}
	for k, n := range got {
		if n != 1 {
			t.Fatalf("element %q delivered %d times, want exactly 1", k, n)
		}
	}
}

// TestQueueHandleSurfacesError proves every queue op propagates a transport error
// from a remote owner (rather than swallowing it).
func TestQueueHandleSurfacesError(t *testing.T) {
	boom := errors.New("transport down")
	tr := &fakeTransport{err: boom}
	mem := cluster.New(cluster.Member{ID: "self", Addr: "self"}, tr, 1)
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
		t.Fatal("setup: no queue whose partition is fully remote")
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

// TestEvictSkipsQueues guards that max-size eviction never touches a queue's
// metadata or segments (both reserved namespaces), so queued elements survive
// memory pressure.
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
