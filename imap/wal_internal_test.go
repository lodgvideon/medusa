package imap

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/partition"
)

// TestWALReplayRecoversWrites is the core durability guarantee: writes made
// after the last snapshot survive an ungraceful crash (the WAL is closed
// without a checkpoint, as the OS would on process exit) and are reconstructed
// when a fresh service replays the log.
func TestWALReplayRecoversWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	ctx := context.Background()

	s1 := svcWith(&fakeTransport{})
	if err := s1.OpenWAL(path); err != nil {
		t.Fatalf("open wal: %v", err)
	}
	mustPut(t, s1, "m", "k1", "v1")
	mustPut(t, s1, "m", "k2", "v2")
	if _, err := s1.applyRemove(ctx, "m", []byte("k1"), false); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := s1.CloseWAL(); err != nil { // crash: no checkpoint/snapshot
		t.Fatalf("close wal: %v", err)
	}

	s2 := svcWith(&fakeTransport{})
	if err := s2.OpenWAL(path); err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer s2.CloseWAL() // release the handle so t.TempDir cleanup succeeds on Windows
	if v, ok := s2.store.get(partition.For([]byte("k2")), "m", []byte("k2")); !ok || string(v) != "v2" {
		t.Fatalf("k2 after replay = %q,%v, want \"v2\",true", v, ok)
	}
	if _, ok := s2.store.get(partition.For([]byte("k1")), "m", []byte("k1")); ok {
		t.Fatal("k1 was removed before the crash; replay must not resurrect it")
	}
}

// TestWALCheckpointTruncates verifies a checkpoint captures everything into the
// snapshot and then empties the log, so replay afterwards yields only writes
// made since the checkpoint.
func TestWALCheckpointTruncates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	s := svcWith(&fakeTransport{})
	if err := s.OpenWAL(path); err != nil {
		t.Fatalf("open wal: %v", err)
	}
	for i := 0; i < 5; i++ {
		mustPut(t, s, "m", string(rune('a'+i)), "x")
	}

	captured := -1
	if err := s.Checkpoint(func(snap *medusav1.Snapshot) error {
		captured = len(snap.GetEntries())
		return nil
	}); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if captured != 5 {
		t.Fatalf("checkpoint captured %d entries, want 5", captured)
	}

	mustPut(t, s, "m", "post", "y")
	if err := s.CloseWAL(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	// A fresh service replaying the truncated WAL must see only the
	// post-checkpoint write — the five checkpointed entries are gone from it.
	s2 := svcWith(&fakeTransport{})
	if err := s2.OpenWAL(path); err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer s2.CloseWAL()
	if v, ok := s2.store.get(partition.For([]byte("post")), "m", []byte("post")); !ok || string(v) != "y" {
		t.Fatalf("post-checkpoint write after replay = %q,%v, want \"y\",true", v, ok)
	}
	if _, ok := s2.store.get(partition.For([]byte("a")), "m", []byte("a")); ok {
		t.Fatal("checkpointed entry replayed from WAL; it should have been truncated")
	}
}

// TestWALReplayStopsAtTornRecord proves an interrupted final write (a partial
// record left by a crash mid-append) is dropped without corrupting replay of
// the records before it.
func TestWALReplayStopsAtTornRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.appendPut("m", []byte("a"), []byte("1"), 0); err != nil {
		t.Fatalf("append a: %v", err)
	}
	if err := w.appendPut("m", []byte("b"), []byte("2"), 0); err != nil {
		t.Fatalf("append b: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Simulate a crash mid-append: a truncated header (fewer than 5 bytes).
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen for torn write: %v", err)
	}
	if _, err := f.Write([]byte{0x00, 0x00}); err != nil {
		t.Fatalf("write torn header: %v", err)
	}
	_ = f.Close()

	got := map[string]string{}
	if _, err := replayWAL(path,
		func(_ string, k, v []byte, _ int64) { got[string(k)] = string(v) },
		func(_ string, k []byte) { delete(got, string(k)) },
		func(_ string) { clear(got) },
	); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 2 || got["a"] != "1" || got["b"] != "2" {
		t.Fatalf("replay with torn tail = %v, want {a:1,b:2}", got)
	}
}

// TestWALReplaySkipsExpired proves a logged entry whose TTL elapsed while the
// node was down is dropped on replay, while a never-expiring one is restored.
func TestWALReplaySkipsExpired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.appendPut("m", []byte("dead"), []byte("x"), nowNano()-int64(time.Hour)); err != nil {
		t.Fatalf("append dead: %v", err)
	}
	if err := w.appendPut("m", []byte("live"), []byte("y"), 0); err != nil {
		t.Fatalf("append live: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s := svcWith(&fakeTransport{})
	if err := s.OpenWAL(path); err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer s.CloseWAL()
	if _, ok := s.store.get(partition.For([]byte("dead")), "m", []byte("dead")); ok {
		t.Error("an entry expired while down must be skipped on replay")
	}
	if _, ok := s.store.get(partition.For([]byte("live")), "m", []byte("live")); !ok {
		t.Error("a live entry must be replayed")
	}
}

// TestWALReplaySkipsCorruptRecord proves a framed record whose payload is not a
// valid SnapshotEntry ends replay cleanly, keeping the records before it.
func TestWALReplaySkipsCorruptRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.appendPut("m", []byte("a"), []byte("1"), 0); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// A well-framed record (length 3, PUT) whose payload is invalid protobuf.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 3, walOpPut, 0xff, 0xff, 0xff}); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}
	_ = f.Close()

	got := map[string]string{}
	if _, err := replayWAL(path,
		func(_ string, k, v []byte, _ int64) { got[string(k)] = string(v) },
		func(_ string, k []byte) { delete(got, string(k)) },
		func(_ string) { clear(got) },
	); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 1 || got["a"] != "1" {
		t.Fatalf("replay past corrupt record = %v, want {a:1}", got)
	}
}

// TestWALReplayRejectsOversizedRecord is the regression guard for the unbounded
// allocation found by the deep review: replay decoded a record's length and
// immediately did make([]byte, n) with no upper bound, so a corrupt or bit-
// flipped length field (e.g. 0xFFFFFFFF ≈ 4 GiB) could OOM-kill or panic the node
// on the very startup path it most needs to finish. Replay now caps the length
// at maxWALRecord and treats anything larger as a torn header, stopping cleanly.
// We assert both the clean stop (the valid record before it survives) and, via
// cumulative allocation, that no multi-gigabyte buffer was allocated — the latter
// is what fails if the cap is ever removed, even on a 64-bit host where the make
// would otherwise transiently succeed.
func TestWALReplayRejectsOversizedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.appendPut("m", []byte("a"), []byte("1"), 0); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Append a header declaring a ~4 GiB payload — a corrupt/bit-flipped length.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := f.Write([]byte{0xff, 0xff, 0xff, 0xff, walOpPut}); err != nil {
		t.Fatalf("write oversized header: %v", err)
	}
	_ = f.Close()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got := map[string]string{}
	if _, err := replayWAL(path,
		func(_ string, k, v []byte, _ int64) { got[string(k)] = string(v) },
		func(_ string, k []byte) { delete(got, string(k)) },
		func(_ string) { clear(got) },
	); err != nil {
		t.Fatalf("replay: %v", err)
	}
	runtime.ReadMemStats(&after)

	if len(got) != 1 || got["a"] != "1" {
		t.Fatalf("replay past oversized record = %v, want {a:1}", got)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<30 {
		t.Fatalf("replay allocated %d bytes for an oversized record; the length cap is missing", grew)
	}
}

// TestWALErrorPaths covers the failure and closed-file branches.
func TestWALErrorPaths(t *testing.T) {
	if _, err := openWAL(filepath.Join(t.TempDir(), "no-such-dir", "wal.log")); err == nil {
		t.Error("openWAL on a missing parent directory should error")
	}

	w, err := openWAL(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := w.close(); err != nil { // idempotent
		t.Errorf("second close = %v, want nil", err)
	}
	if err := w.appendPut("m", []byte("k"), []byte("v"), 0); err == nil {
		t.Error("appendPut after close should report the log is closed")
	}
	if err := w.truncateLocked(); err == nil {
		t.Error("truncateLocked after close should report the log is closed")
	}
}

// TestServiceWALDisabled exercises the wal==nil branches: writes, Checkpoint and
// CloseWAL all work when durability logging is off.
func TestServiceWALDisabled(t *testing.T) {
	s := svcWith(&fakeTransport{}) // no OpenWAL → WAL disabled
	mustPut(t, s, "m", "k", "v")
	if err := s.CloseWAL(); err != nil {
		t.Errorf("CloseWAL with no WAL = %v, want nil", err)
	}
	captured := -1
	if err := s.Checkpoint(func(snap *medusav1.Snapshot) error {
		captured = len(snap.GetEntries())
		return nil
	}); err != nil {
		t.Fatalf("checkpoint without WAL: %v", err)
	}
	if captured != 1 {
		t.Fatalf("checkpoint captured %d entries, want 1", captured)
	}
}

// BenchmarkWALAppend measures the cost of one durable (fsync'd) log append —
// the per-write overhead of enabling persistence. fsync dominates, so this is
// far slower than the in-memory write itself; it quantifies the durability
// trade-off rather than a hot in-memory path.
func BenchmarkWALAppend(b *testing.B) {
	w, err := openWAL(filepath.Join(b.TempDir(), "wal.log"))
	if err != nil {
		b.Fatalf("open wal: %v", err)
	}
	defer w.close()
	key := []byte("hot-key")
	val := []byte("a-cache-value-payload")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.appendPut("m", key, val, 0); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

// TestWALOpenChopsTornTail proves OpenWAL trims a torn tail so a write appended
// afterwards is not shadowed by the remnant on a later replay (the double-crash
// durability window).
func TestWALOpenChopsTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	// One good record followed by a torn tail (a header promising bytes that
	// never arrive), as an interrupted append would leave.
	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.appendPut("m", []byte("first"), []byte("1"), 0); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 9, walOpPut}); err != nil {
		t.Fatalf("write torn tail: %v", err)
	}
	_ = f.Close()

	// First restart: replay "first", chop the torn tail, then append "second".
	s1 := svcWith(&fakeTransport{})
	if err := s1.OpenWAL(path); err != nil {
		t.Fatalf("open wal s1: %v", err)
	}
	mustPut(t, s1, "m", "second", "2")
	if err := s1.CloseWAL(); err != nil {
		t.Fatalf("close wal s1: %v", err)
	}

	// Second restart must recover BOTH records.
	s2 := svcWith(&fakeTransport{})
	if err := s2.OpenWAL(path); err != nil {
		t.Fatalf("open wal s2: %v", err)
	}
	defer s2.CloseWAL()
	for _, kv := range []struct{ k, v string }{{"first", "1"}, {"second", "2"}} {
		if got, ok := s2.store.get(partition.For([]byte(kv.k)), "m", []byte(kv.k)); !ok || string(got) != kv.v {
			t.Fatalf("%q after torn-tail recovery = %q,%v, want %q", kv.k, got, ok, kv.v)
		}
	}
}

// TestWALConcurrentAppendsAllDurable hammers the WAL from many goroutines at
// once and proves nothing is lost under concurrency: every acknowledged write
// is recovered by a fresh replay. Run under -race in CI, it also guards the
// append/checkpoint locking against data races.
func TestWALConcurrentAppendsAllDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	s := svcWith(&fakeTransport{})
	if err := s.OpenWAL(path); err != nil {
		t.Fatalf("open wal: %v", err)
	}
	ctx := context.Background()

	const writers, perWriter = 8, 50
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := []byte{byte(g), byte(i)}
				if _, err := s.applyPut(ctx, "m", key, []byte{byte(g), byte(i)}, 0, false); err != nil {
					t.Errorf("put (%d,%d): %v", g, i, err)
				}
			}
		}(g)
	}
	wg.Wait()
	if err := s.CloseWAL(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	// Every acknowledged write must replay into a fresh store.
	s2 := svcWith(&fakeTransport{})
	if err := s2.OpenWAL(path); err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer s2.CloseWAL()
	for g := 0; g < writers; g++ {
		for i := 0; i < perWriter; i++ {
			key := []byte{byte(g), byte(i)}
			v, ok := s2.store.get(partition.For(key), "m", key)
			if !ok || len(v) != 2 || v[0] != byte(g) || v[1] != byte(i) {
				t.Fatalf("key (%d,%d) = %q,%v after concurrent replay, want present", g, i, v, ok)
			}
		}
	}
}

func mustPut(t *testing.T, s *Service, name, key, val string) {
	t.Helper()
	if _, err := s.applyPut(context.Background(), name, []byte(key), []byte(val), 0, false); err != nil {
		t.Fatalf("applyPut %s/%s: %v", name, key, err)
	}
}
