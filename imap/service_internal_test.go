package imap

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/medusa/cluster"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/partition"
	"github.com/lodgvideon/medusa/transport"
)

// fakeTransport lets us drive the send helpers' response-handling branches
// (unexpected type, transport error) without a real peer.
type fakeTransport struct {
	respType medusav1.MessageType
	err      error
}

func (f *fakeTransport) Addr() string                   { return "fake" }
func (f *fakeTransport) Listen(transport.Handler) error { return nil }
func (f *fakeTransport) Close() error                   { return nil }
func (f *fakeTransport) Send(_ context.Context, _ string, _ medusav1.MessageType, _, dst []byte) (medusav1.MessageType, []byte, error) {
	if f.err != nil {
		return 0, dst, f.err
	}
	return f.respType, dst[:0], nil
}

func svcWith(tr transport.Transport) *Service {
	mem := cluster.New(cluster.Member{ID: "self", Addr: "self"}, tr, 1)
	return NewService(mem, tr)
}

func TestSendGetRejectsUnexpectedType(t *testing.T) {
	svc := svcWith(&fakeTransport{respType: medusav1.MessageType_MESSAGE_TYPE_PUT_RESPONSE})
	if _, _, err := svc.sendGet(context.Background(), "peer", "m", []byte("k")); err == nil {
		t.Fatal("want error on unexpected get response type")
	}
}

func TestSendPutRejectsUnexpectedType(t *testing.T) {
	svc := svcWith(&fakeTransport{respType: medusav1.MessageType_MESSAGE_TYPE_GET_RESPONSE})
	if err := svc.sendPut(context.Background(), "peer", "m", []byte("k"), []byte("v"), 0, false); err == nil {
		t.Fatal("want error on unexpected put response type")
	}
}

func TestSendRemoveRejectsUnexpectedType(t *testing.T) {
	svc := svcWith(&fakeTransport{respType: medusav1.MessageType_MESSAGE_TYPE_GET_RESPONSE})
	if _, err := svc.sendRemove(context.Background(), "peer", "m", []byte("k"), false); err == nil {
		t.Fatal("want error on unexpected remove response type")
	}
}

func TestSendHelpersPropagateTransportError(t *testing.T) {
	boom := errors.New("transport down")
	svc := svcWith(&fakeTransport{err: boom})
	ctx := context.Background()

	if _, _, err := svc.sendGet(ctx, "peer", "m", []byte("k")); !errors.Is(err, boom) {
		t.Errorf("sendGet err = %v, want boom", err)
	}
	if err := svc.sendPut(ctx, "peer", "m", []byte("k"), []byte("v"), 0, false); !errors.Is(err, boom) {
		t.Errorf("sendPut err = %v, want boom", err)
	}
	if _, err := svc.sendRemove(ctx, "peer", "m", []byte("k"), false); !errors.Is(err, boom) {
		t.Errorf("sendRemove err = %v, want boom", err)
	}
}

// TestDropMigratedKeepsRacingWrites is the regression guard for the migration
// data-loss race: a write that lands after Migrate snapshots a partition but
// before it drops it must not be wiped. dropMigrated only deletes entries whose
// value is unchanged since the snapshot.
func TestDropMigratedKeepsRacingWrites(t *testing.T) {
	st := newStore()
	key := []byte("k")
	p := partition.For(key)
	st.put(p, "m", key, []byte("v1"), 0)

	// Snapshot captures v1, as Migrate does before pushing to the new owner.
	snap := st.snapshotPartition(p)

	// A write races in after the snapshot, overwriting the value.
	st.put(p, "m", key, []byte("v2"), 0)

	// Dropping the migrated (v1) snapshot must NOT delete the now-changed entry.
	st.dropMigrated(p, snap)
	if v, ok := st.get(p, "m", key); !ok || string(v) != "v2" {
		t.Fatalf("racing write lost: got %q,%v want \"v2\",true", v, ok)
	}

	// Dropping a snapshot that matches the current value removes it.
	st.dropMigrated(p, st.snapshotPartition(p))
	if _, ok := st.get(p, "m", key); ok {
		t.Fatal("unchanged migrated entry should have been dropped")
	}
}

// twoNodeServices builds two map services, a and b, on a shared in-memory switch
// with backups=1, converged so each sees the other. Returns the services and a's
// (frozen) partition table — no maintenance loop runs, so the table is stable.
func twoNodeServices(t *testing.T) (a, b *Service, aMem *cluster.Membership) {
	t.Helper()
	sw := transport.NewSwitch()
	mk := func(id string) (*Service, *cluster.Membership) {
		tr := sw.NewTransport(id)
		mem := cluster.New(cluster.Member{ID: id, Addr: id}, tr, 1)
		svc := NewService(mem, tr)
		h := func(rt medusav1.MessageType, req, rb []byte) (medusav1.MessageType, []byte, error) {
			switch rt {
			case medusav1.MessageType_MESSAGE_TYPE_JOIN_REQUEST,
				medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST,
				medusav1.MessageType_MESSAGE_TYPE_HEARTBEAT:
				return mem.Handle(rt, req, rb)
			default:
				return svc.Handle(rt, req, rb)
			}
		}
		if err := tr.Listen(h); err != nil {
			t.Fatalf("listen %s: %v", id, err)
		}
		t.Cleanup(func() { _ = tr.Close() })
		return svc, mem
	}
	aSvc, am := mk("a")
	bSvc, bm := mk("b")
	if err := bm.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("join: %v", err)
	}
	if len(am.Members()) != 2 || len(bm.Members()) != 2 {
		t.Fatalf("not converged: a=%d b=%d", len(am.Members()), len(bm.Members()))
	}
	return aSvc, bSvc, am
}

// ownedByAWithBackupB finds a partition + key whose owner is a and first backup b.
func ownedByAWithBackupB(t *testing.T, tbl *partition.Table) (int, []byte) {
	t.Helper()
	for i := 0; i < 200000; i++ {
		k := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		p := partition.For(k)
		if tbl.OwnerOf(p) == "a" {
			if b0, ok := tbl.BackupAt(p, 0); ok && b0 == "b" {
				return p, k
			}
		}
	}
	t.Fatal("no partition owned by a with backup b")
	return 0, nil
}

// TestSyncBackupsHealsDivergentBackup is the core anti-entropy property: a backup
// that is missing an entry the owner holds (as if it had missed a best-effort
// replication) is healed by an anti-entropy pass over that partition.
func TestSyncBackupsHealsDivergentBackup(t *testing.T) {
	a, b, aMem := twoNodeServices(t)
	tbl := aMem.Table()
	p, key := ownedByAWithBackupB(t, tbl)

	// Simulate a missed replication: the entry exists only on the owner.
	a.store.put(p, "m", key, []byte("v"), 0)
	if _, ok := b.store.get(p, "m", key); ok {
		t.Fatal("precondition: backup should not have the entry yet")
	}

	pushed, next := a.SyncBackups(context.Background(), tbl, p, 1)
	if pushed != 1 {
		t.Fatalf("pushed %d entries, want 1", pushed)
	}
	if next != (p+1)%partition.Count {
		t.Fatalf("next cursor = %d, want %d", next, (p+1)%partition.Count)
	}
	if v, ok := b.store.get(p, "m", key); !ok || string(v) != "v" {
		t.Fatalf("backup not healed: got %q,%v want \"v\",true", v, ok)
	}
}

// TestPartitionDigest covers the digest's defining properties: equal content
// (regardless of expiry) hashes equally; a different value or an extra key
// changes it; an empty partition is zero.
func TestPartitionDigest(t *testing.T) {
	key := []byte("k")
	p := partition.For(key)

	future1 := nowNano() + int64(time.Hour)
	future2 := nowNano() + int64(2*time.Hour)
	a := newStore()
	a.put(p, "m", key, []byte("v"), future1)
	b := newStore()
	b.put(p, "m", key, []byte("v"), future2) // same value, different (still-valid) expiry

	if a.partitionDigest(p) != b.partitionDigest(p) {
		t.Fatal("digest must ignore expireAt for the same value")
	}
	if newStore().partitionDigest(p) != 0 {
		t.Fatal("empty partition digest must be 0")
	}

	diff := newStore()
	diff.put(p, "m", key, []byte("OTHER"), 0)
	if a.partitionDigest(p) == diff.partitionDigest(p) {
		t.Fatal("digest must differ for a different value")
	}

	a.put(p, "m", []byte("k2"), []byte("v2"), 0) // extra key
	if a.partitionDigest(p) == b.partitionDigest(p) {
		t.Fatal("digest must differ when one side has an extra key")
	}
}

// TestPartitionDigestBinaryBoundary is the regression guard for the digest
// boundary-ambiguity bug: with a fixed separator, (key="k\x00",value="v") and
// (key="k",value="\x00v") hashed identically and XOR-cancelled, falsely matching
// an empty backup. Length-prefixing must keep them distinct.
func TestPartitionDigestBinaryBoundary(t *testing.T) {
	a := newStore()
	a.put(0, "m", []byte("k\x00"), []byte("v"), 0)
	b := newStore()
	b.put(0, "m", []byte("k"), []byte("\x00v"), 0)
	if a.partitionDigest(0) == b.partitionDigest(0) {
		t.Fatal("length-prefixing must distinguish binary key/value boundary shifts")
	}
	// And the map-name/key boundary.
	c := newStore()
	c.put(0, "a\x00", []byte("b"), []byte("v"), 0)
	d := newStore()
	d.put(0, "a", []byte("\x00b"), []byte("v"), 0)
	if c.partitionDigest(0) == d.partitionDigest(0) {
		t.Fatal("length-prefixing must distinguish map-name/key boundary shifts")
	}
}

// TestHandleDigestRequestOutOfRange verifies an out-of-range partition on the
// wire is rejected with an error rather than panicking on a slice index.
func TestHandleDigestRequestOutOfRange(t *testing.T) {
	svc := svcWith(&fakeTransport{})
	req := &medusav1.DigestRequest{Partition: uint32(partition.Count) + 7}
	b, err := req.MarshalVT()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, _, err := svc.Handle(medusav1.MessageType_MESSAGE_TYPE_DIGEST_REQUEST, b, nil); err == nil {
		t.Fatal("out-of-range digest partition must return an error, not panic")
	}
}

// TestSyncBackupsSkipsInSyncBackup verifies the digest gate: when the backup
// already holds identical content, the owner sends no data (pushed == 0).
func TestSyncBackupsSkipsInSyncBackup(t *testing.T) {
	a, b, aMem := twoNodeServices(t)
	tbl := aMem.Table()
	p, key := ownedByAWithBackupB(t, tbl)

	a.store.put(p, "m", key, []byte("v"), 0)
	b.store.put(p, "m", key, []byte("v"), 0) // backup already in sync

	if pushed, _ := a.SyncBackups(context.Background(), tbl, p, 1); pushed != 0 {
		t.Fatalf("in-sync backup must not be pushed to; pushed %d", pushed)
	}
}

// TestSyncBackupsOnlyOwnerPushes verifies a node does not push partitions it does
// not own (the owner is the single source of truth) and that the cursor wraps.
func TestSyncBackupsOnlyOwnerPushes(t *testing.T) {
	a, _, aMem := twoNodeServices(t)
	tbl := aMem.Table()

	// A partition a does NOT own: put a local entry and confirm SyncBackups skips it.
	var foreign int = -1
	for i := 0; i < 200000; i++ {
		k := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		if p := partition.For(k); tbl.OwnerOf(p) == "b" {
			a.store.put(p, "m", k, []byte("stale"), 0)
			foreign = p
			break
		}
	}
	if foreign < 0 {
		t.Fatal("no partition owned by b")
	}
	if pushed, _ := a.SyncBackups(context.Background(), tbl, foreign, 1); pushed != 0 {
		t.Fatalf("pushed %d for a non-owned partition, want 0", pushed)
	}

	// Cursor wraps around Count.
	if _, next := a.SyncBackups(context.Background(), tbl, partition.Count-1, 1); next != 0 {
		t.Fatalf("cursor did not wrap: got %d, want 0", next)
	}
}

// TestLookupReflectsRemoval covers the building block of the anti-entropy
// resurrection fix: lookup reports a removed key as absent, so SyncBackups's
// re-read-before-push skips a key the owner deleted since the snapshot.
func TestLookupReflectsRemoval(t *testing.T) {
	st := newStore()
	key := []byte("k")
	p := partition.For(key)
	st.put(p, "m", key, []byte("v"), 0)

	data, exp, ok := st.lookup(p, "m", key)
	if !ok || string(data) != "v" || exp != 0 {
		t.Fatalf("lookup after put = %q,%d,%v; want \"v\",0,true", data, exp, ok)
	}
	st.remove(p, "m", key)
	if _, _, ok := st.lookup(p, "m", key); ok {
		t.Fatal("lookup must report a removed key as absent so anti-entropy won't resurrect it")
	}
}

// TestMigrateWALsHandoffDropNoResurrection is the regression guard for the
// cross-feature durability bug found by the interaction review: Migrate's
// hand-off deletion (dropMigrated) was not WAL-logged, so a crash between the
// migration and the next checkpoint replayed the pre-drop snapshot and
// resurrected entries for a partition the node no longer owns. Migrate now WALs
// the drop; a snapshot+replay after the drop must NOT bring the key back.
func TestMigrateWALsHandoffDropNoResurrection(t *testing.T) {
	sw := transport.NewSwitch()
	mk := func(id string) (*Service, *cluster.Membership) {
		tr := sw.NewTransport(id)
		mem := cluster.New(cluster.Member{ID: id, Addr: id}, tr, 1)
		svc := NewService(mem, tr)
		h := func(rt medusav1.MessageType, req, rb []byte) (medusav1.MessageType, []byte, error) {
			switch rt {
			case medusav1.MessageType_MESSAGE_TYPE_JOIN_REQUEST,
				medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST,
				medusav1.MessageType_MESSAGE_TYPE_HEARTBEAT:
				return mem.Handle(rt, req, rb)
			default:
				return svc.Handle(rt, req, rb)
			}
		}
		if err := tr.Listen(h); err != nil {
			t.Fatalf("listen %s: %v", id, err)
		}
		t.Cleanup(func() { _ = tr.Close() })
		return svc, mem
	}
	a, _ := mk("a")
	_, bMem := mk("b")
	if err := bMem.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("join: %v", err)
	}
	walPath := filepath.Join(t.TempDir(), "wal.log")
	if err := a.OpenWAL(walPath); err != nil {
		t.Fatalf("open wal: %v", err)
	}

	key := []byte("kx")
	p := partition.For(key)
	if _, err := a.applyPut(context.Background(), "m", key, []byte("v"), 0, true); err != nil {
		t.Fatalf("applyPut: %v", err)
	}
	// Checkpoint: the snapshot now holds the key and the WAL is truncated — this
	// is the state a restart would load from disk.
	var snap *medusav1.Snapshot
	if err := a.Checkpoint(func(s *medusav1.Snapshot) error { snap = s; return nil }); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// Hand the partition off to a table where a is no longer a holder ("b" owns
	// everything). Migrate pushes to b, drops locally, and WALs the drop.
	a.Migrate(context.Background(), partition.NewTable([]string{"b"}, 1))
	if _, ok := a.store.get(p, "m", key); ok {
		t.Fatal("precondition: a should have dropped the migrated-away key")
	}
	// Crash before the next checkpoint: close the WAL without truncating.
	if err := a.CloseWAL(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	// Restart: load the PRE-drop snapshot, then replay the WAL. The hand-off
	// remove must keep the key from resurrecting.
	a2 := svcWith(&fakeTransport{})
	a2.Restore(snap)
	if _, ok := a2.store.get(p, "m", key); !ok {
		t.Fatal("snapshot should contain the key before WAL replay (test setup)")
	}
	if err := a2.OpenWAL(walPath); err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer a2.CloseWAL()
	if _, ok := a2.store.get(p, "m", key); ok {
		t.Fatal("migrated-away key resurrected after crash+replay; the hand-off drop was not WAL'd")
	}
}

// TestClearWALNoResurrection guards the clear durability path: after a checkpoint
// captures the entries, a clear writes a single WAL record; a crash before the
// next checkpoint must replay the clear (not the pre-clear snapshot entries).
func TestClearWALNoResurrection(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "wal.log")
	s := svcWith(&fakeTransport{})
	if err := s.OpenWAL(walPath); err != nil {
		t.Fatalf("open wal: %v", err)
	}
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c"} {
		if _, err := s.applyPut(ctx, "m", []byte(k), []byte("v"), 0, true); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	// Checkpoint: snapshot holds the 3 entries, WAL truncated.
	var snap *medusav1.Snapshot
	if err := s.Checkpoint(func(sn *medusav1.Snapshot) error { snap = sn; return nil }); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if n, err := s.localClearMap("m"); err != nil || n != 3 {
		t.Fatalf("clear = %d,%v; want 3,nil", n, err)
	}
	if err := s.CloseWAL(); err != nil { // crash before the next checkpoint
		t.Fatalf("close wal: %v", err)
	}

	all := func(svc *Service) int { return svc.store.countMap("m", func(int) bool { return true }) }

	s2 := svcWith(&fakeTransport{})
	s2.Restore(snap)
	if got := all(s2); got != 3 {
		t.Fatalf("snapshot should hold 3 entries before replay; got %d", got)
	}
	if err := s2.OpenWAL(walPath); err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer s2.CloseWAL()
	if got := all(s2); got != 0 {
		t.Fatalf("clear not replayed: %d entries resurrected after crash+replay", got)
	}
}

// TestWALReplayMatchesLiveUnderConcurrentConflict is the regression guard for the
// WAL store-order vs append-order bug: a put racing a remove (or a clear) of the
// same key/map must, however the race resolves in the live store, be replayed by
// a snapshot-less WAL recovery to EXACTLY that live state. Before the fix that
// holds the WAL lock across the store mutation and its append, the two orders
// could diverge and replay would lose the put or resurrect the removed key.
func TestWALReplayMatchesLiveUnderConcurrentConflict(t *testing.T) {
	dir := t.TempDir()
	key := []byte("k")
	p := partition.For(key)
	ctx := context.Background()

	run := func(label string, rounds int, racer func(s *Service)) {
		for r := 0; r < rounds; r++ {
			path := filepath.Join(dir, label+strconv.Itoa(r)+".log")
			s := svcWith(&fakeTransport{})
			if err := s.OpenWAL(path); err != nil {
				t.Fatalf("%s open: %v", label, err)
			}
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); _, _ = s.applyPut(ctx, "m", key, []byte("v"), 0, true) }()
			go func() { defer wg.Done(); racer(s) }()
			wg.Wait()
			lv, lok := s.store.get(p, "m", key)
			_ = s.CloseWAL()

			s2 := svcWith(&fakeTransport{})
			if err := s2.OpenWAL(path); err != nil {
				t.Fatalf("%s reopen: %v", label, err)
			}
			rv, rok := s2.store.get(p, "m", key)
			_ = s2.CloseWAL()
			if lok != rok || string(lv) != string(rv) {
				t.Fatalf("%s round %d: live=(%q,%v) replay=(%q,%v): WAL order diverged from store order", label, r, lv, lok, rv, rok)
			}
		}
	}
	run("putremove", 200, func(s *Service) { _, _ = s.applyRemove(ctx, "m", key, true) })
	run("putclear", 100, func(s *Service) { _, _ = s.localClearMap("m") })
}

// TestSampleOwnedSkipsUnowned verifies eviction sampling only considers owned
// partitions — a backup copy must never be picked (anti-entropy would re-push it).
func TestSampleOwnedSkipsUnowned(t *testing.T) {
	st := newStore()
	st.put(0, "m", []byte("a"), []byte("v"), 0)
	st.put(1, "m", []byte("b"), []byte("v"), 0)
	got := st.sampleOwned(func(p int) bool { return p == 0 }, 10)
	if len(got) != 1 || got[0].key != "a" {
		t.Fatalf("sampleOwned(owned=={0}) = %+v, want only key a", got)
	}
}

// TestEvictEnforcesCap covers the max-size eviction: it drains the store toward
// the cap, is bounded per call by the batch, is a no-op under the cap, and is
// disabled when max <= 0.
func TestEvictEnforcesCap(t *testing.T) {
	s := svcWith(&fakeTransport{}) // single node owns all partitions, no WAL/replication
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		s.store.put(partition.For([]byte{byte(i)}), "m", []byte{byte(i)}, []byte("v"), 0)
	}
	if got := s.store.entryCount(); got != 50 {
		t.Fatalf("seed count = %d, want 50", got)
	}
	if ev := s.Evict(ctx, 30, 1000); ev != 20 || s.store.entryCount() != 30 {
		t.Fatalf("evict to 30: removed %d, count %d; want 20, 30", ev, s.store.entryCount())
	}
	if ev := s.Evict(ctx, 30, 1000); ev != 0 {
		t.Fatalf("evict under cap removed %d, want 0", ev)
	}
	if ev := s.Evict(ctx, 0, 1000); ev != 0 {
		t.Fatalf("evict with max=0 (disabled) removed %d, want 0", ev)
	}
	// batch caps a single call: 30 entries, cap 10 → excess 20, batch 5 → remove 5.
	if ev := s.Evict(ctx, 10, 5); ev != 5 || s.store.entryCount() != 25 {
		t.Fatalf("batch-capped evict: removed %d, count %d; want 5, 25", ev, s.store.entryCount())
	}
}

// TestMigrateDropWALAtomicUnderConcurrentPut is the regression guard for the
// hand-off WAL-ordering bug found by the deep review: the drop mutated the store
// under the shard lock and recorded the removal in a separate WAL critical
// section, so a put for a just-dropped key could slip its [PUT] into the WAL
// between the drop and the [REMOVE]. Replay then applied [PUT][REMOVE] and lost
// the acknowledged write while the live store kept it. dropMigratedLogged now
// holds the WAL lock across both, so the WAL order can never diverge from the
// store order — replay must always reconstruct the live state.
func TestMigrateDropWALAtomicUnderConcurrentPut(t *testing.T) {
	dir := t.TempDir()
	key := []byte("k")
	p := partition.For(key)
	ctx := context.Background()

	for r := 0; r < 300; r++ {
		path := filepath.Join(dir, "mig"+strconv.Itoa(r)+".log")
		s := svcWith(&fakeTransport{})
		if err := s.OpenWAL(path); err != nil {
			t.Fatalf("open: %v", err)
		}
		// Seed the key, then snapshot the partition — this is what a hand-off drops.
		if _, err := s.applyPut(ctx, "m", key, []byte("v0"), 0, true); err != nil {
			t.Fatalf("seed: %v", err)
		}
		entries := s.store.snapshotPartition(p)

		var wg sync.WaitGroup
		wg.Add(2)
		// A put for the same key, racing the hand-off drop + its WAL record.
		go func() { defer wg.Done(); _, _ = s.applyPut(ctx, "m", key, []byte("v1"), 0, true) }()
		go func() { defer wg.Done(); s.dropMigratedLogged(p, entries) }()
		wg.Wait()

		lv, lok := s.store.get(p, "m", key)
		_ = s.CloseWAL()

		s2 := svcWith(&fakeTransport{})
		if err := s2.OpenWAL(path); err != nil {
			t.Fatalf("reopen: %v", err)
		}
		rv, rok := s2.store.get(p, "m", key)
		_ = s2.CloseWAL()
		if lok != rok || string(lv) != string(rv) {
			t.Fatalf("round %d: live=(%q,%v) replay=(%q,%v): hand-off WAL order diverged from store order", r, lv, lok, rv, rok)
		}
	}
}

// TestMigrateReportsCompletionAndHonorsContext proves Migrate is time-boxable:
// it returns false on a cancelled context (an incomplete pass, so the maintenance
// loop leaves the table version unadvanced and retries) and true on a full pass.
// This is what lets the loop bound a rebalance to one interval without freezing
// gossip and failure detection on a slow or unreachable peer.
func TestMigrateReportsCompletionAndHonorsContext(t *testing.T) {
	s := svcWith(&fakeTransport{})
	s.store.put(partition.For([]byte("k")), "m", []byte("k"), []byte("v"), 0)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if s.Migrate(cancelled, partition.NewTable([]string{"self"}, 1)) {
		t.Fatal("Migrate on a cancelled context must report an incomplete pass (false)")
	}
	if !s.Migrate(context.Background(), partition.NewTable([]string{"self"}, 1)) {
		t.Fatal("Migrate over a fully-owned table must report a complete pass (true)")
	}
}

func TestHandleRejectsCorruptPayload(t *testing.T) {
	svc := svcWith(&fakeTransport{})
	bad := []byte{0xff, 0xff, 0xff} // not a valid protobuf message

	for _, mt := range []medusav1.MessageType{
		medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST,
		medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST,
		medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST,
	} {
		if _, _, err := svc.Handle(mt, bad, nil); err == nil {
			t.Errorf("Handle(%v, corrupt) err = nil, want decode error", mt)
		}
	}
}
