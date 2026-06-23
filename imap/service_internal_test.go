package imap

import (
	"context"
	"errors"
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
