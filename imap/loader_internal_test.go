package imap

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/lodgvideon/medusa/cluster"
	"github.com/lodgvideon/medusa/codec"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/partition"
)

// fakeBackingStore is an in-memory MapStore that counts its calls, for asserting
// read/write/delete-through behaviour.
type fakeBackingStore struct {
	mu                     sync.Mutex
	data                   map[string][]byte
	loads, stores, deletes int
	storeErr               error
}

func (f *fakeBackingStore) Load(key []byte) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	v, ok := f.data[string(key)]
	return v, ok, nil
}

func (f *fakeBackingStore) Store(key, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stores++
	if f.storeErr != nil {
		return f.storeErr
	}
	f.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (f *fakeBackingStore) Delete(key []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes++
	delete(f.data, string(key))
	return nil
}

func (f *fakeBackingStore) count() (l, s, d int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loads, f.stores, f.deletes
}

func TestReadThroughLoadsAndCaches(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	backing := &fakeBackingStore{data: map[string][]byte{"k": []byte("fromdb")}}
	s.SetMapStore("m", backing)
	ctx := context.Background()

	v, ok, err := s.Map("m").Get(ctx, []byte("k"))
	if err != nil || !ok || string(v) != "fromdb" {
		t.Fatalf("read-through Get = %q,%v,%v; want fromdb,true,nil", v, ok, err)
	}
	if l, _, _ := backing.count(); l != 1 {
		t.Fatalf("loads = %d, want 1", l)
	}
	// Second Get hits the cache — no second load.
	if _, _, err := s.Map("m").Get(ctx, []byte("k")); err != nil {
		t.Fatal(err)
	}
	if l, _, _ := backing.count(); l != 1 {
		t.Fatalf("loads after a cache hit = %d, want 1", l)
	}
}

func TestWriteAndDeleteThrough(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	backing := &fakeBackingStore{data: map[string][]byte{}}
	s.SetMapStore("m", backing)
	ctx := context.Background()

	if err := s.Map("m").Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, st, _ := backing.count(); st != 1 || string(backing.data["k"]) != "v" {
		t.Fatalf("write-through: stores=%d data=%q", st, backing.data["k"])
	}
	if _, err := s.Map("m").Remove(ctx, []byte("k")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, _, d := backing.count(); d != 1 {
		t.Fatalf("delete-through: deletes=%d, want 1", d)
	}
	if _, ok := backing.data["k"]; ok {
		t.Fatal("delete-through did not remove from the backing store")
	}
}

func TestEvictDropsCacheNotBackingStore(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	backing := &fakeBackingStore{data: map[string][]byte{}}
	s.SetMapStore("m", backing)
	ctx := context.Background()

	if err := s.Map("m").Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := s.Map("m").Evict(ctx, []byte("k")); err != nil {
		t.Fatalf("evict: %v", err)
	}
	if _, _, d := backing.count(); d != 0 {
		t.Fatalf("evict must NOT delete through, deletes=%d", d)
	}
	if _, ok := backing.data["k"]; !ok {
		t.Fatal("evict wrongly removed the entry from the backing store")
	}
	// The next Get reloads from the backing store (read-through).
	lBefore, _, _ := backing.count()
	v, ok, err := s.Map("m").Get(ctx, []byte("k"))
	if err != nil || !ok || string(v) != "v" {
		t.Fatalf("post-evict reload = %q,%v,%v; want v,true,nil", v, ok, err)
	}
	if l, _, _ := backing.count(); l != lBefore+1 {
		t.Fatalf("post-evict Get must reload through the loader, loads delta = %d", l-lBefore)
	}
}

func TestWriteThroughErrorSurfaces(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	boom := errors.New("backing store down")
	s.SetMapStore("m", &fakeBackingStore{data: map[string][]byte{}, storeErr: boom})

	if err := s.Map("m").Put(context.Background(), []byte("k"), []byte("v")); !errors.Is(err, boom) {
		t.Fatalf("write-through error = %v, want boom", err)
	}
	// The in-memory write is kept (write-through failure does not roll it back).
	if v, ok := s.store.get(partition.For([]byte("k")), "m", []byte("k")); !ok || string(v) != "v" {
		t.Fatalf("in-memory value after write-through failure = %q,%v; want kept", v, ok)
	}
}

// readOnlyLoader is a MapLoader that is NOT a MapStore.
type readOnlyLoader struct{ val []byte }

func (r readOnlyLoader) Load([]byte) ([]byte, bool, error) { return r.val, true, nil }

// TestReadThroughOnlyFiresOnOwner is the regression guard for the review finding
// that read-through ran on a non-owner backup (the Get backup-fallback reaches the
// GET handler, which calls getThrough). A node that does not own the partition
// must NOT load — it would orphan a cache entry the owner never learns about.
func TestReadThroughOnlyFiresOnOwner(t *testing.T) {
	tr := &fakeTransport{}
	mem := cluster.New(cluster.Member{ID: "self", Addr: "self"}, tr, 1)
	// Admit a second member so it owns roughly half the partitions, not self.
	mlb, err := codec.Marshal(nil, &medusav1.MemberList{Members: []*medusav1.Member{{Id: "other", Addr: "other"}}})
	if err != nil {
		t.Fatalf("marshal member list: %v", err)
	}
	if _, _, err := mem.Handle(medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST, mlb, nil); err != nil {
		t.Fatalf("merge member: %v", err)
	}
	svc := NewService(mem, tr)
	loader := &fakeBackingStore{data: map[string][]byte{}}
	svc.SetMapLoader("m", loader)

	tbl := mem.Table()
	owned, foreign := keyOwnedBy(tbl, "self"), keyOwnedBy(tbl, "other")
	if owned == nil || foreign == nil {
		t.Fatal("setup: need a key owned by self and one owned by other")
	}

	// A non-owned key must not trigger a load.
	if _, _, err := svc.getThrough("m", foreign); err != nil {
		t.Fatalf("getThrough(foreign): %v", err)
	}
	if l, _, _ := loader.count(); l != 0 {
		t.Fatalf("read-through fired on a non-owner: loads=%d, want 0", l)
	}
	// An owned key does trigger a load.
	if _, _, err := svc.getThrough("m", owned); err != nil {
		t.Fatalf("getThrough(owned): %v", err)
	}
	if l, _, _ := loader.count(); l != 1 {
		t.Fatalf("read-through must fire on the owner: loads=%d, want 1", l)
	}
}

func keyOwnedBy(tbl *partition.Table, id string) []byte {
	for i := 0; i < 4000; i++ {
		k := []byte{byte(i), byte(i >> 8)}
		if tbl.OwnerOf(partition.For(k)) == id {
			return k
		}
	}
	return nil
}

func TestReadOnlyLoaderSkipsWriteThrough(t *testing.T) {
	s := svcWith(&fakeTransport{})
	defer s.Close()
	s.SetMapLoader("m", readOnlyLoader{val: []byte("ro")})
	ctx := context.Background()

	if v, ok, _ := s.Map("m").Get(ctx, []byte("k")); !ok || string(v) != "ro" {
		t.Fatalf("read-only load = %q,%v; want ro,true", v, ok)
	}
	// A plain MapLoader is not a MapStore, so Put must not attempt (or error on)
	// write-through.
	if err := s.Map("m").Put(ctx, []byte("k2"), []byte("v")); err != nil {
		t.Fatalf("put with a read-only loader: %v", err)
	}
}
