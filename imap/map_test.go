package imap_test

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/medusa/cluster"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/imap"
	"github.com/lodgvideon/medusa/partition"
	"github.com/lodgvideon/medusa/transport"
)

func be64(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

// node bundles the layers a single cluster member needs.
type node struct {
	id  string
	mem *cluster.Membership
	svc *imap.Service
	tr  transport.Transport
}

// dispatch routes cluster control messages to membership and map ops to the
// map service — the same split the top-level Node makes.
func dispatch(mem *cluster.Membership, svc *imap.Service) transport.Handler {
	return func(rt medusav1.MessageType, req, rb []byte) (medusav1.MessageType, []byte, error) {
		switch rt {
		case medusav1.MessageType_MESSAGE_TYPE_JOIN_REQUEST,
			medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST,
			medusav1.MessageType_MESSAGE_TYPE_HEARTBEAT:
			return mem.Handle(rt, req, rb)
		default:
			return svc.Handle(rt, req, rb)
		}
	}
}

type fixture struct {
	sw    *transport.Switch
	nodes map[string]*node
}

func newFixture() *fixture {
	return &fixture{sw: transport.NewSwitch(), nodes: map[string]*node{}}
}

func (f *fixture) add(t *testing.T, id string) *node {
	t.Helper()
	tr := f.sw.NewTransport(id)
	mem := cluster.New(cluster.Member{ID: id, Addr: id}, tr, 1)
	svc := imap.NewService(mem, tr)
	if err := tr.Listen(dispatch(mem, svc)); err != nil {
		t.Fatalf("Listen(%s): %v", id, err)
	}
	n := &node{id: id, mem: mem, svc: svc, tr: tr}
	f.nodes[id] = n
	t.Cleanup(func() { _ = tr.Close() })
	return n
}

// cluster3 builds a converged 3-node cluster a/b/c.
func (f *fixture) cluster3(t *testing.T) (a, b, c *node) {
	t.Helper()
	a = f.add(t, "a")
	b = f.add(t, "b")
	c = f.add(t, "c")
	if err := b.mem.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("b.Join: %v", err)
	}
	if err := c.mem.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("c.Join: %v", err)
	}
	return a, b, c
}

func ownerBackup(n *node, key []byte) (owner, backup string) {
	tbl := n.mem.Table()
	p := partition.For(key)
	owner = tbl.OwnerOf(p)
	backup, _ = tbl.BackupOf(p)
	return owner, backup
}

func TestPutGetAcrossNodes(t *testing.T) {
	f := newFixture()
	a, b, c := f.cluster3(t)
	ctx := context.Background()

	// Write through node a, read through b and c — every key routes to its owner.
	for i := 0; i < 200; i++ {
		key := []byte{byte(i), byte(i >> 8)}
		val := []byte{byte(i * 7)}
		if err := a.svc.Map("data").Put(ctx, key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	for _, reader := range []*node{b, c} {
		for i := 0; i < 200; i++ {
			key := []byte{byte(i), byte(i >> 8)}
			want := byte(i * 7)
			got, ok, err := reader.svc.Map("data").Get(ctx, key)
			if err != nil {
				t.Fatalf("node %s get %d: %v", reader.id, i, err)
			}
			if !ok || len(got) != 1 || got[0] != want {
				t.Fatalf("node %s get %d = %v,%v want [%d],true", reader.id, i, got, ok, want)
			}
		}
	}
}

func TestGetMissingKey(t *testing.T) {
	f := newFixture()
	a, b, _ := f.cluster3(t)
	ctx := context.Background()

	_, ok, err := b.svc.Map("data").Get(ctx, []byte("never-written"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Error("found = true for a missing key")
	}
	_ = a
}

func TestRemoveAcrossNodes(t *testing.T) {
	f := newFixture()
	a, b, _ := f.cluster3(t)
	ctx := context.Background()
	key := []byte("removable")

	if err := a.svc.Map("data").Put(ctx, key, []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	existed, err := b.svc.Map("data").Remove(ctx, key)
	if err != nil || !existed {
		t.Fatalf("remove = %v,%v want true,nil", existed, err)
	}
	_, ok, err := a.svc.Map("data").Get(ctx, key)
	if err != nil {
		t.Fatalf("get after remove: %v", err)
	}
	if ok {
		t.Error("key still present after remove")
	}
}

func TestReadFromBackupAfterOwnerCrash(t *testing.T) {
	f := newFixture()
	a, b, c := f.cluster3(t)
	ctx := context.Background()
	byID := map[string]*node{"a": a, "b": b, "c": c}

	// Find a key whose owner and backup are two specific live nodes, and pick a
	// third node to read from (so the read exercises the remote backup path).
	var key []byte
	var ownerID, backupID string
	for i := 0; i < 100000; i++ {
		k := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		o, bk := ownerBackup(a, k)
		if o != "" && bk != "" && o != bk {
			key, ownerID, backupID = k, o, bk
			break
		}
	}
	if key == nil {
		t.Fatal("could not find a key with distinct owner and backup")
	}

	if err := a.svc.Map("data").Put(ctx, key, []byte("survives")); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Crash the owner. Its transport leaves the switch, so routing to it fails.
	_ = byID[ownerID].tr.Close()

	// Read from the backup node itself: owner is unreachable, backup is local.
	got, ok, err := byID[backupID].svc.Map("data").Get(ctx, key)
	if err != nil || !ok || string(got) != "survives" {
		t.Fatalf("backup-local read = %q,%v,%v want survives,true,nil", got, ok, err)
	}

	// Read from the third node: owner unreachable -> remote read from backup.
	var third *node
	for id, n := range byID {
		if id != ownerID && id != backupID {
			third = n
			break
		}
	}
	got, ok, err = third.svc.Map("data").Get(ctx, key)
	if err != nil || !ok || string(got) != "survives" {
		t.Fatalf("backup-remote read from %s = %q,%v,%v want survives,true,nil", third.id, got, ok, err)
	}
}

func BenchmarkMapGetLocal(b *testing.B) {
	sw := transport.NewSwitch()
	tr := sw.NewTransport("a")
	mem := cluster.New(cluster.Member{ID: "a", Addr: "a"}, tr, 1)
	svc := imap.NewService(mem, tr)
	_ = tr.Listen(dispatch(mem, svc))
	defer tr.Close()

	ctx := context.Background()
	m := svc.Map("data")
	key := []byte("hot-key")
	_ = m.Put(ctx, key, []byte("a-cache-value-payload"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = m.Get(ctx, key)
	}
}

// findDistinctKey returns a key whose owner and backup are two different nodes.
func findDistinctKey(n *node) (key []byte, ownerID, backupID string) {
	for i := 0; i < 100000; i++ {
		k := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		o, bk := ownerBackup(n, k)
		if o != "" && bk != "" && o != bk {
			return k, o, bk
		}
	}
	return nil, "", ""
}

// pickWriter returns a live node that is not the owner, preferring one that is
// not the backup either (to exercise the remote-backup path).
func pickWriter(byID map[string]*node, ownerID, backupID string) *node {
	var w *node
	for id, n := range byID {
		if id == ownerID {
			continue
		}
		w = n
		if id != backupID {
			return w
		}
	}
	return w
}

func TestPutFailsOverToBackupAfterOwnerCrash(t *testing.T) {
	f := newFixture()
	a, _, _ := f.cluster3(t)
	ctx := context.Background()

	key, ownerID, backupID := findDistinctKey(a)
	if key == nil {
		t.Fatal("could not find a key with distinct owner and backup")
	}
	_ = f.nodes[ownerID].tr.Close() // crash the owner

	writer := pickWriter(f.nodes, ownerID, backupID)
	if err := writer.svc.Map("data").Put(ctx, key, []byte("via-backup")); err != nil {
		t.Fatalf("put after owner crash: %v", err)
	}

	got, ok, err := f.nodes[backupID].svc.Map("data").Get(ctx, key)
	if err != nil || !ok || string(got) != "via-backup" {
		t.Fatalf("read after failover put = %q,%v,%v want via-backup,true,nil", got, ok, err)
	}
}

func TestRemoveFailsOverToBackupAfterOwnerCrash(t *testing.T) {
	f := newFixture()
	a, _, _ := f.cluster3(t)
	ctx := context.Background()

	key, ownerID, backupID := findDistinctKey(a)
	if key == nil {
		t.Fatal("could not find a key with distinct owner and backup")
	}
	if err := a.svc.Map("data").Put(ctx, key, []byte("doomed")); err != nil {
		t.Fatalf("put: %v", err)
	}
	_ = f.nodes[ownerID].tr.Close() // crash the owner

	remover := pickWriter(f.nodes, ownerID, backupID)
	if _, err := remover.svc.Map("data").Remove(ctx, key); err != nil {
		t.Fatalf("remove after owner crash: %v", err)
	}

	_, ok, err := f.nodes[backupID].svc.Map("data").Get(ctx, key)
	if err != nil {
		t.Fatalf("get after failover remove: %v", err)
	}
	if ok {
		t.Error("key still present on backup after failover remove")
	}
}

func TestPutFailsOverToLocalBackup(t *testing.T) {
	f := newFixture()
	a, _, _ := f.cluster3(t)
	ctx := context.Background()

	key, ownerID, backupID := findDistinctKey(a)
	if key == nil {
		t.Fatal("could not find a key with distinct owner and backup")
	}
	_ = f.nodes[ownerID].tr.Close() // crash the owner

	// Write through the backup node itself: owner unreachable, backup is local.
	if err := f.nodes[backupID].svc.Map("data").Put(ctx, key, []byte("local")); err != nil {
		t.Fatalf("put via backup: %v", err)
	}
	got, ok, err := f.nodes[backupID].svc.Map("data").Get(ctx, key)
	if err != nil || !ok || string(got) != "local" {
		t.Fatalf("read = %q,%v,%v want local,true,nil", got, ok, err)
	}
}

func TestRemoveFailsOverToLocalBackup(t *testing.T) {
	f := newFixture()
	a, _, _ := f.cluster3(t)
	ctx := context.Background()

	key, ownerID, backupID := findDistinctKey(a)
	if key == nil {
		t.Fatal("could not find a key with distinct owner and backup")
	}
	if err := a.svc.Map("data").Put(ctx, key, []byte("doomed")); err != nil {
		t.Fatalf("put: %v", err)
	}
	_ = f.nodes[ownerID].tr.Close() // crash the owner

	// Remove through the backup node itself: owner unreachable, backup is local.
	if _, err := f.nodes[backupID].svc.Map("data").Remove(ctx, key); err != nil {
		t.Fatalf("remove via backup: %v", err)
	}
	if _, ok, _ := f.nodes[backupID].svc.Map("data").Get(ctx, key); ok {
		t.Error("key still present after local-backup remove")
	}
}

func TestPutTTLExpires(t *testing.T) {
	f := newFixture()
	a := f.add(t, "a") // solo node owns every partition
	ctx := context.Background()
	m := a.svc.Map("t")

	if err := m.PutTTL(ctx, []byte("k"), []byte("v"), 80*time.Millisecond); err != nil {
		t.Fatalf("PutTTL: %v", err)
	}
	if v, ok, err := m.Get(ctx, []byte("k")); err != nil || !ok || string(v) != "v" {
		t.Fatalf("before expiry get = %q,%v,%v", v, ok, err)
	}

	time.Sleep(140 * time.Millisecond)
	if _, ok, err := m.Get(ctx, []byte("k")); err != nil || ok {
		t.Fatalf("after expiry get ok = %v, want false", ok)
	}
}

func TestDistributedAtomicCounter(t *testing.T) {
	f := newFixture()
	a, b, c := f.cluster3(t)
	ctx := context.Background()
	key := []byte("counter")

	// Hammer Execute(incr, +1) concurrently from all three nodes. Because the
	// processor runs atomically on the key's owner, every increment counts.
	const perGoroutine = 50
	nodes := []*node{a, b, c}
	var wg sync.WaitGroup
	for _, n := range nodes {
		for g := 0; g < 5; g++ {
			wg.Add(1)
			go func(nd *node) {
				defer wg.Done()
				for i := 0; i < perGoroutine; i++ {
					if _, err := nd.svc.Map("m").Execute(ctx, key, "incr", be64(1)); err != nil {
						t.Errorf("execute: %v", err)
						return
					}
				}
			}(n)
		}
	}
	wg.Wait()

	want := int64(len(nodes) * 5 * perGoroutine) // 750
	v, ok, err := a.svc.Map("m").Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("get counter: %v,%v", ok, err)
	}
	if got := int64(binary.BigEndian.Uint64(v)); got != want {
		t.Fatalf("counter = %d, want %d (lost updates)", got, want)
	}
}

func TestExecuteUnknownProcessor(t *testing.T) {
	f := newFixture()
	a := f.add(t, "a")
	if _, err := a.svc.Map("m").Execute(context.Background(), []byte("k"), "nope", nil); err == nil {
		t.Fatal("expected error for unknown processor")
	}
}

func TestServiceHandleUnknownType(t *testing.T) {
	f := newFixture()
	a := f.add(t, "a")
	if _, _, err := a.svc.Handle(medusav1.MessageType_MESSAGE_TYPE_JOIN_REQUEST, nil, nil); err == nil {
		t.Fatal("expected error for a non-map message type")
	}
}

func TestLocalGetZeroAlloc(t *testing.T) {
	f := newFixture()
	a := f.add(t, "a") // solo node owns every partition, so all keys are local
	ctx := context.Background()
	m := a.svc.Map("data")

	key := []byte("hot-key")
	if err := m.Put(ctx, key, []byte("a-cache-value-of-some-size")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, _, err := m.Get(ctx, key); err != nil { // warm
		t.Fatalf("get: %v", err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		_, _, _ = m.Get(ctx, key)
	})
	if allocs != 0 {
		t.Fatalf("local Get allocated %v allocs/op, want 0", allocs)
	}
}
