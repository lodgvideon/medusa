package medusa_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/lodgvideon/medusa"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/transport"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil))) // quiet node logs
	os.Exit(m.Run())
}

// freeAddr returns a currently-unused loopback address. There is a small
// time-of-check window, but it is reliable enough for local tests.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// startCluster boots n TCP nodes and joins them all through the first.
func startCluster(t *testing.T, n int) []*medusa.Node {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = freeAddr(t)
	}
	nodes := make([]*medusa.Node, n)
	for i, addr := range addrs {
		node, err := medusa.New(medusa.Config{ID: "n" + strconv.Itoa(i), Addr: addr})
		if err != nil {
			t.Fatalf("New(%s): %v", addr, err)
		}
		nodes[i] = node
		t.Cleanup(func() { _ = node.Close() })
	}
	ctx := context.Background()
	for i := 1; i < n; i++ {
		if err := nodes[i].Join(ctx, []string{addrs[0]}); err != nil {
			t.Fatalf("n%d.Join: %v", i, err)
		}
	}
	return nodes
}

// inmemNode builds a node on a shared in-memory switch with a fast maintenance
// tick. The address equals the id.
func inmemNode(t *testing.T, sw *transport.Switch, id string, seeds []string) *medusa.Node {
	t.Helper()
	n, err := medusa.New(medusa.Config{
		ID: id, Addr: id, Transport: sw.NewTransport(id), Seeds: seeds,
		MaintenanceInterval: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new %s: %v", id, err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func awaitMembers(t *testing.T, want int, nodes ...*medusa.Node) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range nodes {
			if len(n.Members()) != want {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cluster did not reach %d members", want)
}

func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// First incarnation: write keys, then Close (which persists a snapshot).
	sw1 := transport.NewSwitch()
	n1, err := medusa.New(medusa.Config{
		ID: "a", Addr: "a", Transport: sw1.NewTransport("a"),
		DataDir: dir, MaintenanceInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("new n1: %v", err)
	}
	const N = 50
	for i := 0; i < N; i++ {
		if err := n1.Map("m").Put(ctx, []byte{byte(i)}, []byte{byte(i * 2)}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := n1.Close(); err != nil {
		t.Fatalf("close n1: %v", err)
	}

	// Second incarnation on the same DataDir must reload every key.
	sw2 := transport.NewSwitch()
	n2, err := medusa.New(medusa.Config{
		ID: "a", Addr: "a", Transport: sw2.NewTransport("a"),
		DataDir: dir, MaintenanceInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("new n2: %v", err)
	}
	t.Cleanup(func() { _ = n2.Close() })

	if got := n2.LocalEntryCount(); got != N {
		t.Fatalf("reloaded %d entries, want %d", got, N)
	}
	for i := 0; i < N; i++ {
		v, ok, err := n2.Map("m").Get(ctx, []byte{byte(i)})
		if err != nil || !ok || len(v) != 1 || v[0] != byte(i*2) {
			t.Fatalf("reloaded key %d = %v,%v,%v", i, v, ok, err)
		}
	}
}

func TestFailureDetectionEvictsCrashedNode(t *testing.T) {
	sw := transport.NewSwitch()
	ctx := context.Background()
	a := inmemNode(t, sw, "a", nil)
	b := inmemNode(t, sw, "b", []string{"a"})
	c := inmemNode(t, sw, "c", []string{"a"})
	awaitMembers(t, 3, a, b, c)

	const N = 120
	for i := 0; i < N; i++ {
		key := []byte{byte(i), byte(i >> 8)}
		if err := a.Map("m").Put(ctx, key, []byte{byte(i)}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// c crashes ungracefully (no leave announcement). The maintenance loop's
	// failure detector must evict it after the miss threshold.
	_ = c.Close()
	awaitMembers(t, 2, a, b)

	// Every key remains readable from the survivors (served from backups and
	// redistributed by the eviction-triggered migration).
	for i := 0; i < N; i++ {
		key := []byte{byte(i), byte(i >> 8)}
		v, ok, err := b.Map("m").Get(ctx, key)
		if err != nil || !ok || len(v) != 1 || v[0] != byte(i) {
			t.Fatalf("key %d after crash eviction: v=%v ok=%v err=%v", i, v, ok, err)
		}
	}
}

func TestGracefulLeavePreservesData(t *testing.T) {
	sw := transport.NewSwitch()
	ctx := context.Background()
	a := inmemNode(t, sw, "a", nil)
	b := inmemNode(t, sw, "b", []string{"a"})
	c := inmemNode(t, sw, "c", []string{"a"})
	awaitMembers(t, 3, a, b, c)

	const N = 150
	for i := 0; i < N; i++ {
		key := []byte{byte(i), byte(i >> 8)}
		if err := a.Map("m").Put(ctx, key, []byte{byte(i)}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// a leaves gracefully: it hands off its partitions and announces departure.
	if err := a.Leave(ctx); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	awaitMembers(t, 2, b, c)

	// Every key must still be readable from the survivors — nothing was stranded
	// on the departed node.
	for i := 0; i < N; i++ {
		key := []byte{byte(i), byte(i >> 8)}
		v, ok, err := b.Map("m").Get(ctx, key)
		if err != nil || !ok || len(v) != 1 || v[0] != byte(i) {
			t.Fatalf("key %d after graceful leave: v=%v ok=%v err=%v", i, v, ok, err)
		}
	}
}

func TestConfigurableBackupsToleratesTwoFailures(t *testing.T) {
	sw := transport.NewSwitch()
	ctx := context.Background()
	tick := 40 * time.Millisecond
	mk := func(id string, seeds []string) *medusa.Node {
		n, err := medusa.New(medusa.Config{
			ID: id, Addr: id, Transport: sw.NewTransport(id), Seeds: seeds,
			MaintenanceInterval: tick, Backups: 2,
		})
		if err != nil {
			t.Fatalf("new %s: %v", id, err)
		}
		t.Cleanup(func() { _ = n.Close() })
		return n
	}

	// Four nodes, two backups: every partition has three distinct holders
	// (owner + 2 backups), so any two-node loss still leaves one holder alive.
	a := mk("a", nil)
	b := mk("b", []string{"a"})
	c := mk("c", []string{"a"})
	d := mk("d", []string{"a"})
	awaitMembers(t, 4, a, b, c, d)

	if got := a.BackupCount(); got != 2 {
		t.Fatalf("BackupCount = %d, want 2", got)
	}

	const N = 200
	for i := 0; i < N; i++ {
		key := []byte{byte(i), byte(i >> 8)}
		if err := a.Map("m").Put(ctx, key, []byte{byte(i)}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Two nodes crash at once. A single-backup cluster would lose any key whose
	// owner and sole backup were the two casualties; two backups must not.
	_ = c.Close()
	_ = d.Close()
	awaitMembers(t, 2, a, b)

	// Every key remains readable from the survivors once the eviction-triggered
	// rebalance has moved data into place.
	deadline := time.Now().Add(4 * time.Second)
	for i := 0; i < N; i++ {
		key := []byte{byte(i), byte(i >> 8)}
		var (
			v   []byte
			ok  bool
			err error
		)
		for time.Now().Before(deadline) {
			if v, ok, err = b.Map("m").Get(ctx, key); err == nil && ok {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil || !ok || len(v) != 1 || v[0] != byte(i) {
			t.Fatalf("key %d after two-node loss: v=%v ok=%v err=%v", i, v, ok, err)
		}
	}
}

func TestMaintenanceLoopConvergesWithoutExplicitJoin(t *testing.T) {
	sw := transport.NewSwitch()
	tick := 50 * time.Millisecond

	a, err := medusa.New(medusa.Config{
		ID: "a", Addr: "a", Transport: sw.NewTransport("a"), MaintenanceInterval: tick,
	})
	if err != nil {
		t.Fatalf("new a: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	// b only knows the seed; its maintenance loop must join and converge on its own.
	b, err := medusa.New(medusa.Config{
		ID: "b", Addr: "b", Seeds: []string{"a"}, Transport: sw.NewTransport("b"), MaintenanceInterval: tick,
	})
	if err != nil {
		t.Fatalf("new b: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(a.Members()) == 2 && len(b.Members()) == 2 {
			return // converged
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("did not converge: a sees %d members, b sees %d", len(a.Members()), len(b.Members()))
}

func TestPartitionMigrationOnJoin(t *testing.T) {
	sw := transport.NewSwitch()
	tick := 40 * time.Millisecond
	mk := func(id string, seeds []string) *medusa.Node {
		n, err := medusa.New(medusa.Config{
			ID: id, Addr: id, Transport: sw.NewTransport(id), Seeds: seeds, MaintenanceInterval: tick,
		})
		if err != nil {
			t.Fatalf("new %s: %v", id, err)
		}
		t.Cleanup(func() { _ = n.Close() })
		return n
	}
	waitMembers := func(want int, nodes ...*medusa.Node) {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			ok := true
			for _, n := range nodes {
				if len(n.Members()) != want {
					ok = false
				}
			}
			if ok {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("cluster did not reach %d members", want)
	}

	ctx := context.Background()
	a := mk("a", nil)
	b := mk("b", []string{"a"})
	waitMembers(2, a, b)

	const N = 200
	for i := 0; i < N; i++ {
		key := []byte{byte(i), byte(i >> 8)}
		if err := a.Map("m").Put(ctx, key, []byte{byte(i)}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// A third node joins — its share of the partitions must migrate to it.
	c := mk("c", []string{"a"})
	waitMembers(3, a, b, c)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && c.LocalEntryCount() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if c.LocalEntryCount() == 0 {
		t.Fatal("no data migrated to the newly joined node")
	}

	// Every key must still be readable from the new node after migration.
	for i := 0; i < N; i++ {
		key := []byte{byte(i), byte(i >> 8)}
		v, ok, err := c.Map("m").Get(ctx, key)
		if err != nil || !ok || len(v) != 1 || v[0] != byte(i) {
			t.Fatalf("key %d after migration: v=%v ok=%v err=%v", i, v, ok, err)
		}
	}
}

func TestBindAdvertiseSplitConverges(t *testing.T) {
	// Bind on 127.0.0.1 but advertise via "localhost" (which also resolves to
	// 127.0.0.1) to exercise a bind address distinct from the advertised one.
	mk := func(id string, seeds []string) *medusa.Node {
		bind := freeAddr(t) // 127.0.0.1:PORT
		_, port, err := net.SplitHostPort(bind)
		if err != nil {
			t.Fatalf("split %q: %v", bind, err)
		}
		advertise := "localhost:" + port
		n, err := medusa.New(medusa.Config{
			ID: id, Addr: advertise, BindAddr: bind, Seeds: seeds,
			MaintenanceInterval: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("new %s: %v", id, err)
		}
		t.Cleanup(func() { _ = n.Close() })
		return n
	}

	a := mk("a", nil)
	b := mk("b", []string{a.Addr()}) // a.Addr() is the bind addr; advertise also routes here

	// b joins via a's address; both must converge to 2 members.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if len(a.Members()) == 2 && len(b.Members()) == 2 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("did not converge: a sees %d, b sees %d", len(a.Members()), len(b.Members()))
}

func TestClusterFormationOverTCP(t *testing.T) {
	nodes := startCluster(t, 3)
	for i, n := range nodes {
		if got := len(n.Members()); got != 3 {
			t.Fatalf("node %d sees %d members, want 3", i, got)
		}
	}
}

func TestDistributedMapOverTCP(t *testing.T) {
	nodes := startCluster(t, 3)
	ctx := context.Background()
	const n = 500

	// Write everything through node 0.
	for i := 0; i < n; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		val := []byte("val-" + strconv.Itoa(i))
		if err := nodes[0].Map("grid").Put(ctx, key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Read everything back through nodes 1 and 2 — keys route to their owners.
	for ni := 1; ni < 3; ni++ {
		for i := 0; i < n; i++ {
			key := []byte("key-" + strconv.Itoa(i))
			want := "val-" + strconv.Itoa(i)
			got, ok, err := nodes[ni].Map("grid").Get(ctx, key)
			if err != nil || !ok || string(got) != want {
				t.Fatalf("node %d get %d = %q,%v,%v want %q", ni, i, got, ok, err, want)
			}
		}
	}
}

func TestNodeAddrAndCheckLiveness(t *testing.T) {
	nodes := startCluster(t, 3)
	if nodes[0].Addr() == "" {
		t.Error("Addr() returned empty string")
	}

	if err := nodes[2].Close(); err != nil { // crash a peer
		t.Fatalf("close: %v", err)
	}
	evicted := nodes[0].CheckLiveness(context.Background())
	if len(evicted) == 0 {
		t.Fatal("CheckLiveness evicted nothing after a peer crashed")
	}
	if len(nodes[0].Members()) != 2 {
		t.Errorf("members after eviction = %d, want 2", len(nodes[0].Members()))
	}
}

func TestNewFailsOnBusyAddr(t *testing.T) {
	addr := freeAddr(t)
	first, err := medusa.New(medusa.Config{Addr: addr})
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })

	if _, err := medusa.New(medusa.Config{Addr: addr}); err == nil {
		t.Fatal("expected New to fail on an in-use address")
	}
}

func TestDispatchRejectsUnknownType(t *testing.T) {
	sw := transport.NewSwitch()
	srvTr := sw.NewTransport("srv")
	n, err := medusa.New(medusa.Config{ID: "srv", Addr: "srv", Transport: srvTr})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })

	cli := sw.NewTransport("cli")
	t.Cleanup(func() { _ = cli.Close() })
	_, _, err = cli.Send(context.Background(), "srv",
		medusav1.MessageType_MESSAGE_TYPE_UNSPECIFIED, nil, nil)
	if err == nil {
		t.Fatal("expected an error dispatching an unspecified message type")
	}
}

func TestFailoverReadsBackupOverTCP(t *testing.T) {
	nodes := startCluster(t, 3)
	ctx := context.Background()
	const n = 300

	for i := 0; i < n; i++ {
		key := []byte("k-" + strconv.Itoa(i))
		val := []byte("v-" + strconv.Itoa(i))
		if err := nodes[0].Map("grid").Put(ctx, key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Kill node 0. Every key it owned has a backup on node 1 or node 2, so all
	// keys remain readable: reads to the dead owner fail over to the backup.
	if err := nodes[0].Close(); err != nil {
		t.Fatalf("close node 0: %v", err)
	}

	for i := 0; i < n; i++ {
		key := []byte("k-" + strconv.Itoa(i))
		want := "v-" + strconv.Itoa(i)
		got, ok, err := nodes[1].Map("grid").Get(ctx, key)
		if err != nil || !ok || string(got) != want {
			t.Fatalf("post-failover get %d = %q,%v,%v want %q", i, got, ok, err, want)
		}
	}
}
