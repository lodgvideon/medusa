package cluster_test

import (
	"context"
	"testing"

	"github.com/lodgvideon/medusa/cluster"
	"github.com/lodgvideon/medusa/codec"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/partition"
	"github.com/lodgvideon/medusa/transport"
)

// harness builds in-memory nodes on a shared switch. The address equals the id.
type harness struct {
	sw  *transport.Switch
	trs map[string]transport.Transport
}

func newHarness() *harness {
	return &harness{sw: transport.NewSwitch(), trs: map[string]transport.Transport{}}
}

func (h *harness) node(t *testing.T, id string) *cluster.Membership {
	t.Helper()
	tr := h.sw.NewTransport(id)
	mem := cluster.New(cluster.Member{ID: id, Addr: id}, tr)
	if err := tr.Listen(mem.Handle); err != nil {
		t.Fatalf("Listen(%s): %v", id, err)
	}
	h.trs[id] = tr
	t.Cleanup(func() { _ = tr.Close() })
	return mem
}

// crash closes a node's transport so it stops answering — a simulated failure.
func (h *harness) crash(id string) { _ = h.trs[id].Close() }

func ids(members []cluster.Member) []string {
	out := make([]string, len(members))
	for i, m := range members {
		out[i] = m.ID
	}
	return out
}

func sameMembers(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// assertSameTable fails if two memberships would route any partition differently.
func assertSameTable(t *testing.T, a, b *cluster.Membership) {
	t.Helper()
	ta, tb := a.Table(), b.Table()
	for p := 0; p < partition.Count; p++ {
		if ta.OwnerOf(p) != tb.OwnerOf(p) {
			t.Fatalf("owner disagreement at partition %d: %q vs %q", p, ta.OwnerOf(p), tb.OwnerOf(p))
		}
	}
}

func TestNewMembershipContainsSelf(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	if got := ids(a.Members()); !sameMembers(got, "a") {
		t.Fatalf("members = %v, want [a]", got)
	}
	if a.Table().OwnerOf(0) != "a" {
		t.Errorf("solo owner = %q, want a", a.Table().OwnerOf(0))
	}
}

func TestJoinTwoNodesConverge(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a") // seed
	b := h.node(t, "b")

	if err := b.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	for _, m := range []*cluster.Membership{a, b} {
		if got := ids(m.Members()); !sameMembers(got, "a", "b") {
			t.Fatalf("members = %v, want [a b]", got)
		}
	}
	assertSameTable(t, a, b)
}

func TestJoinThreeNodesConverge(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a") // seed
	b := h.node(t, "b")
	c := h.node(t, "c")

	if err := b.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("b.Join: %v", err)
	}
	if err := c.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("c.Join: %v", err)
	}

	for name, m := range map[string]*cluster.Membership{"a": a, "b": b, "c": c} {
		if got := ids(m.Members()); !sameMembers(got, "a", "b", "c") {
			t.Fatalf("node %s members = %v, want [a b c]", name, got)
		}
	}
	assertSameTable(t, a, b)
	assertSameTable(t, a, c)
}

func TestMergeIsIdempotent(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	b := h.node(t, "b")
	if err := b.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	before := a.Version()
	b.Gossip(context.Background()) // no new members
	if after := a.Version(); after != before {
		t.Errorf("version moved on redundant gossip: %d -> %d", before, after)
	}
}

func TestRemoveShrinksView(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	b := h.node(t, "b")
	if err := b.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	if !a.Remove("b") {
		t.Fatal("Remove(b) = false, want true")
	}
	if got := ids(a.Members()); !sameMembers(got, "a") {
		t.Fatalf("members after remove = %v, want [a]", got)
	}
	if a.Remove("b") {
		t.Error("removing an absent member returned true")
	}
}

func TestCannotRemoveSelf(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	if a.Remove("a") {
		t.Error("Remove(self) = true, want false")
	}
}

func TestAddrOf(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	if addr, ok := a.AddrOf("a"); !ok || addr != "a" {
		t.Errorf("AddrOf(a) = %q,%v want a,true", addr, ok)
	}
	if _, ok := a.AddrOf("ghost"); ok {
		t.Error("AddrOf(ghost) ok = true, want false")
	}
}

func TestCheckLivenessEvictsDeadPeer(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	b := h.node(t, "b")
	c := h.node(t, "c")
	if err := b.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("b.Join: %v", err)
	}
	if err := c.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("c.Join: %v", err)
	}

	h.crash("c") // c stops answering heartbeats

	evicted := a.CheckLiveness(context.Background())
	if !sameMembers(evicted, "c") {
		t.Fatalf("evicted = %v, want [c]", evicted)
	}
	if _, ok := a.AddrOf("c"); ok {
		t.Error("c still present after eviction")
	}
	if _, ok := a.AddrOf("b"); !ok {
		t.Error("live peer b was wrongly evicted")
	}
}

func TestDetectFailuresEvictsAfterThreshold(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	b := h.node(t, "b")
	if err := b.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	ctx := context.Background()
	h.crash("b") // b stops answering heartbeats

	// With threshold 3, the first two checks must not evict.
	if got := a.DetectFailures(ctx, 3); len(got) != 0 {
		t.Fatalf("evicted after 1 miss: %v", got)
	}
	if got := a.DetectFailures(ctx, 3); len(got) != 0 {
		t.Fatalf("evicted after 2 misses: %v", got)
	}
	if got := a.DetectFailures(ctx, 3); !sameMembers(got, "b") {
		t.Fatalf("evicted after 3 misses = %v, want [b]", got)
	}
	if _, ok := a.AddrOf("b"); ok {
		t.Error("b still present after eviction")
	}
}

func TestRejoinClearsTombstone(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	b := h.node(t, "b")
	ctx := context.Background()
	if err := b.Join(ctx, []string{"a"}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	// Evict b.
	h.crash("b")
	for i := 0; i < 3; i++ {
		a.DetectFailures(ctx, 3)
	}
	if _, ok := a.AddrOf("b"); ok {
		t.Fatal("b not evicted")
	}

	// Gossip must NOT resurrect a tombstoned member.
	ml := &medusav1.MemberList{Members: []*medusav1.Member{{Id: "b", Addr: "b"}}}
	mlBytes, _ := codec.Marshal(nil, ml)
	_, _, _ = a.Handle(medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST, mlBytes, nil)
	if _, ok := a.AddrOf("b"); ok {
		t.Fatal("gossip resurrected a tombstoned member")
	}

	// An explicit JOIN (b restarted) must clear the tombstone and re-admit it.
	jr := &medusav1.JoinRequest{Candidate: &medusav1.Member{Id: "b", Addr: "b"}}
	jrBytes, _ := codec.Marshal(nil, jr)
	if _, _, err := a.Handle(medusav1.MessageType_MESSAGE_TYPE_JOIN_REQUEST, jrBytes, nil); err != nil {
		t.Fatalf("Handle JOIN: %v", err)
	}
	if _, ok := a.AddrOf("b"); !ok {
		t.Fatal("a restarted node could not rejoin after eviction")
	}
}

func TestPruneTombstonesAllowsReAdd(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	b := h.node(t, "b")
	ctx := context.Background()
	if err := b.Join(ctx, []string{"a"}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	h.crash("b")
	for i := 0; i < 3; i++ {
		a.DetectFailures(ctx, 3)
	}
	if _, ok := a.AddrOf("b"); ok {
		t.Fatal("b not evicted")
	}

	gossipB := func() {
		ml := &medusav1.MemberList{Members: []*medusav1.Member{{Id: "b", Addr: "b"}}}
		payload, _ := codec.Marshal(nil, ml)
		_, _, _ = a.Handle(medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST, payload, nil)
	}
	gossipB()
	if _, ok := a.AddrOf("b"); ok {
		t.Fatal("tombstoned member resurrected by gossip")
	}

	a.PruneTombstones(0) // expire every tombstone
	gossipB()
	if _, ok := a.AddrOf("b"); !ok {
		t.Fatal("member not re-addable after its tombstone was pruned")
	}
}

func TestDetectFailuresKeepsLivePeer(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	b := h.node(t, "b")
	if err := b.Join(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if got := a.DetectFailures(ctx, 2); len(got) != 0 {
			t.Fatalf("evicted a live peer: %v", got)
		}
	}
	if _, ok := a.AddrOf("b"); !ok {
		t.Error("live peer b was wrongly evicted")
	}
}

func TestMergeUpdatesChangedAddr(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")

	gossip := func(addr string) {
		ml := &medusav1.MemberList{Members: []*medusav1.Member{{Id: "b", Addr: addr}}}
		payload, err := codec.Marshal(nil, ml)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, _, err := a.Handle(medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST, payload, nil); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}

	gossip("b-addr-1")
	if addr, _ := a.AddrOf("b"); addr != "b-addr-1" {
		t.Fatalf("addr = %q, want b-addr-1", addr)
	}
	// A node that restarted at a new address must heal peers' view.
	gossip("b-addr-2")
	if addr, _ := a.AddrOf("b"); addr != "b-addr-2" {
		t.Fatalf("addr after re-gossip = %q, want b-addr-2", addr)
	}
}

func TestHandleUnknownTypeErrors(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	if _, _, err := a.Handle(medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST, nil, nil); err == nil {
		t.Fatal("expected error for unhandled message type")
	}
}

func TestJoinUnreachableSeedErrors(t *testing.T) {
	h := newHarness()
	b := h.node(t, "b")
	if err := b.Join(context.Background(), []string{"nonexistent"}); err == nil {
		t.Fatal("expected error joining via an unreachable seed")
	}
}

func TestJoinNoSeedsIsNoop(t *testing.T) {
	h := newHarness()
	a := h.node(t, "a")
	if err := a.Join(context.Background(), nil); err != nil {
		t.Fatalf("Join(nil) = %v, want nil", err)
	}
	if got := ids(a.Members()); !sameMembers(got, "a") {
		t.Fatalf("members = %v, want [a]", got)
	}
}
