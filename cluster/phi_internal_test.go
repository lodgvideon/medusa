package cluster

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/transport"
)

const sec = int64(time.Second)

// warm builds a phiHistory of n intervals of the given size (nanoseconds) whose
// last heartbeat was at `last`. Fields are set directly so the estimator is
// driven by deterministic timestamps with no real clock.
func warm(interval int64, n int, last int64) *phiHistory {
	h := newPhiHistory()
	for i := 0; i < n; i++ {
		iv := float64(interval)
		h.intervals = append(h.intervals, iv)
		h.sum += iv
		h.sumSq += iv * iv
	}
	h.last = last
	return h
}

// TestPhiRisesWithSilence proves suspicion is ~0 right after a heartbeat, rises
// monotonically with continued silence, and eventually crosses the eviction
// threshold — the core phi-accrual behaviour.
func TestPhiRisesWithSilence(t *testing.T) {
	last := int64(100) * sec
	h := warm(sec, 10, last) // mean 1s
	if !h.warm() {
		t.Fatal("10 samples should be warm")
	}
	if p := h.phi(last); p > 1 {
		t.Errorf("phi immediately after a heartbeat = %.2f, want ~0", p)
	}
	prev := -1.0
	for gap := int64(0); gap <= 12; gap++ {
		p := h.phi(last + gap*sec)
		if p < prev-1e-9 {
			t.Errorf("phi not monotonic: gap=%ds phi=%.2f < prev=%.2f", gap, p, prev)
		}
		prev = p
	}
	if p := h.phi(last + 30*sec); p < defaultPhiThreshold {
		t.Errorf("phi after 30s of silence = %.2f, want >= %.1f", p, defaultPhiThreshold)
	}
}

// TestPhiToleratesJitter proves the detector's point: at the same gap, a link
// whose heartbeats arrive with high variance is less suspect than a metronomic
// one — so jitter does not trigger a false eviction.
func TestPhiToleratesJitter(t *testing.T) {
	last := int64(1000) * sec
	gap := last + 4*sec

	steady := warm(sec, 10, last) // mean 1s, variance ~0 (floored)

	jittery := newPhiHistory() // same 1s mean, wide spread
	for i := 0; i < 10; i++ {
		iv := float64(sec) / 5
		if i%2 == 1 {
			iv = float64(sec) * 9 / 5
		}
		jittery.intervals = append(jittery.intervals, iv)
		jittery.sum += iv
		jittery.sumSq += iv * iv
	}
	jittery.last = last

	ps, pj := steady.phi(gap), jittery.phi(gap)
	if pj >= ps {
		t.Errorf("jittery phi (%.2f) should be below steady phi (%.2f) at the same gap", pj, ps)
	}
}

// TestPhiColdReturnsZero proves an under-sampled history yields phi 0, so the
// caller falls back to the consecutive-miss count.
func TestPhiColdReturnsZero(t *testing.T) {
	h := newPhiHistory()
	h.record(sec)
	h.record(2 * sec) // a single interval — below phiMinSamples
	if h.warm() {
		t.Fatal("one interval should not be warm")
	}
	if p := h.phi(100 * sec); p != 0 {
		t.Errorf("cold phi = %.2f, want 0", p)
	}
}

// TestPhiHistoryWindowBounded proves the interval window is capped (so memory is
// bounded) and the running mean stays correct as it rotates.
func TestPhiHistoryWindowBounded(t *testing.T) {
	h := newPhiHistory()
	for i := 0; i <= phiWindow+50; i++ {
		h.record(int64(i) * sec)
	}
	if len(h.intervals) != phiWindow {
		t.Errorf("window length = %d, want %d", len(h.intervals), phiWindow)
	}
	if m := h.mean(); math.Abs(m-float64(sec)) > 1 {
		t.Errorf("mean = %.0f, want ~%d", m, sec)
	}
}

// failTransport makes every Send fail, so a peer pinged through it looks
// unreachable — for driving DetectFailures without a live peer.
type failTransport struct{}

func (failTransport) Addr() string                   { return "self" }
func (failTransport) Listen(transport.Handler) error { return nil }
func (failTransport) Close() error                   { return nil }
func (failTransport) Send(context.Context, string, medusav1.MessageType, []byte, []byte) (medusav1.MessageType, []byte, error) {
	return 0, nil, errors.New("unreachable")
}

// TestDetectFailuresPhiEvictsWarmPeer proves a warm peer is evicted by phi, not
// the miss count: with an ancient last heartbeat phi is enormous, so a single
// check evicts even though the miss threshold is far higher.
func TestDetectFailuresPhiEvictsWarmPeer(t *testing.T) {
	m := New(Member{ID: "a", Addr: "a"}, failTransport{}, 1)
	m.merge([]Member{{ID: "b", Addr: "b"}})
	m.phi["b"] = warm(sec, 10, sec) // last heartbeat ~1970 → vast silence vs now

	if got := m.DetectFailures(context.Background(), 250); len(got) != 1 || got[0] != "b" {
		t.Fatalf("warm dead peer evicted = %v, want [b]", got)
	}
	if _, ok := m.AddrOf("b"); ok {
		t.Error("b still present after phi eviction")
	}
}

// TestRejoinResetsPhiHistory proves an explicit JOIN clears a peer's heartbeat
// history (matching removeLocked), so the first ping after a restart cannot
// record a downtime-spanning interval that would corrupt the estimator and delay
// future evictions. Regression guard for the rejoin/removeLocked asymmetry.
func TestRejoinResetsPhiHistory(t *testing.T) {
	m := New(Member{ID: "a", Addr: "a"}, failTransport{}, 1)
	m.merge([]Member{{ID: "b", Addr: "b"}})
	m.phi["b"] = warm(sec, 10, sec) // warm history, still a member (never evicted)

	m.rejoin(Member{ID: "b", Addr: "b"})

	if _, ok := m.phi["b"]; ok {
		t.Fatal("rejoin must clear the phi history so a restarted peer's estimator starts fresh")
	}
}

// TestDetectFailuresPhiToleratesWarmBlip proves phi overrides the miss count for
// a warm peer: a single missed heartbeat after a recent one is tolerated, even
// with the miss threshold at 1 (which would evict a cold peer immediately).
func TestDetectFailuresPhiToleratesWarmBlip(t *testing.T) {
	m := New(Member{ID: "a", Addr: "a"}, failTransport{}, 1)
	m.merge([]Member{{ID: "b", Addr: "b"}})
	m.phi["b"] = warm(sec, 10, time.Now().UnixNano()) // heartbeat just now

	if got := m.DetectFailures(context.Background(), 1); len(got) != 0 {
		t.Fatalf("phi should tolerate one blip on a warm peer, evicted %v", got)
	}
	if _, ok := m.AddrOf("b"); !ok {
		t.Fatal("warm peer wrongly evicted on a transient blip")
	}
}
