package imap_test

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/lodgvideon/medusa/imap"
)

func aggEnc(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}
func aggDec(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }

// TestMapAggregateDistributed proves the cluster-wide map-reduce: every member
// reduces the entries it owns and the caller combines the partials, so any node
// computes the same count/sum/min/max over the whole map.
func TestMapAggregateDistributed(t *testing.T) {
	f := newFixture()
	a, b, c := f.cluster3(t)
	ctx := context.Background()

	var want int64
	for n := int64(1); n <= 10; n++ {
		if err := a.svc.Map("nums").Put(ctx, []byte{byte(n)}, aggEnc(n)); err != nil {
			t.Fatalf("put %d: %v", n, err)
		}
		want += n
	}

	for _, nd := range []*node{a, b, c} {
		m := nd.svc.Map("nums")
		if got, err := m.Aggregate(ctx, "count"); err != nil || aggDec(got) != 10 {
			t.Fatalf("count from %s = %d,%v; want 10", nd.id, aggDec(got), err)
		}
		if got, err := m.Aggregate(ctx, "sum"); err != nil || aggDec(got) != want {
			t.Fatalf("sum from %s = %d,%v; want %d", nd.id, aggDec(got), err, want)
		}
		if got, err := m.Aggregate(ctx, "min"); err != nil || aggDec(got) != 1 {
			t.Fatalf("min from %s = %d,%v; want 1", nd.id, aggDec(got), err)
		}
		if got, err := m.Aggregate(ctx, "max"); err != nil || aggDec(got) != 10 {
			t.Fatalf("max from %s = %d,%v; want 10", nd.id, aggDec(got), err)
		}
	}

	if _, err := a.svc.Map("nums").Aggregate(ctx, "nope"); !errors.Is(err, imap.ErrUnknownAggregator) {
		t.Fatalf("unknown aggregator err = %v, want imap.ErrUnknownAggregator", err)
	}
	// Empty map: count is 0; min has no value.
	if got, _ := b.svc.Map("empty").Aggregate(ctx, "count"); aggDec(got) != 0 {
		t.Fatalf("count over empty map = %d, want 0", aggDec(got))
	}
	if got, _ := b.svc.Map("empty").Aggregate(ctx, "min"); got != nil {
		t.Fatalf("min over empty map = %v, want nil", got)
	}
}
