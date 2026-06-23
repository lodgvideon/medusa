package partition_test

import (
	"strconv"
	"testing"

	"github.com/lodgvideon/medusa/partition"
)

func TestForInRange(t *testing.T) {
	for i := 0; i < 10000; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		p := partition.For(key)
		if p < 0 || p >= partition.Count {
			t.Fatalf("For(%q) = %d, want in [0,%d)", key, p, partition.Count)
		}
	}
}

func TestForDeterministic(t *testing.T) {
	key := []byte("the-same-key-every-time")
	first := partition.For(key)
	for i := 0; i < 1000; i++ {
		if got := partition.For(key); got != first {
			t.Fatalf("For not deterministic: got %d, first was %d", got, first)
		}
	}
}

func TestForEmptyKey(t *testing.T) {
	p := partition.For(nil)
	if p < 0 || p >= partition.Count {
		t.Fatalf("For(nil) = %d, want in [0,%d)", p, partition.Count)
	}
}

func TestForDistribution(t *testing.T) {
	counts := make([]int, partition.Count)
	const n = partition.Count * 1000 // ~1000 keys per partition if uniform
	for i := 0; i < n; i++ {
		counts[partition.For([]byte("key-"+strconv.Itoa(i)))]++
	}
	for p, c := range counts {
		if c == 0 {
			t.Errorf("partition %d received no keys — distribution too skewed", p)
		}
		if c > 3000 { // 3x the ~1000 expected
			t.Errorf("partition %d received %d keys — distribution too skewed", p, c)
		}
	}
}

func TestForZeroAlloc(t *testing.T) {
	key := []byte("allocation-sensitive-key")
	allocs := testing.AllocsPerRun(1000, func() {
		_ = partition.For(key)
	})
	if allocs != 0 {
		t.Fatalf("For allocated %v allocs/op, want 0", allocs)
	}
}
