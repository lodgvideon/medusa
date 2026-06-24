package imap

import "testing"

func TestBuiltinAggregators(t *testing.T) {
	vals := [][]byte{i64(10), i64(20), i64(-5)}

	if got := readInt64(countAgg{}.Reduce(vals)); got != 3 {
		t.Errorf("count.Reduce = %d, want 3", got)
	}
	if got := readInt64(countAgg{}.Combine([][]byte{i64(3), i64(2), nil})); got != 5 {
		t.Errorf("count.Combine = %d, want 5", got)
	}
	if got := readInt64(sumAgg{}.Reduce(vals)); got != 25 {
		t.Errorf("sum.Reduce = %d, want 25", got)
	}
	if got := readInt64(sumAgg{}.Combine([][]byte{i64(25), i64(5)})); got != 30 {
		t.Errorf("sum.Combine = %d, want 30", got)
	}
	if got := readInt64(minAgg{}.Reduce(vals)); got != -5 {
		t.Errorf("min.Reduce = %d, want -5", got)
	}
	if got := readInt64(maxAgg{}.Reduce(vals)); got != 20 {
		t.Errorf("max.Reduce = %d, want 20", got)
	}
}

func TestAggregatorEmptyInput(t *testing.T) {
	// count/sum over nothing are the additive identity 0; min/max have no value.
	if got := readInt64(countAgg{}.Reduce(nil)); got != 0 {
		t.Errorf("count over empty = %d, want 0", got)
	}
	if got := readInt64(sumAgg{}.Reduce(nil)); got != 0 {
		t.Errorf("sum over empty = %d, want 0", got)
	}
	if got := (minAgg{}).Reduce(nil); got != nil {
		t.Errorf("min over empty = %v, want nil", got)
	}
	if got := (maxAgg{}).Combine([][]byte{nil, nil}); got != nil {
		t.Errorf("max combine of all-empty = %v, want nil", got)
	}
	// Combine skips empty partials (members that owned nothing).
	if got := readInt64((minAgg{}).Combine([][]byte{nil, i64(4), nil, i64(2)})); got != 2 {
		t.Errorf("min.Combine skipping empties = %d, want 2", got)
	}
}

func TestRegisterAndLookupAggregator(t *testing.T) {
	for _, name := range []string{"count", "sum", "min", "max"} {
		if _, ok := lookupAggregator(name); !ok {
			t.Errorf("built-in %q must be registered", name)
		}
	}
	if _, ok := lookupAggregator("no_such_agg"); ok {
		t.Error("an unregistered name must not resolve")
	}
	RegisterAggregator("agg_test_custom", countAgg{})
	if _, ok := lookupAggregator("agg_test_custom"); !ok {
		t.Error("a registered aggregator must resolve")
	}
}
