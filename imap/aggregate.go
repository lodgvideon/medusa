package imap

import (
	"encoding/binary"
	"errors"
	"sync"
)

// ErrUnknownAggregator is returned (wrapped) when Map.Aggregate is given a name
// no aggregator is registered under — a caller/config error, distinct from a peer
// being unreachable. The HTTP layer maps it to 400 rather than 502.
var ErrUnknownAggregator = errors.New("imap: unknown aggregator")

// Aggregator is a distributed map-reduce reducer over a map's values, invoked by
// registered name so the name (not a closure) crosses the network — like an
// EntryProcessor. Reduce folds the values a member OWNS into an encoded partial on
// that member; Combine merges every member's partial into the final result on the
// caller. Each entry is folded exactly once (a backup copy is never visited). It
// is the injection seam (SOLID dependency inversion) for custom reductions.
//
// Contract: Combine must be associative and commutative — members are visited in
// arbitrary order and an unreachable member is simply omitted — and both halves
// must be deterministic. An empty partial ([]byte of length 0) means "this member
// owned nothing"; Combine should treat it as the identity.
type Aggregator interface {
	Reduce(values [][]byte) []byte
	Combine(partials [][]byte) []byte
}

var (
	aggMu       sync.RWMutex
	aggregators = map[string]Aggregator{
		"count": countAgg{},
		"sum":   sumAgg{},
		"min":   minAgg{},
		"max":   maxAgg{},
	}
)

// RegisterAggregator makes agg available to Map.Aggregate under name. Register the
// same aggregator under the same name on every node: it runs on each member's
// owned data and is combined on the caller, so all nodes must resolve the name.
func RegisterAggregator(name string, agg Aggregator) {
	aggMu.Lock()
	defer aggMu.Unlock()
	aggregators[name] = agg
}

func lookupAggregator(name string) (Aggregator, bool) {
	aggMu.RLock()
	defer aggMu.RUnlock()
	a, ok := aggregators[name]
	return a, ok
}

// putI64 encodes n as a big-endian int64 — the partial/result form of the numeric
// built-ins, matching the incr counter encoding so a map of counters sums cleanly.
func putI64(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

// sumI64 sums each element read as a big-endian int64 (readInt64 treats a short
// or empty slice as 0). It is both the count/sum Combine and the sum Reduce.
func sumI64(parts [][]byte) int64 {
	var sum int64
	for _, p := range parts {
		sum += readInt64(p)
	}
	return sum
}

// countAgg counts entries; the value is ignored. Partial and result are int64.
type countAgg struct{}

func (countAgg) Reduce(values [][]byte) []byte    { return putI64(int64(len(values))) }
func (countAgg) Combine(partials [][]byte) []byte { return putI64(sumI64(partials)) }

// sumAgg sums values read as big-endian int64.
type sumAgg struct{}

func (sumAgg) Reduce(values [][]byte) []byte    { return putI64(sumI64(values)) }
func (sumAgg) Combine(partials [][]byte) []byte { return putI64(sumI64(partials)) }

// minAgg / maxAgg take the extreme of values read as big-endian int64. An empty
// input yields an empty partial; Combine skips empty partials, so over an empty
// map the result is empty (no minimum/maximum exists).
type minAgg struct{}

func (minAgg) Reduce(values [][]byte) []byte    { return extremum(values, true) }
func (minAgg) Combine(partials [][]byte) []byte { return extremum(partials, true) }

type maxAgg struct{}

func (maxAgg) Reduce(values [][]byte) []byte    { return extremum(values, false) }
func (maxAgg) Combine(partials [][]byte) []byte { return extremum(partials, false) }

// extremum returns the min (or max) of the non-empty elements as a big-endian
// int64, or an empty slice when there are none.
func extremum(items [][]byte, min bool) []byte {
	var best int64
	seen := false
	for _, it := range items {
		if len(it) == 0 {
			continue // an empty partial contributes no value
		}
		x := readInt64(it)
		if !seen || (min && x < best) || (!min && x > best) {
			best, seen = x, true
		}
	}
	if !seen {
		return nil
	}
	return putI64(best)
}
