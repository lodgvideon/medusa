package partition

// For maps a key to a partition id in the range [0, Count).
//
// Contract (enforced by hash_test.go):
//   - Deterministic: equal keys always map to the same partition — across
//     nodes and across process restarts. (No per-process seed.)
//   - In range: the result is always within [0, Count).
//   - Allocation-free: the read/write hot path calls this on every operation,
//     so it must not allocate. Note that fnv.New32a() / hash.Hash escapes to
//     the heap — fold the key bytes inline instead.
//   - Well-distributed: distinct keys spread roughly evenly across partitions.
//
// For uses FNV-1a folded inline over the key bytes (no hash.Hash, so nothing
// escapes to the heap), then reduces the 32-bit result into [0, Count) with a
// modulo. A 32-bit hash over a 271-way modulo has negligible bias.
func For(key []byte) int {
	const (
		offsetBasis uint32 = 2166136261
		prime       uint32 = 16777619
	)
	h := offsetBasis
	for _, b := range key {
		h ^= uint32(b)
		h *= prime
	}
	return int(h % Count)
}
