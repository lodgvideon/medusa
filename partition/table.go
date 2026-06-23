// Package partition maps keys to a fixed set of partitions and assigns those
// partitions to cluster members. Both halves are deterministic: every node
// that observes the same membership computes an identical assignment with no
// coordination, which is what lets any node route a key to its owner.
package partition

import "sort"

// Count is the fixed number of partitions. Like Hazelcast's default it is a
// prime, which mildly improves key spread under modulo reduction. The count
// never changes at runtime; only ownership is rebalanced as members join and
// leave.
const Count = 271

// Table assigns every partition an owner member and, when the cluster is large
// enough, a configurable number of distinct backup members. It is rebuilt
// deterministically from a sorted member list.
//
// The assignment uses rendezvous (highest-random-weight) hashing: for each
// partition p, every member is scored by a deterministic weight hash(memberID,
// p), and the members are ranked by that weight — the top member owns p and the
// next backupCount own its backups. Replicas are distinct from each other as
// long as the cluster has more members than replicas; with fewer members the
// surplus backup slots are simply empty.
//
// Rendezvous hashing is chosen over a simpler round-robin (owner = p mod n)
// because it minimises data movement on a membership change: adding or removing
// a member only reassigns the partitions whose top-ranked member actually
// changed — about Count/n of them — instead of reshuffling nearly all of them.
// Balance is statistical rather than exact (each member owns roughly Count/n
// partitions), a worthwhile trade for cheap elastic scaling.
type Table struct {
	members     []string
	owners      [Count]int32
	backups     []int32 // Count rows × backupCount cols, row-major; -1 = empty slot
	backupCount int     // configured backups per partition (replication factor − 1)
}

// hrwWeight is the rendezvous weight of member id for partition p: a 64-bit
// FNV-1a hash over the id bytes followed by the partition number, run through a
// splitmix64 finalizer so the result avalanches well even for short, similar ids
// (e.g. "a","b","c") and sequential partition numbers — without strong mixing,
// rendezvous balance degrades badly for such inputs. It is a pure function of
// (id, p) — no per-process seed — so every node scores a given (member,
// partition) pair identically.
func hrwWeight(id string, p int) uint64 {
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	h := offset64
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= prime64
	}
	u := uint32(p)
	for i := 0; i < 4; i++ {
		h ^= uint64(byte(u >> (8 * i)))
		h *= prime64
	}
	// splitmix64 finalizer — strong avalanche for uniform weights.
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

// NewTable builds a partition table for the given member ids and backup count.
// The input is copied and sorted, so callers may pass an unsorted slice and
// keep using it. A negative backupCount is treated as zero.
func NewTable(memberIDs []string, backupCount int) *Table {
	members := append([]string(nil), memberIDs...)
	sort.Strings(members)
	if backupCount < 0 {
		backupCount = 0
	}

	t := &Table{members: members, backupCount: backupCount}
	t.backups = make([]int32, Count*backupCount) // zero-length when backupCount == 0
	n := len(members)

	// ranked is reused across partitions; for each partition it is filled with
	// (weight, member index) and sorted by descending weight (member index breaks
	// ties so the result is fully deterministic).
	type ranked struct {
		weight uint64
		idx    int32
	}
	scratch := make([]ranked, n)

	for p := 0; p < Count; p++ {
		base := p * backupCount
		if n == 0 {
			t.owners[p] = -1
			for j := 0; j < backupCount; j++ {
				t.backups[base+j] = -1
			}
			continue
		}
		for i := 0; i < n; i++ {
			scratch[i] = ranked{weight: hrwWeight(members[i], p), idx: int32(i)}
		}
		sort.Slice(scratch, func(a, b int) bool {
			if scratch[a].weight != scratch[b].weight {
				return scratch[a].weight > scratch[b].weight
			}
			return scratch[a].idx < scratch[b].idx
		})
		t.owners[p] = scratch[0].idx
		for j := 0; j < backupCount; j++ {
			if j+1 < n {
				t.backups[base+j] = scratch[j+1].idx
			} else {
				t.backups[base+j] = -1 // not enough distinct members for this backup
			}
		}
	}
	return t
}

// Members returns the sorted member ids backing this table.
func (t *Table) Members() []string { return t.members }

// BackupCount returns the configured number of backups per partition (the
// replication factor minus one).
func (t *Table) BackupCount() int { return t.backupCount }

// OwnerOf returns the member id that owns partition p, or "" if the table has
// no members.
func (t *Table) OwnerOf(p int) string {
	i := t.owners[p]
	if i < 0 {
		return ""
	}
	return t.members[i]
}

// NumBackups returns how many actual backup members partition p has. It is at
// most BackupCount and is reduced when the cluster is too small to fill every
// backup slot with a distinct member.
func (t *Table) NumBackups(p int) int {
	base := p * t.backupCount
	n := 0
	for j := 0; j < t.backupCount; j++ {
		if t.backups[base+j] >= 0 {
			n++
		}
	}
	return n
}

// BackupAt returns the i-th backup member id for partition p (0-indexed) and
// whether it exists. Backups are ordered by descending rendezvous weight, so
// BackupAt(p, 0) is the next-ranked member after the owner and the natural
// failover target.
func (t *Table) BackupAt(p, i int) (string, bool) {
	if i < 0 || i >= t.backupCount {
		return "", false
	}
	m := t.backups[p*t.backupCount+i]
	if m < 0 {
		return "", false
	}
	return t.members[m], true
}

// BackupOf returns the first backup member id for partition p and whether one
// exists. It is the failover target for routing and is allocation-free, so the
// hot read path can consult it without touching the heap.
func (t *Table) BackupOf(p int) (string, bool) { return t.BackupAt(p, 0) }
