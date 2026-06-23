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
// The assignment is a simple round-robin over "replicas": replica r of
// partition p is the ((p+r) mod n)-th member, with replica 0 the owner and
// replicas 1..backupCount the backups. Backups are distinct from the owner and
// from each other as long as the cluster has more members than replicas; with
// fewer members the surplus backup slots are simply empty. This is the simplest
// scheme that keeps ownership balanced and replicas distinct; its trade-off is
// that a membership change reshuffles most partitions. A future revision can
// swap in rendezvous (HRW) hashing to minimise data movement without changing
// this type's interface.
type Table struct {
	members     []string
	owners      [Count]int32
	backups     []int32 // Count rows × backupCount cols, row-major; -1 = empty slot
	backupCount int     // configured backups per partition (replication factor − 1)
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
	n := int32(len(members))
	for p := 0; p < Count; p++ {
		if n == 0 {
			t.owners[p] = -1
		} else {
			t.owners[p] = int32(p) % n
		}
		base := p * backupCount
		for j := 0; j < backupCount; j++ {
			r := int32(j + 1) // replica index 1..backupCount
			if r >= n {       // not enough distinct members for this backup
				t.backups[base+j] = -1
			} else {
				t.backups[base+j] = (int32(p) + r) % n
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
// whether it exists. Backups are ordered by replica index, so BackupAt(p, 0) is
// the first successor and the natural failover target.
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
