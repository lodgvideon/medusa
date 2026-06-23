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

// Table assigns every partition an owner member and, when the cluster has more
// than one member, a distinct backup member. It is rebuilt deterministically
// from a sorted member list.
//
// The assignment is a simple round-robin: partition p is owned by the
// (p mod n)-th member and backed up by the (p+1 mod n)-th. This is the
// simplest scheme that keeps ownership balanced and backups distinct; its
// trade-off is that a membership change reshuffles most partitions. A future
// revision can swap in rendezvous (HRW) hashing to minimise data movement
// without changing this type's interface.
type Table struct {
	members []string
	owners  [Count]int32
	backups [Count]int32 // -1 when there is no backup (0 or 1 member)
}

// NewTable builds a partition table for the given member ids. The input is
// copied and sorted, so callers may pass an unsorted slice and keep using it.
func NewTable(memberIDs []string) *Table {
	members := append([]string(nil), memberIDs...)
	sort.Strings(members)

	t := &Table{members: members}
	n := int32(len(members))
	for p := 0; p < Count; p++ {
		if n == 0 {
			t.owners[p] = -1
			t.backups[p] = -1
			continue
		}
		t.owners[p] = int32(p) % n
		if n == 1 {
			t.backups[p] = -1
		} else {
			t.backups[p] = (int32(p) + 1) % n
		}
	}
	return t
}

// Members returns the sorted member ids backing this table.
func (t *Table) Members() []string { return t.members }

// OwnerOf returns the member id that owns partition p, or "" if the table has
// no members.
func (t *Table) OwnerOf(p int) string {
	i := t.owners[p]
	if i < 0 {
		return ""
	}
	return t.members[i]
}

// BackupOf returns the backup member id for partition p and whether one exists.
func (t *Table) BackupOf(p int) (string, bool) {
	i := t.backups[p]
	if i < 0 {
		return "", false
	}
	return t.members[i], true
}
