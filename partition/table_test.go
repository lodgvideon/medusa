package partition_test

import (
	"testing"

	"github.com/lodgvideon/medusa/partition"
)

func TestTableDeterministicAcrossOrder(t *testing.T) {
	a := partition.NewTable([]string{"c", "a", "b"}, 1)
	b := partition.NewTable([]string{"b", "c", "a"}, 1) // same set, different order

	for p := 0; p < partition.Count; p++ {
		if a.OwnerOf(p) != b.OwnerOf(p) {
			t.Fatalf("owner mismatch at %d: %q vs %q", p, a.OwnerOf(p), b.OwnerOf(p))
		}
		ab, _ := a.BackupOf(p)
		bb, _ := b.BackupOf(p)
		if ab != bb {
			t.Fatalf("backup mismatch at %d: %q vs %q", p, ab, bb)
		}
	}
}

func TestTableSingleMemberHasNoBackup(t *testing.T) {
	tbl := partition.NewTable([]string{"only"}, 1)
	for p := 0; p < partition.Count; p++ {
		if tbl.OwnerOf(p) != "only" {
			t.Fatalf("owner at %d = %q, want %q", p, tbl.OwnerOf(p), "only")
		}
		if _, ok := tbl.BackupOf(p); ok {
			t.Fatalf("single-member table must have no backup at %d", p)
		}
		if n := tbl.NumBackups(p); n != 0 {
			t.Fatalf("single-member table NumBackups at %d = %d, want 0", p, n)
		}
	}
}

func TestTableEmptyMembers(t *testing.T) {
	tbl := partition.NewTable(nil, 1)
	if got := tbl.OwnerOf(0); got != "" {
		t.Errorf("empty table OwnerOf = %q, want \"\"", got)
	}
	if _, ok := tbl.BackupOf(0); ok {
		t.Error("empty table must have no backup")
	}
}

func TestTableOwnerAndBackupDistinct(t *testing.T) {
	tbl := partition.NewTable([]string{"a", "b", "c", "d"}, 1)
	for p := 0; p < partition.Count; p++ {
		owner := tbl.OwnerOf(p)
		backup, ok := tbl.BackupOf(p)
		if !ok {
			t.Fatalf("expected a backup at %d", p)
		}
		if owner == backup {
			t.Fatalf("owner == backup at %d: %q", p, owner)
		}
	}
}

func TestTableOwnershipBalanced(t *testing.T) {
	members := []string{"a", "b", "c", "d", "e"}
	tbl := partition.NewTable(members, 1)

	counts := make(map[string]int, len(members))
	for p := 0; p < partition.Count; p++ {
		counts[tbl.OwnerOf(p)]++
	}
	// Count=271 over 5 members => each owns 54 or 55 partitions.
	for _, m := range members {
		if counts[m] < 54 || counts[m] > 55 {
			t.Errorf("member %q owns %d partitions, want 54-55", m, counts[m])
		}
	}
}

func TestTableMembersSorted(t *testing.T) {
	tbl := partition.NewTable([]string{"c", "a", "b"}, 1)
	got := tbl.Members()
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("Members() = %v, want [a b c]", got)
	}
}

func TestTableCopiesInput(t *testing.T) {
	in := []string{"b", "a"}
	_ = partition.NewTable(in, 1)
	// NewTable must not mutate the caller's slice order.
	if in[0] != "b" || in[1] != "a" {
		t.Errorf("NewTable mutated caller input: %v", in)
	}
}

func TestTableBackupCount(t *testing.T) {
	tbl := partition.NewTable([]string{"a", "b", "c"}, 2)
	if tbl.BackupCount() != 2 {
		t.Fatalf("BackupCount() = %d, want 2", tbl.BackupCount())
	}
	if got := partition.NewTable(nil, -3).BackupCount(); got != 0 {
		t.Fatalf("negative backupCount = %d, want clamped to 0", got)
	}
}

// With backupCount=2 and ≥3 members, every partition has two distinct backups,
// and owner+backups are all different members.
func TestTableMultipleBackupsDistinct(t *testing.T) {
	members := []string{"a", "b", "c", "d", "e"}
	tbl := partition.NewTable(members, 2)
	for p := 0; p < partition.Count; p++ {
		if n := tbl.NumBackups(p); n != 2 {
			t.Fatalf("NumBackups at %d = %d, want 2", p, n)
		}
		owner := tbl.OwnerOf(p)
		b0, ok0 := tbl.BackupAt(p, 0)
		b1, ok1 := tbl.BackupAt(p, 1)
		if !ok0 || !ok1 {
			t.Fatalf("missing backup at %d: %v %v", p, ok0, ok1)
		}
		if owner == b0 || owner == b1 || b0 == b1 {
			t.Fatalf("replicas not distinct at %d: owner=%q b0=%q b1=%q", p, owner, b0, b1)
		}
	}
}

// Requesting more backups than the cluster can supply yields as many distinct
// backups as there are other members — never a duplicate of the owner.
func TestTableBackupsCappedBySmallCluster(t *testing.T) {
	tbl := partition.NewTable([]string{"a", "b"}, 3) // only 1 other member available
	for p := 0; p < partition.Count; p++ {
		if n := tbl.NumBackups(p); n != 1 {
			t.Fatalf("NumBackups at %d = %d, want 1 (capped)", p, n)
		}
		if _, ok := tbl.BackupAt(p, 1); ok {
			t.Fatalf("backup slot 1 at %d should be empty in a 2-member cluster", p)
		}
		owner := tbl.OwnerOf(p)
		b0, _ := tbl.BackupAt(p, 0)
		if owner == b0 {
			t.Fatalf("backup equals owner at %d: %q", p, owner)
		}
	}
}

// BackupAt rejects indices outside the configured backup range.
func TestTableBackupAtOutOfRange(t *testing.T) {
	tbl := partition.NewTable([]string{"a", "b", "c"}, 1)
	if _, ok := tbl.BackupAt(0, 1); ok { // i == backupCount
		t.Error("BackupAt with i >= backupCount should report no backup")
	}
	if _, ok := tbl.BackupAt(0, -1); ok { // negative index
		t.Error("BackupAt with negative i should report no backup")
	}
}

// Replicas (owner + all backups) must be deterministic regardless of input order.
func TestTableMultiBackupDeterministic(t *testing.T) {
	a := partition.NewTable([]string{"c", "a", "d", "b"}, 2)
	b := partition.NewTable([]string{"b", "d", "a", "c"}, 2)
	for p := 0; p < partition.Count; p++ {
		for i := 0; i < 2; i++ {
			ai, _ := a.BackupAt(p, i)
			bi, _ := b.BackupAt(p, i)
			if ai != bi {
				t.Fatalf("backup %d mismatch at %d: %q vs %q", i, p, ai, bi)
			}
		}
	}
}
