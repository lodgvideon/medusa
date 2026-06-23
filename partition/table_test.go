package partition_test

import (
	"testing"

	"github.com/lodgvideon/medusa/partition"
)

func TestTableDeterministicAcrossOrder(t *testing.T) {
	a := partition.NewTable([]string{"c", "a", "b"})
	b := partition.NewTable([]string{"b", "c", "a"}) // same set, different order

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
	tbl := partition.NewTable([]string{"only"})
	for p := 0; p < partition.Count; p++ {
		if tbl.OwnerOf(p) != "only" {
			t.Fatalf("owner at %d = %q, want %q", p, tbl.OwnerOf(p), "only")
		}
		if _, ok := tbl.BackupOf(p); ok {
			t.Fatalf("single-member table must have no backup at %d", p)
		}
	}
}

func TestTableEmptyMembers(t *testing.T) {
	tbl := partition.NewTable(nil)
	if got := tbl.OwnerOf(0); got != "" {
		t.Errorf("empty table OwnerOf = %q, want \"\"", got)
	}
	if _, ok := tbl.BackupOf(0); ok {
		t.Error("empty table must have no backup")
	}
}

func TestTableOwnerAndBackupDistinct(t *testing.T) {
	tbl := partition.NewTable([]string{"a", "b", "c", "d"})
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
	tbl := partition.NewTable(members)

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
	tbl := partition.NewTable([]string{"c", "a", "b"})
	got := tbl.Members()
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("Members() = %v, want [a b c]", got)
	}
}

func TestTableCopiesInput(t *testing.T) {
	in := []string{"b", "a"}
	_ = partition.NewTable(in)
	// NewTable must not mutate the caller's slice order.
	if in[0] != "b" || in[1] != "a" {
		t.Errorf("NewTable mutated caller input: %v", in)
	}
}
