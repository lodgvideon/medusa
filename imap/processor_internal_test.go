package imap

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/lodgvideon/medusa/partition"
)

// TestRegisterAndExecuteCustomProcessor covers custom processor registration and
// the execute path end to end, including its write-ahead-log records for both a
// Set (the custom processor) and a Delete (the built-in "delete"), plus the
// unknown-processor error and delete-on-missing no-op.
func TestRegisterAndExecuteCustomProcessor(t *testing.T) {
	RegisterProcessor("double", func(cur []byte, _ bool, _ []byte) ([]byte, Action, []byte) {
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, uint64(readInt64(cur)*2))
		return out, Set, out
	})

	s := svcWith(&fakeTransport{})
	if err := s.OpenWAL(filepath.Join(t.TempDir(), "wal.log")); err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer s.CloseWAL()
	ctx := context.Background()

	five := make([]byte, 8)
	binary.BigEndian.PutUint64(five, 5)
	if _, err := s.applyPut(ctx, "m", []byte("k"), five, 0, false); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	out, err := s.applyExecute(ctx, "m", []byte("k"), "double", nil)
	if err != nil {
		t.Fatalf("execute double: %v", err)
	}
	if readInt64(out) != 10 {
		t.Fatalf("double(5) = %d, want 10", readInt64(out))
	}

	if _, err := s.applyExecute(ctx, "m", []byte("k"), "nope", nil); err == nil {
		t.Error("execute of an unknown processor should error")
	}

	// "delete" removes an existing key (Delete action → WAL remove record)...
	if _, err := s.applyExecute(ctx, "m", []byte("k"), "delete", nil); err != nil {
		t.Fatalf("execute delete: %v", err)
	}
	if _, ok := s.store.get(partition.For([]byte("k")), "m", []byte("k")); ok {
		t.Error("delete processor should have removed the key")
	}
	// ...and is a no-op on an already-absent key (Keep action).
	if out, err := s.applyExecute(ctx, "m", []byte("k"), "delete", nil); err != nil || out != nil {
		t.Errorf("delete on missing key = %q,%v, want nil,nil", out, err)
	}
}
