package imap

import (
	"testing"
	"time"

	"github.com/lodgvideon/medusa/partition"
)

func TestStorePutGetRemove(t *testing.T) {
	s := newStore()
	p := partition.For([]byte("k"))

	if _, ok := s.get(p, "m", []byte("k")); ok {
		t.Fatal("get on empty store returned ok")
	}
	if created := s.put(p, "m", []byte("k"), []byte("v1"), 0); !created {
		t.Fatal("first put created = false, want true")
	}
	if created := s.put(p, "m", []byte("k"), []byte("v2"), 0); created {
		t.Fatal("overwrite put created = true, want false")
	}
	v, ok := s.get(p, "m", []byte("k"))
	if !ok || string(v) != "v2" {
		t.Fatalf("get = %q,%v want v2,true", v, ok)
	}
	if !s.remove(p, "m", []byte("k")) {
		t.Fatal("remove existing = false, want true")
	}
	if s.remove(p, "m", []byte("k")) {
		t.Fatal("remove absent = true, want false")
	}
}

func TestStoreCopiesValue(t *testing.T) {
	s := newStore()
	p := partition.For([]byte("k"))
	val := []byte("original")
	s.put(p, "m", []byte("k"), val, 0)

	val[0] = 'X' // mutate the caller's buffer after Put
	v, _ := s.get(p, "m", []byte("k"))
	if string(v) != "original" {
		t.Fatalf("stored value = %q, want original — store did not copy", v)
	}
}

func TestStoreTTLExpiry(t *testing.T) {
	s := newStore()
	p := partition.For([]byte("k"))

	// An entry whose expiry is already in the past reads as absent...
	s.put(p, "m", []byte("k"), []byte("v"), nowNano()-1)
	if _, ok := s.get(p, "m", []byte("k")); ok {
		t.Fatal("expired entry returned from get")
	}
	// ...and is reclaimed by the sweeper.
	if n := s.sweepExpired(); n != 1 {
		t.Fatalf("sweepExpired = %d, want 1", n)
	}
	if c := s.entryCount(); c != 0 {
		t.Fatalf("entryCount after sweep = %d, want 0", c)
	}

	// An entry with a future expiry is live.
	s.put(p, "m", []byte("k"), []byte("v"), nowNano()+int64(time.Hour))
	if _, ok := s.get(p, "m", []byte("k")); !ok {
		t.Fatal("live entry not found")
	}
	if n := s.sweepExpired(); n != 0 {
		t.Fatalf("sweepExpired removed a live entry: %d", n)
	}
}

func TestStoreNamespacesByMap(t *testing.T) {
	s := newStore()
	p := partition.For([]byte("k"))
	s.put(p, "users", []byte("k"), []byte("u"), 0)
	s.put(p, "sessions", []byte("k"), []byte("s"), 0)

	if v, _ := s.get(p, "users", []byte("k")); string(v) != "u" {
		t.Errorf("users[k] = %q, want u", v)
	}
	if v, _ := s.get(p, "sessions", []byte("k")); string(v) != "s" {
		t.Errorf("sessions[k] = %q, want s", v)
	}
}
