package codec_test

import (
	"bytes"
	"testing"

	"github.com/lodgvideon/medusa/codec"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
)

func TestMarshalRoundTrip(t *testing.T) {
	src := &medusav1.GetResponse{Found: true, Value: []byte("the-quick-brown-fox")}

	wire, err := codec.Marshal(nil, src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got := &medusav1.GetResponse{}
	if err := got.UnmarshalVT(wire); err != nil {
		t.Fatalf("UnmarshalVT: %v", err)
	}
	if !got.Found {
		t.Errorf("Found = false, want true")
	}
	if !bytes.Equal(got.Value, src.Value) {
		t.Errorf("Value = %q, want %q", got.Value, src.Value)
	}
}

func TestMarshalReusesBuffer(t *testing.T) {
	src := &medusav1.GetResponse{Found: true, Value: []byte("hello")}

	buf := make([]byte, 0, 64)
	out, err := codec.Marshal(buf, src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// The returned slice must alias the pre-allocated backing array, not a new one.
	if &out[:1][0] != &buf[:1][0] {
		t.Errorf("Marshal did not reuse the provided buffer's backing array")
	}
}

func TestMarshalGrowsWhenTooSmall(t *testing.T) {
	src := &medusav1.GetResponse{Found: true, Value: bytes.Repeat([]byte("x"), 1024)}

	out, err := codec.Marshal(make([]byte, 0, 8), src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &medusav1.GetResponse{}
	if err := got.UnmarshalVT(out); err != nil {
		t.Fatalf("UnmarshalVT: %v", err)
	}
	if len(got.Value) != 1024 {
		t.Errorf("Value len = %d, want 1024", len(got.Value))
	}
}

// TestMarshalZeroAlloc enforces the core promise of the framework: marshaling
// into a warm buffer must not allocate. AllocsPerRun fails the build if a
// future change reintroduces an allocation on the encode path.
func TestMarshalZeroAlloc(t *testing.T) {
	src := &medusav1.GetResponse{Found: true, Value: []byte("a-reasonably-sized-cache-value-payload")}
	buf := make([]byte, 0, 128) // warm: capacity already exceeds the message size

	allocs := testing.AllocsPerRun(1000, func() {
		buf, _ = codec.Marshal(buf, src)
	})
	if allocs != 0 {
		t.Fatalf("Marshal allocated %v allocs/op, want 0", allocs)
	}
}

// TestUnmarshalZeroAlloc proves the read hot path: decoding a value-carrying
// response into a reused struct reuses the value slice's capacity and does not
// allocate.
func TestUnmarshalZeroAlloc(t *testing.T) {
	src := &medusav1.GetResponse{Found: true, Value: []byte("a-reasonably-sized-cache-value-payload")}
	wire, err := codec.Marshal(nil, src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	dst := &medusav1.GetResponse{}
	_ = dst.UnmarshalVT(wire) // warm: allocate the Value slice once

	allocs := testing.AllocsPerRun(1000, func() {
		_ = dst.UnmarshalVT(wire)
	})
	if allocs != 0 {
		t.Fatalf("UnmarshalVT allocated %v allocs/op, want 0", allocs)
	}
}

func TestBufferPoolResetsOnReturn(t *testing.T) {
	b := codec.GetBuf()
	if len(*b) != 0 {
		t.Fatalf("borrowed buffer length = %d, want 0", len(*b))
	}
	*b = append(*b, "some-data"...)
	codec.PutBuf(b)

	b2 := codec.GetBuf()
	if len(*b2) != 0 {
		t.Fatalf("recycled buffer length = %d, want 0 (not reset)", len(*b2))
	}
	codec.PutBuf(b2)
}

func TestMarshalIntoBorrowedBuffer(t *testing.T) {
	src := &medusav1.PutResponse{Created: true}
	b := codec.GetBuf()
	defer codec.PutBuf(b)

	out, err := codec.Marshal((*b)[:0], src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	*b = out

	got := &medusav1.PutResponse{}
	if err := got.UnmarshalVT(out); err != nil {
		t.Fatalf("UnmarshalVT: %v", err)
	}
	if !got.Created {
		t.Error("round trip through a borrowed buffer lost data")
	}
}

func BenchmarkMarshal(b *testing.B) {
	src := &medusav1.GetResponse{Found: true, Value: []byte("a-reasonably-sized-cache-value-payload")}
	buf := make([]byte, 0, 128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ = codec.Marshal(buf, src)
	}
}
