// Package codec provides zero-allocation helpers for encoding and decoding
// protobuf messages generated with vtprotobuf.
//
// The standard google.golang.org/protobuf Marshal allocates a fresh byte
// slice on every call. vtprotobuf instead exposes SizeVT and
// MarshalToSizedBufferVT, which let us marshal into a buffer we own and reuse.
// In steady state — once a reused buffer is large enough and a reused message
// struct already owns slices of sufficient capacity — the encode/decode hot
// path performs zero heap allocations.
package codec

import "sync"

// bufPool recycles scratch byte buffers for short-lived marshal/transport work.
// Buffers are stored by pointer so returning one does not allocate a slice header.
var bufPool = sync.Pool{New: func() any { b := make([]byte, 0, 256); return &b }}

// GetBuf borrows a length-zero byte buffer from the shared pool.
func GetBuf() *[]byte { return bufPool.Get().(*[]byte) }

// PutBuf returns b to the pool. The caller must not use b afterwards.
func PutBuf(b *[]byte) {
	*b = (*b)[:0]
	bufPool.Put(b)
}

// Marshaler is implemented by every vtprotobuf-generated message.
type Marshaler interface {
	SizeVT() int
	MarshalToSizedBufferVT([]byte) (int, error)
}

// Unmarshaler is implemented by every vtprotobuf-generated message.
type Unmarshaler interface {
	UnmarshalVT([]byte) error
}

// Marshal encodes m into dst, reusing dst's backing array when it is large
// enough. The returned slice aliases dst's storage, so a caller that needs to
// retain the bytes past the next Marshal into the same buffer must copy them.
//
// MarshalToSizedBufferVT fills the buffer from the end; passing a buffer whose
// length equals the message size makes the message occupy the whole buffer.
func Marshal(dst []byte, m Marshaler) ([]byte, error) {
	size := m.SizeVT()
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	if _, err := m.MarshalToSizedBufferVT(dst); err != nil {
		return nil, err
	}
	return dst, nil
}
