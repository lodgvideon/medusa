package transport

import (
	"encoding/binary"
	"errors"
	"io"

	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
)

const headerSize = 5 // 4-byte length + 1-byte type

// MaxFrameSize caps a single payload, guarding against a corrupt or hostile
// length prefix that would otherwise drive an unbounded allocation.
const MaxFrameSize = 64 << 20 // 64 MiB

// ErrFrameTooLarge is returned when a frame's declared length exceeds MaxFrameSize.
var ErrFrameTooLarge = errors.New("medusa/transport: frame exceeds MaxFrameSize")

// writeFrame writes a framed message to w. hdr is caller-owned scratch of at
// least headerSize bytes, reused across calls so writeFrame allocates nothing.
func writeFrame(w io.Writer, hdr []byte, t medusav1.MessageType, payload []byte) error {
	binary.BigEndian.PutUint32(hdr, uint32(len(payload)))
	hdr[headerSize-1] = byte(t)
	if _, err := w.Write(hdr[:headerSize]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

// readFrame reads one framed message from r. hdr is caller-owned scratch of at
// least headerSize bytes. The payload is read into buf, which is grown only
// when the incoming frame does not fit; the (possibly reallocated) buffer is
// returned so the caller can keep reusing it.
func readFrame(r io.Reader, hdr, buf []byte) (medusav1.MessageType, []byte, error) {
	if _, err := io.ReadFull(r, hdr[:headerSize]); err != nil {
		return 0, buf, err
	}
	n := binary.BigEndian.Uint32(hdr[:4])
	if n > MaxFrameSize {
		return 0, buf, ErrFrameTooLarge
	}
	t := medusav1.MessageType(hdr[headerSize-1])
	if cap(buf) < int(n) {
		buf = make([]byte, n)
	} else {
		buf = buf[:n]
	}
	if n > 0 {
		if _, err := io.ReadFull(r, buf); err != nil {
			return t, buf, err
		}
	}
	return t, buf, nil
}
