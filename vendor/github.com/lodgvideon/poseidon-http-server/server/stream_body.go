package server

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/lodgvideon/poseidon-http-server/conn"
)

// ErrBodyTooLarge is returned by a streaming Request.BodyReader once the total
// number of bytes read would exceed Options.MaxRequestBodyBytes. It mirrors the
// 413 (Request Entity Too Large) rejection used in buffered mode, surfaced to
// the handler through the reader so it can abort without OOM.
var ErrBodyTooLarge = errors.New("server: request body exceeds configured limit")

// ---------------------------------------------------------------------------
// streamBody — io.ReadCloser backed by ServerStream events
// ---------------------------------------------------------------------------

// streamBody implements io.ReadCloser by reading DATA frames from a
// ServerStream on demand. This avoids buffering the entire request body
// in memory.
type streamBody struct {
	stream *conn.ServerStream
	ctx    context.Context

	// limit caps the total number of body bytes that may be read: >0 is an
	// explicit cap, -1 means unlimited. read accumulates into total and
	// returns ErrBodyTooLarge once total would exceed limit.
	limit int64
	total int64

	mu   sync.Mutex
	buf  []byte // leftover data from last event
	done bool   // true after EndStream or Reset
}

// newStreamBody creates a streaming body reader with the given byte limit
// (>0 explicit cap, -1 unlimited). The first call to Read will consume the
// HEADERS event (if not already consumed) and then read DATA frames lazily.
func newStreamBody(ctx context.Context, stream *conn.ServerStream, limit int64) *streamBody {
	return &streamBody{
		stream: stream,
		ctx:    ctx,
		limit:  limit,
	}
}

// Read reads request body bytes. Blocks until data is available, EOF,
// or the context is cancelled.
func (sb *streamBody) Read(p []byte) (int, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	// Drain buffered data first.
	if len(sb.buf) > 0 {
		n := copy(p, sb.buf)
		sb.buf = sb.buf[n:]
		return n, nil
	}

	if sb.done {
		return 0, io.EOF
	}

	// Read next event from stream.
	for {
		ev, err := sb.stream.Recv(sb.ctx)
		if err != nil {
			sb.done = true
			return 0, err
		}

		switch ev.Type {
		case conn.EventHeaders:
			// Skip — headers already consumed by serveStream.
			if ev.EndStream {
				sb.done = true
				return 0, io.EOF
			}

		case conn.EventData:
			if len(ev.Data) == 0 {
				if ev.EndStream {
					sb.done = true
					return 0, io.EOF
				}
				continue
			}
			return sb.deliverData(p, ev.Data, ev.EndStream)

		case conn.EventTrailers:
			sb.done = true
			return 0, io.EOF

		case conn.EventReset:
			sb.done = true
			return 0, io.ErrUnexpectedEOF
		}
	}
}

// deliverData enforces the body-size cap, copies a DATA payload into p,
// buffers any remainder, and updates done state. Caller holds sb.mu.
func (sb *streamBody) deliverData(p, data []byte, endStream bool) (int, error) {
	// Enforce the body-size cap as bytes enter the reader. We check against
	// the total received (not just delivered) so a single over-cap DATA frame
	// is rejected without being handed to the caller — bounding both delivered
	// bytes and the leftover buffer.
	if sb.limit >= 0 {
		sb.total += int64(len(data))
		if sb.total > sb.limit {
			sb.done = true
			return 0, ErrBodyTooLarge
		}
	}

	n := copy(p, data)
	if n < len(data) {
		sb.buf = append(sb.buf, data[n:]...)
	}
	if endStream {
		sb.done = true
		if n == 0 {
			return 0, io.EOF
		}
	}
	return n, nil
}

// Close marks the body as done. Subsequent reads return EOF.
func (sb *streamBody) Close() error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.done = true
	return nil
}
