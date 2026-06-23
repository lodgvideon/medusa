package conn

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// ServerStream is a single server-side HTTP/2 stream.
// Single-goroutine: the handler owns the stream after AcceptStream.
type ServerStream struct {
	id     uint32
	sc     *ServerConn
	events chan StreamEvent

	// ctx is cancelled when the stream is reset by the client, completes, or its
	// connection closes. Set by registerStream; nil for unregistered or pushed
	// streams (Context() then falls back to context.Background()).
	ctx    context.Context
	cancel context.CancelFunc

	mu            sync.Mutex
	localEnded    bool
	remoteEnded   bool
	closed        bool
	headersSent   bool
	headersReceived bool
	// priority stores the RFC 7540 §5.3 priority payload received in
	// the first HEADERS frame (or set by PushWithPriority), if any.
	// nil if no priority was specified. Accessed atomically: written
	// once by the reader goroutine (OnHeaders or PushWithPriority),
	// read by handler goroutines.
	priority atomic.Pointer[frame.Priority]

	// Flow control.
	recvWindow         int32
	recvRefundPending  uint32
	sendWindow         int32
}

// Priority returns the RFC 7540 §5.3 priority payload extracted from
// the request's first HEADERS frame, or nil if the client did not
// include a priority block. Useful for handlers that want to propagate
// the client's priority hint into the response HEADERS, or into a
// server-pushed PUSH_PROMISE.
func (ss *ServerStream) Priority() *frame.Priority { return ss.priority.Load() }

// setPriority stores the request's priority payload. Called by
// serverConnHandler.OnHeaders exactly once, on the first HEADERS frame
// of the stream, or by PushWithPriority for pushed streams.
func (ss *ServerStream) setPriority(p *frame.Priority) {
	if p == nil {
		return
	}
	ss.priority.Store(p)
}

// StreamEventType discriminates the StreamEvent variants.
type StreamEventType uint8

const (
	EventHeaders  StreamEventType = iota + 1
	EventData
	EventTrailers
	EventReset
)

// String returns the name of t.
func (t StreamEventType) String() string {
	switch t {
	case EventHeaders:
		return "headers"
	case EventData:
		return "data"
	case EventTrailers:
		return "trailers"
	case EventReset:
		return "reset"
	default:
		return "unknown"
	}
}

// StreamEvent is one observation about an in-flight stream.
type StreamEvent struct {
	Type      StreamEventType
	Headers   []hpack.HeaderField
	Data      []byte
	EndStream bool
	RSTCode   frame.ErrCode
}

// ID returns the HTTP/2 stream identifier.
func (ss *ServerStream) ID() uint32 { return ss.id }

// Context returns a context that is cancelled when the stream is reset by the
// client (RST_STREAM), completes, or the underlying connection closes. Handlers
// should select on its Done channel (or pass it to blocking calls) to abort
// work promptly when the client goes away. It is never nil and is safe to call
// on a nil stream (returns a background context) so constructors that tolerate
// a nil stream — e.g. server.NewResponseWriter(nil) in tests — do not panic.
func (ss *ServerStream) Context() context.Context {
	if ss == nil || ss.ctx == nil {
		return context.Background()
	}
	return ss.ctx
}

// SendHeaders sends a response HEADERS frame with the given fields.
// The first call on a stream seeds the per-stream send window from
// the peer's SETTINGS_INITIAL_WINDOW_SIZE. Always sets END_HEADERS.
func (ss *ServerStream) SendHeaders(ctx context.Context, fields []hpack.HeaderField, endStream bool) error {
	return ss.SendHeadersWithPriority(ctx, fields, endStream, nil)
}

// SendHeadersWithPriority is like SendHeaders but embeds an RFC 7540
// §5.3 priority block (E + StreamDep + Weight) into the first HEADERS
// frame via the PRIORITY flag. Pass nil to omit the priority block
// (equivalent to SendHeaders).
func (ss *ServerStream) SendHeadersWithPriority(ctx context.Context, fields []hpack.HeaderField, endStream bool, prio *frame.Priority) error {
	ss.mu.Lock()
	if ss.closed || ss.localEnded {
		ss.mu.Unlock()
		return ErrStreamClosed
	}
	ss.mu.Unlock()
	if err := ss.sc.writeServerHeaders(ctx, ss, fields, endStream, prio); err != nil {
		return err
	}
	ss.mu.Lock()
	ss.headersSent = true
	ss.mu.Unlock()
	if endStream && ss.markLocalEnd() {
		// Fully closed (both halves ended): release the stream.
		ss.sc.markStreamDone(ss.id)
	}
	return nil
}

// SendData sends a DATA frame, automatically chunking to the peer's
// MAX_FRAME_SIZE and respecting both per-stream and connection-level
// outbound flow control (RFC 7540 §6.9). Blocks until enough send-window
// credit is available.
func (ss *ServerStream) SendData(ctx context.Context, p []byte, endStream bool) error {
	ss.mu.Lock()
	if ss.closed || ss.localEnded {
		ss.mu.Unlock()
		return ErrStreamClosed
	}
	ss.mu.Unlock()
	if err := ss.sc.writeServerData(ctx, ss, p, endStream); err != nil {
		return err
	}
	if endStream && ss.markLocalEnd() {
		// Fully closed (both halves ended): release the stream.
		ss.sc.markStreamDone(ss.id)
	}
	return nil
}

// Recv blocks until the next event is ready. A buffered event is always returned
// in preference to context cancellation, so a final event delivered in the same
// step as the stream's completion or reset (markStreamDone cancels the context)
// is never dropped.
func (ss *ServerStream) Recv(ctx context.Context) (StreamEvent, error) {
	select {
	case e, ok := <-ss.events:
		if !ok {
			return StreamEvent{}, ErrStreamClosed
		}
		return e, nil
	default:
	}
	select {
	case e, ok := <-ss.events:
		if !ok {
			return StreamEvent{}, ErrStreamClosed
		}
		return e, nil
	case <-ctx.Done():
		return StreamEvent{}, ctx.Err()
	}
}

// Close sends RST_STREAM(CANCEL) if neither side has ended. Idempotent.
func (ss *ServerStream) Close() error {
	ss.mu.Lock()
	already := ss.closed
	bothEnded := ss.localEnded && ss.remoteEnded
	ss.closed = true
	ss.mu.Unlock()
	if already {
		return nil
	}
	if bothEnded {
		return nil
	}
	return ss.sc.writeServerRSTStream(ss, frame.ErrCodeCancel)
}

// markRemoteEnd marks the remote side as closed.
// markRemoteEnd records that the client has ended its half of the stream
// (END_STREAM received). It returns true if the stream is now fully closed
// (both the remote and local halves are done), so the caller can release it.
// While the stream is only half-closed (remote), it MUST stay registered: the
// server may still be writing its response and must keep receiving that stream's
// WINDOW_UPDATE / RST_STREAM (RFC 7540 §5.1).
func (ss *ServerStream) markRemoteEnd() bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.remoteEnded = true
	return ss.localEnded
}

// markLocalEnd records that the server has ended its half of the stream (sent
// END_STREAM). It returns true if the stream is now fully closed.
func (ss *ServerStream) markLocalEnd() bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.localEnded = true
	return ss.remoteEnded
}

// markRemoteEndReset records a client RST_STREAM as ending the remote half and
// reports whether the request was still open (END_STREAM not yet observed) at
// that moment — computed atomically under ss.mu so the rapid-reset
// classification (CVE-2023-44487) cannot race the flag write. RST is a hard
// close, so the caller releases the stream unconditionally afterwards.
func (ss *ServerStream) markRemoteEndReset() (wasOpen bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	wasOpen = !ss.remoteEnded
	ss.remoteEnded = true
	return wasOpen
}

// push delivers an event from the reader goroutine. Non-blocking;
// on overflow marks stream closed and sends RST.
func (ss *ServerStream) push(e StreamEvent) {
	select {
	case ss.events <- e:
		return
	default:
	}
	ss.mu.Lock()
	already := ss.closed
	ss.closed = true
	ss.mu.Unlock()
	if already {
		return
	}
	go func() {
		_ = ss.sc.writeServerRSTStream(ss, frame.ErrCodeRefusedStream)
	}()
	select {
	case ss.events <- StreamEvent{Type: EventReset, RSTCode: frame.ErrCodeRefusedStream, EndStream: true}:
	default:
	}
}

func newServerStream(id uint32, eventBuf int, sc *ServerConn, recvWindow int32) *ServerStream {
	return &ServerStream{
		id:         id,
		sc:         sc,
		events:     make(chan StreamEvent, eventBuf),
		recvWindow: recvWindow,
	}
}
