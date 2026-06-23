package conn

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// ---------------------------------------------------------------------------
// Server Push (RFC 7540 §8.2)
//
// The server pushes a promised response by sending PUSH_PROMISE on the
// client-initiated stream, then sends the promised response on a new
// server-initiated stream with an even ID (2, 4, 6, ...).
//
// Requirements:
//   - Client must have SETTINGS_ENABLE_PUSH = 1 (default).
//   - PUSH_PROMISE must be sent before any response headers on the parent stream.
//   - Promised stream ID must be even, server-initiated, monotonically increasing.
//   - Server must not push in response to a push response (no recursive push).
// ---------------------------------------------------------------------------

// ErrPushDisabled is returned when the client has disabled push via
// SETTINGS_ENABLE_PUSH = 0.
var ErrPushDisabled = errors.New("poseidon: server push disabled by client")

// ErrPushAfterResponse is returned when attempting to push after the
// parent stream has already sent response headers (RFC 7540 §8.2.1).
var ErrPushAfterResponse = errors.New("poseidon: push promise after response headers sent")

// pushIDCounter generates even-numbered stream IDs for server-initiated
// push streams. Must start at 2 (the lowest valid server stream ID).
type pushIDCounter struct {
	v atomic.Uint32
}

func newPushIDCounter() *pushIDCounter {
	c := &pushIDCounter{}
	c.v.Store(2) // first push stream ID
	return c
}

// next returns the next even stream ID (2, 4, 6, ...).
func (c *pushIDCounter) next() uint32 {
	for {
		old := c.v.Load()
		nextID := old + 2
		if c.v.CompareAndSwap(old, nextID) {
			return old
		}
	}
}

// isPushEnabled checks whether the peer has enabled push.
func (sc *ServerConn) isPushEnabled() bool {
	sc.psMu.RLock()
	defer sc.psMu.RUnlock()
	v := settingValue(sc.peerSettings, frame.SettingEnablePush, 1) // default: enabled
	return v == 1
}

// writePushPromise encodes headers and writes a PUSH_PROMISE frame on the
// parent stream, then creates the promised push stream.
//
// The caller must hold the parent stream's trust (i.e. this is called
// from Push on ServerStream).
func (sc *ServerConn) writePushPromise(_ context.Context, parent *ServerStream, promisedID uint32, fields []hpack.HeaderField) (*ServerStream, error) {
	if sc.closed.Load() {
		return nil, ErrConnClosed
	}

	sc.wmu.Lock()
	defer sc.wmu.Unlock()

	// Encode the header block.
	buf := encBufPool.Get().(*[]byte)
	*buf = (*buf)[:0]
	block := sc.enc.EncodeBlock(*buf, fields)

	// Write PUSH_PROMISE on the parent stream.
	err := sc.fr.WritePushPromise(parent.id, promisedID, block, true, 0)

	*buf = block[:0]
	encBufPool.Put(buf)
	if err != nil {
		return nil, err
	}

	// Create the promised stream.
	pushStream := &ServerStream{
		id:     promisedID,
		sc:     sc,
		events: make(chan StreamEvent, 4),
	}

	// Seed push stream send window from peer's INITIAL_WINDOW_SIZE.
	sc.psMu.RLock()
	initial := settingValue(sc.peerSettings, frame.SettingInitialWindowSize, connInitialRecvWindow)
	sc.psMu.RUnlock()
	pushStream.mu.Lock()
	pushStream.sendWindow = int32(initial) //nolint:gosec // G115: INITIAL_WINDOW_SIZE ≤ 2^31-1 per RFC
	pushStream.mu.Unlock()

	// Register the push stream. Bind a per-stream context (like registerStream)
	// so markStreamDone cancels a pushed response's handler on push-stream reset
	// or connection close.
	sc.smu.Lock()
	pushStream.ctx, pushStream.cancel = context.WithCancel(sc.connCtx)
	sc.streams[promisedID] = pushStream
	sc.smu.Unlock()

	sc.bumpFramesSent()

	return pushStream, nil
}

// Push sends a PUSH_PROMISE on the parent stream and returns the promised
// stream for writing the pushed response.
//
// Requirements per RFC 7540 §8.2:
//   - Client must allow push (SETTINGS_ENABLE_PUSH=1).
//   - Must be called BEFORE response headers are sent on the parent stream.
//   - promisedID must be server-initiated (even).
//
// The returned ServerStream can be used to write headers and data for the
// pushed response. It is the caller's responsibility to close it.
func (ss *ServerStream) Push(ctx context.Context, promiseHeaders []hpack.HeaderField) (*ServerStream, error) {
	// Check push enabled.
	if !ss.sc.isPushEnabled() {
		return nil, ErrPushDisabled
	}

	// RFC 7540 §8.2.1: PUSH_PROMISE must come before response headers.
	ss.mu.Lock()
	if ss.headersSent {
		ss.mu.Unlock()
		return nil, ErrPushAfterResponse
	}
	ss.mu.Unlock()

	// Allocate even stream ID.
	promisedID := ss.sc.pushIDs.next()

	// Write PUSH_PROMISE and create the push stream.
	return ss.sc.writePushPromise(ctx, ss, promisedID, promiseHeaders)
}

// PushWithPriority is like Push but also stores an RFC 7540 §5.3
// priority payload on the returned pushed ServerStream. The priority
// is emitted in the first response HEADERS frame on the push stream
// (via SendHeadersWithPriority). The PUSH_PROMISE frame itself does
// not carry the priority block — that is the canonical use of
// §5.3: the server signals priority when it actually starts sending
// the pushed response.
//
// Pass nil to leave the push stream without priority (equivalent to Push).
func (ss *ServerStream) PushWithPriority(ctx context.Context, promiseHeaders []hpack.HeaderField, prio *frame.Priority) (*ServerStream, error) {
	ps, err := ss.Push(ctx, promiseHeaders)
	if err != nil {
		return nil, err
	}
	ps.setPriority(prio)
	return ps, nil
}
