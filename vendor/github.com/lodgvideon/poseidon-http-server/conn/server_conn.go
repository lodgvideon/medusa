package conn

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// ServerConn manages a single server-side HTTP/2 connection.
// Goroutine-safe for AcceptStream and Close; per-stream methods
// are single-goroutine.
type ServerConn struct {
	transport net.Conn
	fr        *frame.Framer
	enc       *hpack.Encoder
	dec       *hpack.Decoder
	opts      ServerConnOptions

	// peerSettings is the most recently observed client SETTINGS.
	// Guarded by psMu.
	psMu         sync.RWMutex
	peerSettings frame.SettingsParams

	wmu sync.Mutex // serializes all writes to fr

	// smu guards streams map and stream ID tracking.
	smu     sync.Mutex
	streams map[uint32]*ServerStream

	// fcMu guards the connection-level recv window.
	fcMu              sync.Mutex
	connRecvWindow    int32
	connRefundPending uint32

	// fcOutMu guards the outbound connection-level send window.
	fcOutMu            sync.Mutex
	fcOutCond          *sync.Cond
	peerConnSendWindow int32

	closed     atomic.Bool
	readerDone chan struct{}

	// handler is the single frame.Handler used for BOTH the SETTINGS handshake
	// and the reader loop, so a header block split across that boundary keeps
	// its pending CONTINUATION state instead of being lost.
	handler *serverConnHandler

	// connCtx is cancelled when the connection closes (or its parent ctx is
	// cancelled). Per-stream contexts derive from it, so closing the connection
	// cancels every in-flight handler.
	connCtx    context.Context
	connCancel context.CancelFunc

	// acceptCh delivers new client-initiated streams to AcceptStream.
	acceptCh chan *ServerStream

	// pingMu guards pingWaiters. pingCounter produces unique payloads.
	pingMu      sync.Mutex
	pingWaiters map[[8]byte]chan struct{}
	pingCounter atomic.Uint64

	// goAwayRequested flags that the server has initiated GOAWAY.
	goAwayRequested atomic.Bool

	// rapidResetCount accumulates client-initiated RST_STREAM frames that
	// reset a stream before it produced useful work (CVE-2023-44487).
	// Compared against opts.rapidResetBudget(). Atomic: incremented by the
	// single reader goroutine, read on the same path; no allocation.
	rapidResetCount atomic.Uint32

	// maxClientStreamID tracks the highest client-initiated (odd) stream
	// ID ever registered, so GOAWAY reports the correct last-processed
	// stream ID (RFC 7540 §6.8) even after the stream has been torn down
	// and removed from the streams map.
	maxClientStreamID atomic.Uint32

	// pushIDs generates even stream IDs for server push (RFC 7540 §8.2).
	pushIDs *pushIDCounter

	// Stats counters.
	atomicBytesSent       atomic.Int64
	atomicBytesReceived   atomic.Int64
	atomicFramesSent      atomic.Int64
	atomicFramesReceived  atomic.Int64
	atomicStreamsAccepted atomic.Uint32
}

// ServerConnOptions configures the server-side connection.
type ServerConnOptions struct {
	// AdvertisedSettings are sent in the server's SETTINGS frame.
	AdvertisedSettings AdvertisedSettings
	// StreamEventBuffer is the per-stream event channel capacity.
	StreamEventBuffer int
	// KeepaliveInterval, when non-zero, enables a background keepalive
	// loop. Zero disables keepalive.
	KeepaliveInterval time.Duration
	// KeepaliveTimeout is the max time to wait for PING ACK before
	// closing the connection. Defaults to max(interval*5, 5s).
	KeepaliveTimeout time.Duration
	// MaxRapidResets bounds the number of client-initiated RST_STREAM
	// frames that reset a stream before it produced useful work, before
	// the server treats the connection as a Rapid Reset flood
	// (CVE-2023-44487) and tears it down with GOAWAY(ENHANCE_YOUR_CALM).
	//
	//   0  => secure default: max(MaxConcurrentStreams*4, rapidResetFloor)
	//   <0 => mitigation disabled
	//   >0 => explicit budget
	//
	// Mirrors the Go x/net/http2 fix philosophy: secure-by-default, with
	// a budget proportional to the advertised stream concurrency so that
	// legitimate cancellations never trip it under normal operation.
	MaxRapidResets int

	// HandshakeTimeout bounds the time allowed to complete the HTTP/2
	// connection preface + SETTINGS exchange (RFC 7540 §3.5). A client that
	// connects but trickles the preface/SETTINGS slowly is dropped once the
	// deadline elapses — a Slowloris defense at the connection-establishment
	// stage. The deadline is cleared once the handshake completes, so it does
	// NOT act as a blanket connection read deadline and never interferes with
	// long-lived, multiplexed HTTP/2 keep-alive traffic.
	//
	//   0  => secure default (defaultHandshakeTimeout)
	//   <0 => disabled
	//   >0 => explicit timeout
	HandshakeTimeout time.Duration
}

// defaultHandshakeTimeout is the secure-by-default bound on completing the
// HTTP/2 handshake. Generous enough for high-latency clients yet short enough
// to shed Slowloris-style connections that never finish the preface.
const defaultHandshakeTimeout = 10 * time.Second

// rapidResetFloor is the minimum Rapid Reset budget regardless of how
// small MaxConcurrentStreams is, so low-concurrency configs still tolerate
// a reasonable burst of legitimate cancellations.
const rapidResetFloor = 100

func (o ServerConnOptions) defaulted() ServerConnOptions {
	// Always fully default the advertised settings (idempotent: only zero fields
	// change). Gating this on MaxConcurrentStreams==0 used to leave
	// InitialWindowSize at 0 when a caller set only MaxConcurrentStreams — which
	// then advertised a 0 recv window and deadlocked every request body.
	o.AdvertisedSettings = o.AdvertisedSettings.defaulted()
	if o.StreamEventBuffer <= 0 {
		o.StreamEventBuffer = 8
	}
	if o.MaxRapidResets == 0 {
		budget := int(o.AdvertisedSettings.MaxConcurrentStreams) * 4
		if budget < rapidResetFloor {
			budget = rapidResetFloor
		}
		o.MaxRapidResets = budget
	}
	if o.HandshakeTimeout == 0 {
		o.HandshakeTimeout = defaultHandshakeTimeout
	}
	return o
}

// rapidResetBudget returns the configured Rapid Reset budget, or 0 if the
// mitigation is disabled (negative MaxRapidResets).
func (o ServerConnOptions) rapidResetBudget() int {
	if o.MaxRapidResets < 0 {
		return 0
	}
	return o.MaxRapidResets
}

// ConnStats is a point-in-time counter snapshot.
//
//nolint:revive // exported stutters with package; kept for API consistency with client.
type ConnStats struct {
	BytesSent       int64
	BytesReceived   int64
	FramesSent      int64
	FramesReceived  int64
	StreamsAccepted uint32
	// RapidResets counts client RST_STREAM frames charged against the
	// Rapid Reset budget (CVE-2023-44487) on this connection.
	RapidResets uint32
	// GoAwaySent reports whether this connection has emitted a GOAWAY
	// (graceful or error). A connection sends at most one GOAWAY.
	GoAwaySent bool
}

// clientPreface is the HTTP/2 connection preface sent by clients
// (RFC 7540 §3.5).
var clientPreface = []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")

// NewServerConn performs the HTTP/2 server-side handshake over an
// already-connected transport (typically a *tls.Conn or net.Conn for h2c):
//
//  1. Read the 24-byte client preface magic (RFC 7540 §3.5)
//  2. Send server SETTINGS frame with advertised settings
//  3. Read client SETTINGS frame
//  4. Send SETTINGS ACK for client SETTINGS
//  5. Read client SETTINGS ACK for our SETTINGS
//  6. Start reader goroutine
//
// Returns ErrBadPreface if the client preface is invalid.
func NewServerConn(ctx context.Context, nc net.Conn, opts ServerConnOptions) (*ServerConn, error) {
	opts = opts.defaulted()

	// Slowloris defense: bound the handshake (preface + SETTINGS exchange)
	// with a read deadline. A client that connects but never finishes the
	// preface is dropped here instead of pinning a goroutine forever. The
	// deadline is cleared on success so it does NOT linger as a blanket
	// connection read deadline that would break HTTP/2 keep-alive.
	if opts.HandshakeTimeout > 0 {
		if dl, ok := nc.(interface{ SetReadDeadline(time.Time) error }); ok {
			_ = dl.SetReadDeadline(time.Now().Add(opts.HandshakeTimeout))
		}
	}

	// Step 1: read and verify client preface.
	if err := readClientPreface(nc); err != nil {
		_ = nc.Close()
		return nil, err
	}

	sc := &ServerConn{
		transport:          nc,
		enc:                hpack.NewEncoder(),
		dec:                hpack.NewDecoder(),
		opts:               opts,
		streams:            map[uint32]*ServerStream{},
		readerDone:         make(chan struct{}),
		acceptCh:           make(chan *ServerStream, 64),
		pingWaiters:        make(map[[8]byte]chan struct{}),
		connRecvWindow:     int32(connInitialRecvWindow),
		peerConnSendWindow: int32(connInitialRecvWindow),
		pushIDs:            newPushIDCounter(),
	}
	// Wrap the transport so every frame byte read/written is tallied for Stats
	// (BytesReceived/BytesSent). The 24-byte preface read above is not counted;
	// everything from the SETTINGS exchange onward is.
	sc.fr = frame.NewFramer(
		&countingWriter{w: nc, n: &sc.atomicBytesSent},
		&countingReader{r: nc, n: &sc.atomicBytesReceived},
	)
	sc.fcOutCond = sync.NewCond(&sc.fcOutMu)
	// Connection-lifetime context: cancelled on Close or when the caller's ctx
	// is cancelled. Per-stream contexts derive from this.
	sc.connCtx, sc.connCancel = context.WithCancel(ctx)

	// Step 2: send server SETTINGS.
	myParams := encodeAdvertised(opts.AdvertisedSettings)
	if err := sc.fr.WriteSettings(myParams); err != nil {
		_ = nc.Close()
		return nil, fmt.Errorf("server write settings: %w", err)
	}
	sc.atomicFramesSent.Add(1)

	// Steps 3-5: handshake — read client SETTINGS, send ACK, read ACK.
	// Create the real frame handler early so that non-SETTINGS frames
	// arriving during the handshake (e.g. HEADERS) are not lost.
	sc.handler = newServerConnHandler(sc, sc.dec, int(sc.opts.AdvertisedSettings.MaxHeaderListSize), int32(sc.opts.AdvertisedSettings.InitialWindowSize)) //nolint:gosec // G115: AdvertisedSettings.defaulted() clamps InitialWindowSize to ≤ 2^31-1
	peer, err := handshakeServerSettings(ctx, sc.fr, sc.handler)
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	// Validate the client's INITIAL SETTINGS (RFC 9113 §6.5.2). The handshake
	// path does not flow through applyPeerSettings, so a bad initial value — e.g.
	// an INITIAL_WINDOW_SIZE that would make every stream's send window negative —
	// must be rejected here, with a GOAWAY carrying the right error code.
	if verr := validatePeerSettings(peer); verr != nil {
		var ce connError
		if errors.As(verr, &ce) {
			sc.sendGoAway(ce.code)
		}
		_ = nc.Close()
		return nil, verr
	}
	sc.psMu.Lock()
	sc.peerSettings = peer
	sc.psMu.Unlock()
	sc.applyInitialPeerSettings(peer)

	// Handshake complete: clear the read deadline so the long-lived, multiplexed
	// connection is never killed by a stale handshake deadline. Post-handshake
	// liveness is intentionally NOT a blanket connection read deadline (that would
	// break keep-alive); it is governed by the per-stream idle timeout at the accept
	// layer (server.acceptLoop), which closes a connection left idle with no streams.
	// The brief window before readerLoop starts performs no reads, so there is no
	// slow-client vector there.
	if opts.HandshakeTimeout > 0 {
		if dl, ok := nc.(interface{ SetReadDeadline(time.Time) error }); ok {
			_ = dl.SetReadDeadline(time.Time{})
		}
	}

	go sc.readerLoop()
	if opts.KeepaliveInterval > 0 {
		go sc.keepaliveLoop(opts.KeepaliveInterval)
	}
	return sc, nil
}

// readClientPreface reads exactly 24 bytes and validates against
// the HTTP/2 client preface magic (RFC 7540 §3.5).
func readClientPreface(nc net.Conn) error {
	buf := make([]byte, len(clientPreface))
	if _, err := io.ReadFull(nc, buf); err != nil {
		return fmt.Errorf("read preface: %w", err)
	}
	for i, b := range buf {
		if b != clientPreface[i] {
			return ErrBadPreface
		}
	}
	return nil
}

// handshakeServerSettings runs the server-side SETTINGS exchange:
//
//   - Read client SETTINGS
//   - Send SETTINGS ACK
//   - Read client SETTINGS ACK for our SETTINGS
//
// Returns the client's SETTINGS.
func handshakeServerSettings(ctx context.Context, fr *frame.Framer, delegate frame.Handler) (frame.SettingsParams, error) {
	rec := &settingsRecorder{delegate: delegate}
	for !rec.peerSeen {
		if err := readOneFrame(ctx, fr, rec); err != nil {
			return frame.SettingsParams{}, fmt.Errorf("server read client settings: %w", err)
		}
	}
	if err := fr.WriteSettingsAck(); err != nil {
		return frame.SettingsParams{}, fmt.Errorf("server write settings ack: %w", err)
	}
	for !rec.ackSeen {
		if err := readOneFrame(ctx, fr, rec); err != nil {
			return frame.SettingsParams{}, fmt.Errorf("server read client ack: %w", err)
		}
	}
	return rec.peer, nil
}

func readOneFrame(ctx context.Context, fr *frame.Framer, h frame.Handler) error {
	_, err := fr.ReadFrame(ctx, h)
	return err
}

// AcceptStream blocks until a new client-initiated stream arrives
// (HEADERS frame on an idle stream ID). Returns the stream with
// initial headers ready to read via Recv.
func (sc *ServerConn) AcceptStream(ctx context.Context) (*ServerStream, error) {
	if sc.closed.Load() {
		return nil, ErrConnClosed
	}
	select {
	case ss, ok := <-sc.acceptCh:
		if !ok {
			return nil, ErrConnClosed
		}
		return ss, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close sends GOAWAY(NO_ERROR) and closes the underlying connection.
// Idempotent.
func (sc *ServerConn) Close() error {
	if !sc.closed.CompareAndSwap(false, true) {
		return nil
	}
	if sc.connCancel != nil {
		sc.connCancel() // cancel every in-flight handler's context
	}
	sc.fcOutMu.Lock()
	if sc.fcOutCond != nil {
		sc.fcOutCond.Broadcast()
	}
	sc.fcOutMu.Unlock()

	// Best-effort GOAWAY. The write deadline is set under wmu together with the
	// write so a concurrent teardown path (sendGoAway) cannot clobber the deadline
	// on the shared transport.
	sc.wmu.Lock()
	if dl, ok := sc.transport.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = dl.SetWriteDeadline(time.Now().Add(closeGoAwayDeadline))
	}
	// Account for the GOAWAY frame and flag the connection as having emitted one,
	// matching GoAway/sendGoAway, so Stats (FramesSent, GoAwaySent) stays accurate
	// for Close-initiated teardowns (e.g. keepalive timeout).
	if err := sc.fr.WriteGoAway(sc.lastPeerStreamID(), frame.ErrCodeNoError, nil); err == nil {
		sc.bumpFramesSent()
		sc.goAwayRequested.Store(true)
	}
	sc.wmu.Unlock()
	_ = sc.transport.Close()
	<-sc.readerDone
	sc.fr.Close()
	return nil
}

// Stats returns a point-in-time snapshot of connection counters.
func (sc *ServerConn) Stats() ConnStats {
	return ConnStats{
		BytesSent:       sc.atomicBytesSent.Load(),
		BytesReceived:   sc.atomicBytesReceived.Load(),
		FramesSent:      sc.atomicFramesSent.Load(),
		FramesReceived:  sc.atomicFramesReceived.Load(),
		StreamsAccepted: sc.atomicStreamsAccepted.Load(),
		RapidResets:     sc.rapidResetCount.Load(),
		GoAwaySent:      sc.goAwayRequested.Load(),
	}
}

// countingReader / countingWriter tally bytes flowing through the transport into
// an atomic counter, so Stats can report BytesReceived / BytesSent. They sit
// between the Framer and the raw connection; the byte count is updated even on a
// partial read/write that ends in error (n bytes still crossed the wire).
type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.n.Add(int64(n))
	}
	return n, err
}

type countingWriter struct {
	w io.Writer
	n *atomic.Int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.n.Add(int64(n))
	}
	return n, err
}

func (sc *ServerConn) lastPeerStreamID() uint32 {
	maxID := sc.maxClientStreamID.Load()
	sc.smu.Lock()
	for id := range sc.streams {
		if id > maxID {
			maxID = id
		}
	}
	sc.smu.Unlock()
	return maxID
}

// noteClientStreamID records id as the highest client-initiated stream ID
// seen, monotonically (CAS loop). Cheap and race-safe; no allocation.
func (sc *ServerConn) noteClientStreamID(id uint32) {
	for {
		cur := sc.maxClientStreamID.Load()
		if id <= cur {
			return
		}
		if sc.maxClientStreamID.CompareAndSwap(cur, id) {
			return
		}
	}
}

// validateClientStreamID enforces RFC 9113 §5.1.1 for a newly opened client
// stream: the ID must be odd and strictly greater than every client stream ID
// already seen. An even ID, or a reused/decreasing ID (idle-stream reuse), is a
// connection error of type PROTOCOL_ERROR. Called only for new streams, on the
// single reader goroutine, before registerStream advances maxClientStreamID.
func (sc *ServerConn) validateClientStreamID(id uint32) error {
	if id%2 == 0 {
		return connError{code: frame.ErrCodeProtocolError, msg: "client-initiated stream ID must be odd"}
	}
	if id <= sc.maxClientStreamID.Load() {
		return connError{code: frame.ErrCodeProtocolError, msg: "client stream ID must exceed all previous client streams"}
	}
	return nil
}

func (sc *ServerConn) applyInitialPeerSettings(peer frame.SettingsParams) {
	for i := range peer.N {
		p := peer.Pairs[i]
		if p.ID == frame.SettingHeaderTableSize {
			sc.enc.SetMaxDynamicTableSize(p.Value)
		}
	}
}

// IsAlive reports whether the connection is open.
func (sc *ServerConn) IsAlive() bool {
	return !sc.closed.Load()
}

// Ping sends a PING and blocks until the client's ACK arrives.
func (sc *ServerConn) Ping(ctx context.Context) (time.Duration, error) {
	if sc.closed.Load() {
		return 0, ErrConnClosed
	}
	n := sc.pingCounter.Add(1)
	var payload [8]byte
	//nolint:gosec // ping counter is monotonic, overflow is fine
	binary.BigEndian.PutUint64(payload[:], n)

	ch := make(chan struct{})
	sc.pingMu.Lock()
	sc.pingWaiters[payload] = ch
	sc.pingMu.Unlock()

	sc.wmu.Lock()
	if sc.closed.Load() {
		sc.wmu.Unlock()
		sc.pingMu.Lock()
		delete(sc.pingWaiters, payload)
		sc.pingMu.Unlock()
		return 0, ErrConnClosed
	}
	start := time.Now()
	err := sc.fr.WritePing(false, payload)
	if err == nil {
		sc.bumpFramesSent()
	}
	sc.wmu.Unlock()
	if err != nil {
		sc.pingMu.Lock()
		delete(sc.pingWaiters, payload)
		sc.pingMu.Unlock()
		return 0, err
	}

	select {
	case <-ch:
		return time.Since(start), nil
	case <-ctx.Done():
		sc.pingMu.Lock()
		delete(sc.pingWaiters, payload)
		sc.pingMu.Unlock()
		return 0, ctx.Err()
	case <-sc.readerDone:
		sc.pingMu.Lock()
		delete(sc.pingWaiters, payload)
		sc.pingMu.Unlock()
		return 0, ErrConnClosed
	}
}

// deliverPingAck signals any Ping call waiting for payload.
func (sc *ServerConn) deliverPingAck(payload [8]byte) {
	sc.pingMu.Lock()
	ch, ok := sc.pingWaiters[payload]
	if ok {
		delete(sc.pingWaiters, payload)
	}
	sc.pingMu.Unlock()
	if ok {
		close(ch)
	}
}

// GoAway sends GOAWAY with the given error code. After this call
// AcceptStream returns ErrConnClosed but existing streams continue.
func (sc *ServerConn) GoAway(code frame.ErrCode) error {
	if !sc.goAwayRequested.CompareAndSwap(false, true) {
		return nil // already sent
	}
	sc.wmu.Lock()
	defer sc.wmu.Unlock()
	if sc.closed.Load() {
		return ErrConnClosed
	}
	err := sc.fr.WriteGoAway(sc.lastPeerStreamID(), code, nil)
	if err == nil {
		sc.bumpFramesSent()
	}
	return err
}

func (sc *ServerConn) keepaliveLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			pingTimeout := sc.opts.KeepaliveTimeout
			if pingTimeout == 0 {
				pingTimeout = interval * 5
				if pingTimeout < 5*time.Second {
					pingTimeout = 5 * time.Second
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
			_, err := sc.Ping(ctx)
			cancel()
			if err != nil {
				_ = sc.Close()
				return
			}
		case <-sc.readerDone:
			_ = sc.Close()
			return
		}
	}
}

func (sc *ServerConn) bumpFramesSent() { sc.atomicFramesSent.Add(1) }

// readerLoop reads frames from the connection and dispatches them.
func (sc *ServerConn) readerLoop() {
	defer close(sc.readerDone)
	// Reuse the handshake handler so a header block split across the handshake
	// boundary keeps its pending CONTINUATION state instead of being lost.
	h := sc.handler
	for {
		_, err := sc.fr.ReadFrame(context.Background(), h)
		if err != nil {
			// A connError from a handler callback (e.g. PROTOCOL_ERROR or
			// the Rapid Reset ENHANCE_YOUR_CALM trip) is a connection-fatal
			// error: emit GOAWAY with its code before tearing down so the
			// peer learns why the connection is closing (RFC 7540 §6.8).
			var ce connError
			if errors.As(err, &ce) {
				sc.sendGoAway(ce.code)
			}
			sc.shutdownStreams(err)
			return
		}
		sc.atomicFramesReceived.Add(1)
	}
}

// sendGoAway emits a best-effort GOAWAY with the given error code and the
// last client stream ID we processed. Bounded by a short write deadline so
// a stuck transport cannot block connection teardown.
func (sc *ServerConn) sendGoAway(code frame.ErrCode) {
	if !sc.goAwayRequested.CompareAndSwap(false, true) {
		return
	}
	sc.wmu.Lock()
	// Deadline set under wmu with the write so it cannot race Close()'s deadline
	// on the shared transport.
	if dl, ok := sc.transport.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = dl.SetWriteDeadline(time.Now().Add(closeGoAwayDeadline))
	}
	if !sc.closed.Load() {
		if err := sc.fr.WriteGoAway(sc.lastPeerStreamID(), code, nil); err == nil {
			sc.bumpFramesSent()
		}
	}
	sc.wmu.Unlock()
}

func (sc *ServerConn) shutdownStreams(_ error) {
	sc.smu.Lock()
	defer sc.smu.Unlock()
	for _, s := range sc.streams {
		select {
		case s.events <- StreamEvent{Type: EventReset, RSTCode: frame.ErrCodeInternalError, EndStream: true}:
		default:
		}
		close(s.events)
	}
}

// --- shared helpers ---

// encodeAdvertised converts AdvertisedSettings to a SettingsParams frame payload.
func encodeAdvertised(a AdvertisedSettings) frame.SettingsParams {
	var p frame.SettingsParams
	add := func(id frame.SettingID, v uint32) {
		p.Pairs[p.N] = frame.SettingPair{ID: id, Value: v}
		p.N++
	}
	add(frame.SettingHeaderTableSize, a.HeaderTableSize)
	add(frame.SettingEnablePush, 0) // server never accepts push
	add(frame.SettingMaxConcurrentStreams, a.MaxConcurrentStreams)
	add(frame.SettingInitialWindowSize, a.InitialWindowSize)
	add(frame.SettingMaxFrameSize, a.MaxFrameSize)
	if a.MaxHeaderListSize != 0 {
		add(frame.SettingMaxHeaderListSize, a.MaxHeaderListSize)
	}
	return p
}

// settingValue returns the value of `id` from `s` or `def` when not present.
func settingValue(s frame.SettingsParams, id frame.SettingID, def uint32) uint32 {
	for i := range s.N {
		if s.Pairs[i].ID == id {
			return s.Pairs[i].Value
		}
	}
	return def
}

// AdvertisedSettings is what we send to the peer in our SETTINGS frame.
// Zero values are replaced by RFC 7540 defaults.
type AdvertisedSettings struct {
	HeaderTableSize      uint32
	MaxConcurrentStreams uint32
	InitialWindowSize    uint32
	MaxFrameSize         uint32
	MaxHeaderListSize    uint32
}

func (s AdvertisedSettings) defaulted() AdvertisedSettings {
	if s.HeaderTableSize == 0 {
		s.HeaderTableSize = 4096
	}
	if s.MaxConcurrentStreams == 0 {
		s.MaxConcurrentStreams = 100
	}
	if s.InitialWindowSize == 0 {
		s.InitialWindowSize = 65535
	} else if s.InitialWindowSize > 1<<31-1 {
		// RFC 9113 §6.5.2: a SETTINGS_INITIAL_WINDOW_SIZE above 2^31-1 is illegal.
		// Clamp the server's own advertised value so we never advertise an out-of-
		// range window (and the int32 recv-window seed derived from it stays valid).
		s.InitialWindowSize = 1<<31 - 1
	}
	if s.MaxFrameSize == 0 {
		s.MaxFrameSize = 16384
	}
	return s
}

// connInitialRecvWindow is the connection-level recv window size.
// RFC 7540 §6.9.2 fixes this at 65535.
const connInitialRecvWindow = 65535

// recvWindowRefundThreshold batches WINDOW_UPDATE at this granularity.
const recvWindowRefundThreshold = 32768

// closeGoAwayDeadline bounds GOAWAY write during Close.
const closeGoAwayDeadline = 200 * time.Millisecond

// settingsRecorder records the peer's SETTINGS and ACK state.
// Non-SETTINGS frames that arrive during the handshake (e.g. the client's
// request HEADERS sent in the same TCP segment as SETTINGS) are forwarded
// to the delegate handler so they are not lost.
type settingsRecorder struct {
	peer     frame.SettingsParams
	peerSeen bool
	ackSeen  bool
	delegate frame.Handler // optional; receives forwarded frames
}

func (r *settingsRecorder) OnData(fh frame.FrameHeader, data []byte, pad uint8) error {
	if r.delegate != nil {
		return r.delegate.OnData(fh, data, pad)
	}
	return nil
}
func (r *settingsRecorder) OnHeaders(fh frame.FrameHeader, hb frame.HeaderBlock, p *frame.Priority, flags uint8) error {
	if r.delegate != nil {
		return r.delegate.OnHeaders(fh, hb, p, flags)
	}
	return nil
}
func (r *settingsRecorder) OnPriority(fh frame.FrameHeader, p frame.Priority) error {
	if r.delegate != nil {
		return r.delegate.OnPriority(fh, p)
	}
	return nil
}
func (r *settingsRecorder) OnRSTStream(fh frame.FrameHeader, c frame.ErrCode) error {
	if r.delegate != nil {
		return r.delegate.OnRSTStream(fh, c)
	}
	return nil
}
func (r *settingsRecorder) OnSettings(fh frame.FrameHeader, s frame.SettingsParams) error {
	if fh.Flags&frame.FlagSettingsAck != 0 {
		r.ackSeen = true
		return nil
	}
	r.peer = s
	r.peerSeen = true
	return nil
}
func (r *settingsRecorder) OnPushPromise(fh frame.FrameHeader, sid uint32, hb frame.HeaderBlock, flags uint8) error {
	if r.delegate != nil {
		return r.delegate.OnPushPromise(fh, sid, hb, flags)
	}
	return nil
}
func (r *settingsRecorder) OnPing(fh frame.FrameHeader, payload [8]byte) error {
	if r.delegate != nil {
		return r.delegate.OnPing(fh, payload)
	}
	return nil
}
func (r *settingsRecorder) OnGoAway(fh frame.FrameHeader, sid uint32, c frame.ErrCode, d []byte) error {
	if r.delegate != nil {
		return r.delegate.OnGoAway(fh, sid, c, d)
	}
	return nil
}
func (r *settingsRecorder) OnWindowUpdate(fh frame.FrameHeader, inc uint32) error {
	if r.delegate != nil {
		return r.delegate.OnWindowUpdate(fh, inc)
	}
	return nil
}
func (r *settingsRecorder) OnContinuation(fh frame.FrameHeader, hb frame.HeaderBlock) error {
	if r.delegate != nil {
		return r.delegate.OnContinuation(fh, hb)
	}
	return nil
}

func (r *settingsRecorder) OnOrigin(fh frame.FrameHeader, origins []string) error {
	if r.delegate != nil {
		return r.delegate.OnOrigin(fh, origins)
	}
	return nil
}

func (r *settingsRecorder) OnAltSvc(fh frame.FrameHeader, entries []frame.AltSvcEntry) error {
	if r.delegate != nil {
		return r.delegate.OnAltSvc(fh, entries)
	}
	return nil
}

var _ frame.Handler = (*settingsRecorder)(nil)

// --- Error types ---

var (
	// ErrBadPreface is returned when the client sends an invalid
	// HTTP/2 connection preface.
	ErrBadPreface = errors.New("conn: invalid HTTP/2 client preface")
	// ErrConnClosed is returned after the connection has been closed.
	ErrConnClosed = errors.New("conn: connection closed")
	// ErrStreamClosed is returned when operating on a closed stream.
	ErrStreamClosed = errors.New("conn: stream already closed")
)
