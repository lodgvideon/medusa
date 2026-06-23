package server

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-server/conn"
)

// ErrServerClosed is returned by Serve and ListenAndServe after the server
// has been shut down via Close.
var ErrServerClosed = errors.New("server: server closed")

// Logger is the minimal logging interface the server writes diagnostics to.
// A nil Options.Logger falls back to the standard library log package.
type Logger interface {
	Printf(format string, args ...interface{})
}

type stdLogger struct{}

func (stdLogger) Printf(format string, args ...interface{}) { log.Printf(format, args...) }

// Options configures a Server. The zero value is usable; each field documents
// its default when left empty.
type Options struct {
	Addr                     string
	Handler                  Handler
	HTTPHandler              http.Handler
	Middleware               []Middleware
	ConnOpts                 conn.ServerConnOptions
	MaxConcurrentConnections int
	GracefulShutdownTimeout  time.Duration
	Logger                   Logger

	// H2C enables HTTP/2 cleartext (prior knowledge + HTTP/1.1 Upgrade).
	// When false (default), the server expects direct HTTP/2 connections
	// (typically over TLS). When true, the server detects HTTP/1.1 Upgrade
	// requests and responds with 101 Switching Protocols.
	H2C bool

	// StreamingBody enables io.ReadCloser body instead of buffering.
	// When true, Request.BodyReader is set and Body is nil.
	StreamingBody bool

	// IdleTimeout is the maximum amount of time to wait for the next
	// request/stream on an idle connection.
	//
	//   0  => secure default (defaultIdleTimeout)
	//   <0 => disabled (no idle timeout; keep-alive forever)
	//   >0 => explicit timeout
	//
	// A sensible default protects long-lived HTTP/2 connections from being
	// held open indefinitely by idle clients, while sequential/active streams
	// reset the clock on every new stream.
	IdleTimeout time.Duration

	// MaxRequestBodyBytes caps the size of an inbound request body to bound
	// memory use and defend against memory-exhaustion DoS via large uploads.
	// It is enforced in BOTH buffered mode (accumulation stops and the request
	// is rejected with 413 once the cap is exceeded — never buffering beyond
	// the cap) and streaming mode (BodyReader returns ErrBodyTooLarge once the
	// total bytes read exceed the cap).
	//
	//   0  => secure default (defaultMaxRequestBodyBytes, 10 MiB)
	//   <0 => unlimited / disabled
	//   >0 => explicit limit in bytes
	MaxRequestBodyBytes int64

	// OnDrainStart, if set, is invoked exactly once at the very START of
	// Shutdown — before the listener is closed and before GOAWAY is sent —
	// so callers can flip readiness to NOT-ready (e.g.
	// HealthState.SetNotReady and/or grpc health SetServingStatus(NotServing)).
	// This lets Kubernetes stop routing new traffic to this instance while
	// in-flight streams continue to drain. It runs synchronously while the
	// server lock is held, so it must not block or call back into the server.
	OnDrainStart func()
}

// defaultMaxRequestBodyBytes is the secure-by-default cap on buffered request
// bodies (10 MiB), applied when MaxRequestBodyBytes is zero.
const defaultMaxRequestBodyBytes = 10 << 20

// defaultIdleTimeout is the secure-by-default idle connection timeout applied
// when Options.IdleTimeout is zero.
const defaultIdleTimeout = 120 * time.Second

// resolveMaxBodyBytes resolves the body-size cap sentinel into a concrete
// limit: a positive byte count, or -1 for "unlimited".
func (o Options) resolveMaxBodyBytes() int64 {
	switch {
	case o.MaxRequestBodyBytes == 0:
		return defaultMaxRequestBodyBytes
	case o.MaxRequestBodyBytes < 0:
		return -1 // unlimited
	default:
		return o.MaxRequestBodyBytes
	}
}

func (o Options) validate() error {
	if o.Handler == nil && o.HTTPHandler == nil {
		return errors.New("server: Handler or HTTPHandler is required")
	}
	return nil
}

func (o Options) resolvedHandler() Handler {
	h := o.Handler
	if h == nil {
		h = FromHTTPHandler(o.HTTPHandler)
	}
	if len(o.Middleware) > 0 {
		h = Chain(o.Middleware...)(h)
	}
	return h
}

// Server is an HTTP/2 (optionally h2c) server built on the poseidon conn layer.
// Construct one with NewServer, then drive it with Serve or ListenAndServe.
type Server struct {
	handler      Handler
	connOpts     conn.ServerConnOptions
	opts         Options
	maxBodyBytes int64 // resolved: >0 limit, -1 unlimited
	logger       Logger
	mu           sync.Mutex
	closed       bool
	shutdown     bool
	listeners    map[net.Listener]struct{} // listeners passed to Serve; closed by Close/Shutdown
	conns        map[*conn.ServerConn]struct{}
	closedStats  transportTotals // counters folded in from closed conns (guarded by mu)
	inFlight     sync.WaitGroup  // active streams being served
	done         chan struct{}
	closeCh      chan struct{}
}

// transportTotals accumulates ConnStats from connections that have closed, so
// TransportStats can report monotonic counters (closed totals + live conns).
// Guarded by Server.mu.
type transportTotals struct {
	bytesSent       int64
	bytesReceived   int64
	framesSent      int64
	framesReceived  int64
	streamsAccepted uint64
	rapidResets     uint64
	goAways         uint64
}

// NewServer validates opts and returns a ready-to-serve Server. It returns a
// non-nil error if opts is invalid.
func NewServer(opts Options) (*Server, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = stdLogger{}
	}
	if opts.GracefulShutdownTimeout <= 0 {
		opts.GracefulShutdownTimeout = 30 * time.Second
	}
	// Secure-by-default idle timeout: zero => default, negative => disabled.
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = defaultIdleTimeout
	}
	return &Server{
		handler:      opts.resolvedHandler(),
		connOpts:     opts.ConnOpts,
		opts:         opts,
		maxBodyBytes: opts.resolveMaxBodyBytes(),
		logger:       logger,
		listeners:    make(map[net.Listener]struct{}),
		conns:        make(map[*conn.ServerConn]struct{}),
		done:         make(chan struct{}),
		closeCh:      make(chan struct{}),
	}, nil
}

// idleTimeout returns the effective idle timeout: 0 when disabled (negative
// sentinel) so the accept loop skips the per-stream deadline.
func (s *Server) idleTimeout() time.Duration {
	if s.opts.IdleTimeout < 0 {
		return 0
	}
	return s.opts.IdleTimeout
}

// ListenAndServe listens on opts.Addr (TCP) and serves connections until ctx
// is cancelled or Close is called, returning ErrServerClosed on clean shutdown.
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.opts.Addr)
	if err != nil {
		return err
	}
	s.logger.Printf("poseidon: listening on %s", ln.Addr())
	return s.Serve(ctx, ln)
}

// Serve accepts connections from ln until ctx is cancelled or Close is called,
// returning ErrServerClosed on clean shutdown. It takes ownership of ln.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		// Already closed before Serve started; don't accept anything.
		s.mu.Unlock()
		_ = ln.Close()
		return ErrServerClosed
	}
	s.listeners[ln] = struct{}{}
	s.mu.Unlock()

	// serveDone bounds the watcher goroutine to this Serve call's lifetime.
	serveDone := make(chan struct{})
	defer func() {
		close(serveDone)
		s.mu.Lock()
		delete(s.listeners, ln)
		s.mu.Unlock()
	}()

	// Watch for ctx cancellation (→ Close) and terminate on Close()/Shutdown()
	// or when Serve returns. Selecting on closeCh/serveDone — not just ctx.Done()
	// — means the watcher always exits: with ctx == context.Background(),
	// ctx.Done() is a nil channel (never ready), so on its own this goroutine
	// would block forever (a leak) after the server stops.
	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-s.closeCh:
		case <-serveDone:
		}
	}()
	for {
		nc, err := ln.Accept()
		if err != nil {
			select {
			case <-s.closeCh:
				return ErrServerClosed
			default:
				return err
			}
		}
		if s.opts.MaxConcurrentConnections > 0 &&
			s.ConnCount() >= s.opts.MaxConcurrentConnections {
			s.logger.Printf("poseidon: rejecting %s: max connections", nc.RemoteAddr())
			_ = nc.Close()
			continue
		}
		go func() {
			if s.opts.H2C {
				s.detectAndServe(ctx, nc)
			} else {
				s.serveConn(ctx, nc)
			}
		}()
	}
}

func (s *Server) serveConn(ctx context.Context, nc net.Conn) {
	opts := s.connOpts
	if opts.StreamEventBuffer <= 0 {
		opts.StreamEventBuffer = 8
	}
	sc, err := conn.NewServerConn(ctx, nc, opts)
	if err != nil {
		s.logger.Printf("poseidon: handshake failed for %s: %v", nc.RemoteAddr(), err)
		_ = nc.Close()
		return
	}
	s.trackConn(sc, true)
	defer s.trackConn(sc, false)
	s.acceptLoop(ctx, sc)
}

// acceptLoop reads streams from a ServerConn with optional idle timeout.
func (s *Server) acceptLoop(ctx context.Context, sc *conn.ServerConn) {
	if idle := s.idleTimeout(); idle > 0 {
		for {
			acceptCtx, cancel := context.WithTimeout(ctx, idle)
			stream, err := sc.AcceptStream(acceptCtx)
			cancel()
			if err != nil {
				return
			}
			if !s.spawnStream(stream) {
				return
			}
		}
	}
	for {
		stream, err := sc.AcceptStream(ctx)
		if err != nil {
			return
		}
		if !s.spawnStream(stream) {
			return
		}
	}
}

// spawnStream begins serving stream unless the server is shutting down. It
// increments inFlight under s.mu, synchronized with Shutdown/Close (which set
// s.shutdown/s.closed under the same lock before waiting on inFlight) — so an
// Add can never race a returning inFlight.Wait(), the documented WaitGroup
// misuse. Returns false when the server is draining, signalling the accept loop
// to stop; the just-accepted stream is reset so the client can retry elsewhere.
func (s *Server) spawnStream(stream *conn.ServerStream) bool {
	s.mu.Lock()
	if s.shutdown || s.closed {
		s.mu.Unlock()
		_ = stream.Close() // refuse a stream that arrived after drain began
		return false
	}
	s.inFlight.Add(1)
	s.mu.Unlock()
	go s.serveStream(stream)
	return true
}

func (s *Server) serveStream(stream *conn.ServerStream) {
	defer s.inFlight.Done()

	// Backstop panic isolation: a panic anywhere in the request lifecycle
	// (buildRequest, body read, or a handler not already guarded by
	// dispatchAndClose) must not crash the whole process. Recover, log, and
	// tear down just this stream — every other connection survives.
	defer func() {
		if rec := recover(); rec != nil {
			s.logger.Printf("poseidon: recovered panic serving stream %d: %v\n%s", stream.ID(), rec, debug.Stack())
			_ = stream.Close()
		}
	}()

	// Drive the whole request lifecycle off the stream's context so a client
	// RST_STREAM or a connection close cancels the handler — and its writes and
	// body reads — promptly. It descends from the server context, so server
	// shutdown still propagates.
	ctx := stream.Context()

	var req *Request

	// Read HEADERS first.
	ev, err := stream.Recv(ctx)
	if err != nil {
		return
	}
	if ev.Type != conn.EventHeaders {
		_ = stream.Close()
		return
	}

	req = s.buildRequest(ev.Headers, stream.ID())
	if ev.EndStream {
		s.dispatchAndClose(ctx, stream, req)
		return
	}

	// Streaming mode: attach io.ReadCloser and dispatch immediately.
	if s.opts.StreamingBody {
		req.BodyReader = newStreamBody(ctx, stream, s.maxBodyBytes)
		s.dispatchAndClose(ctx, stream, req)
		return
	}

	// Buffered mode: collect DATA frames then dispatch.
	s.serveStreamBuffered(ctx, stream, req)
}

// serveStreamBuffered collects DATA/Trailers frames and dispatches.
func (s *Server) serveStreamBuffered(ctx context.Context, stream *conn.ServerStream, req *Request) {
	var bodyChunks [][]byte
	var total int64
	for {
		ev, err := stream.Recv(ctx)
		if err != nil {
			return
		}
		switch ev.Type {
		case conn.EventData:
			if ev.Data != nil {
				// Enforce the body-size cap BEFORE accumulating, so an
				// over-cap upload can never balloon memory: reject with 413
				// and drop the (already-collected) chunks immediately.
				total += int64(len(ev.Data))
				if s.maxBodyBytes >= 0 && total > s.maxBodyBytes {
					s.rejectTooLarge(stream)
					return
				}
				bodyChunks = append(bodyChunks, ev.Data)
			}
			if ev.EndStream {
				req.Body = joinChunks(bodyChunks)
				s.dispatchAndClose(ctx, stream, req)
				return
			}
		case conn.EventTrailers:
			req.Trailers = ev.Headers
			if ev.EndStream {
				req.Body = joinChunks(bodyChunks)
				s.dispatchAndClose(ctx, stream, req)
				return
			}
		case conn.EventReset:
			_ = stream.Close()
			return
		case conn.EventHeaders:
			// Extra HEADERS (illegal mid-stream), ignore.
		}
	}
}

// rejectTooLarge responds 413 (Request Entity Too Large) and tears the stream
// down. Used when a buffered request body exceeds MaxRequestBodyBytes. We send
// the status with END_STREAM via empty trailers, then Close (which RSTs if the
// client has not finished). No further body bytes are buffered.
func (s *Server) rejectTooLarge(stream *conn.ServerStream) {
	w := newConnResponseWriter(stream, nil)
	_ = w.WriteHeaders(http.StatusRequestEntityTooLarge, nil)
	_ = w.WriteTrailers(nil)
	_ = stream.Close()
}

func (s *Server) dispatchAndClose(ctx context.Context, stream *conn.ServerStream, req *Request) {
	if req == nil {
		_ = stream.Close()
		return
	}
	w := newConnResponseWriter(stream, req)
	// Per-request panic recovery: a panicking handler returns a 500 (if it has
	// not already written a response) and resets the stream, instead of
	// crashing the server. Mirrors net/http's per-request isolation.
	defer func() {
		if rec := recover(); rec != nil {
			s.logger.Printf("poseidon: recovered handler panic on stream %d: %v\n%s", stream.ID(), rec, debug.Stack())
			if !w.Written() {
				_ = w.WriteHeaders(500, nil)
				_ = w.WriteTrailers(nil)
			}
			_ = stream.Close()
		}
	}()
	if err := s.handler.ServeHTTP(ctx, req, w); err != nil {
		s.logger.Printf("poseidon: handler error on stream %d: %v", stream.ID(), err)
		if !w.Written() {
			_ = w.WriteHeaders(500, nil)
		}
		// Send EndStream via empty trailers.
		_ = w.WriteTrailers(nil)
		_ = stream.Close()
		return
	}
	if !w.Written() {
		_ = w.WriteHeaders(200, nil)
	}
	// Send EndStream via empty trailers.
	_ = w.WriteTrailers(nil)
	_ = stream.Close()
}

func (s *Server) buildRequest(headers []hpack.HeaderField, streamID uint32) *Request {
	req := &Request{Headers: headers, streamID: streamID}
	for _, h := range headers {
		switch string(h.Name) {
		case ":method":
			req.Method = string(h.Value)
		case ":path":
			// Raw :path per RFC 7540 §8.1.2.3; may include query string.
			// Path stays raw for back-compat with chi-style routers
			// that match routes by the full request line; RawQuery
			// exposes the pre-parsed query string (without '?').
			raw := string(h.Value)
			_, query := splitPathQuery(raw)
			req.Path = raw
			req.RawQuery = query
		case ":scheme":
			req.Scheme = string(h.Value)
		case ":authority":
			req.Authority = string(h.Value)
		}
	}
	return req
}

// splitPathQuery splits an :path value into path and query string.
// The query is returned without the leading '?'. Returns path only
// if no query is present. Both inputs are safe with arbitrary user
// data (no allocation beyond a single substring copy).
func splitPathQuery(reqPath string) (path, rawQuery string) {
	for i := range len(reqPath) {
		if reqPath[i] == '?' {
			return reqPath[:i], reqPath[i+1:]
		}
	}
	return reqPath, ""
}

func joinChunks(chunks [][]byte) []byte {
	if len(chunks) == 0 {
		return nil
	}
	if len(chunks) == 1 {
		return chunks[0]
	}
	var n int
	for _, c := range chunks {
		n += len(c)
	}
	out := make([]byte, 0, n)
	for _, c := range chunks {
		out = append(out, c...)
	}
	return out
}

func (s *Server) trackConn(sc *conn.ServerConn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		s.conns[sc] = struct{}{}
	} else {
		s.forgetConn(sc)
	}
}

// forgetConn removes sc from the live set, folding its final counters into
// closedStats so transport totals stay monotonic across connection close. It is
// idempotent (a no-op if sc is already gone), so the per-connection teardown
// defer and Close/Shutdown can all funnel through it without double-counting.
// Caller must hold s.mu.
func (s *Server) forgetConn(sc *conn.ServerConn) {
	if _, ok := s.conns[sc]; !ok {
		return
	}
	st := sc.Stats()
	s.closedStats.bytesSent += st.BytesSent
	s.closedStats.bytesReceived += st.BytesReceived
	s.closedStats.framesSent += st.FramesSent
	s.closedStats.framesReceived += st.FramesReceived
	s.closedStats.streamsAccepted += uint64(st.StreamsAccepted)
	s.closedStats.rapidResets += uint64(st.RapidResets)
	if st.GoAwaySent {
		s.closedStats.goAways++
	}
	delete(s.conns, sc)
}

// ConnCount returns the number of connections the server is currently tracking.
func (s *Server) ConnCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// TransportStats is an aggregate, point-in-time snapshot of HTTP/2 transport
// counters across every connection the server has handled. The byte/frame/stream
// and rapid-reset/GOAWAY fields are monotonic counters (they include connections
// that have since closed); ActiveConns is a gauge of currently-open connections.
type TransportStats struct {
	ActiveConns     int
	BytesSent       int64
	BytesReceived   int64
	FramesSent      int64
	FramesReceived  int64
	StreamsAccepted uint64
	RapidResets     uint64
	GoAways         uint64
}

// TransportStats returns the aggregate transport counters across all live and
// closed connections. Safe for concurrent use. Suitable as the source for a
// metrics exporter (see middleware.MetricsCollector.SetTransportSource).
func (s *Server) TransportStats() TransportStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts := TransportStats{
		ActiveConns:     len(s.conns),
		BytesSent:       s.closedStats.bytesSent,
		BytesReceived:   s.closedStats.bytesReceived,
		FramesSent:      s.closedStats.framesSent,
		FramesReceived:  s.closedStats.framesReceived,
		StreamsAccepted: s.closedStats.streamsAccepted,
		RapidResets:     s.closedStats.rapidResets,
		GoAways:         s.closedStats.goAways,
	}
	for sc := range s.conns {
		st := sc.Stats()
		ts.BytesSent += st.BytesSent
		ts.BytesReceived += st.BytesReceived
		ts.FramesSent += st.FramesSent
		ts.FramesReceived += st.FramesReceived
		ts.StreamsAccepted += uint64(st.StreamsAccepted)
		ts.RapidResets += uint64(st.RapidResets)
		if st.GoAwaySent {
			ts.GoAways++
		}
	}
	return ts
}

// Addr returns a listener address, or nil if not listening. When multiple
// listeners are served, an arbitrary one is returned.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ln := range s.listeners {
		return ln.Addr()
	}
	return nil
}

// Close stops accepting new connections and tears down the listener, causing
// Serve/ListenAndServe to return ErrServerClosed. It is safe to call multiple
// times; only the first call has an effect.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.closeCh)
	for ln := range s.listeners {
		_ = ln.Close()
		delete(s.listeners, ln)
	}
	for sc := range s.conns {
		_ = sc.Close()
		s.forgetConn(sc)
	}
	return nil
}

// Shutdown gracefully shuts down the server without interrupting active
// streams. It closes the listener and waits for in-flight streams to
// complete or the context to be cancelled.
//
// If the context expires before all streams are done, remaining
// connections are forcibly closed (equivalent to Close).
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrServerClosed
	}
	s.shutdown = true
	s.closed = true

	// Drain start: flip readiness to NOT-ready BEFORE closing the listener or
	// sending GOAWAY, so k8s removes this instance from Service endpoints and
	// stops routing new traffic while in-flight streams continue to drain.
	if s.opts.OnDrainStart != nil {
		s.opts.OnDrainStart()
	}

	// Close listeners — stop accepting new connections.
	for ln := range s.listeners {
		_ = ln.Close()
		delete(s.listeners, ln)
	}
	close(s.closeCh)

	// Snapshot current connections.
	conns := make([]*conn.ServerConn, 0, len(s.conns))
	for sc := range s.conns {
		conns = append(conns, sc)
	}
	s.mu.Unlock()

	// Send GOAWAY to all connections so clients stop opening new streams.
	for _, sc := range conns {
		_ = sc.GoAway(frame.ErrCodeNoError)
	}

	// Wait for in-flight streams to complete or context cancellation.
	done := make(chan struct{})
	go func() {
		s.inFlight.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All streams completed gracefully.
	case <-ctx.Done():
		// Timeout — forcibly close remaining connections.
		s.mu.Lock()
		for _, sc := range conns {
			_ = sc.Close()
			s.forgetConn(sc)
		}
		s.mu.Unlock()
		return ctx.Err()
	}

	// Close connections gracefully.
	s.mu.Lock()
	for _, sc := range conns {
		_ = sc.Close()
		s.forgetConn(sc)
	}
	s.mu.Unlock()
	return nil
}
