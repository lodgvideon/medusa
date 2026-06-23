// Package server provides a high-level HTTP/2 server built on the conn package.
//
// It handles TLS + h2c listening, accepts connections, dispatches inbound
// streams to registered handlers, and manages graceful shutdown.
//
// Design (SOLID):
//   - S: owns only the accept loop + routing
//   - O: extensible via Handler interface and Middleware chain
//   - L: any Handler implementation is interchangeable
//   - I: small interfaces — Handler, Middleware, Listener
//   - D: depends on Listener and Handler interfaces, not concrete types
package server

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-server/conn"
)

// ---------------------------------------------------------------------------
// Handler interface
// ---------------------------------------------------------------------------

// Handler is the native zero-allocation HTTP/2 handler interface.
// Implementations must be safe for concurrent use if registered on a
// shared server; the server dispatches each stream to the handler in its
// own goroutine.
type Handler interface {
	ServeHTTP(ctx context.Context, req *Request, w ResponseWriter) error
}

// HandlerFunc is a type adapter that allows the use of ordinary functions
// as Handler values.
type HandlerFunc func(ctx context.Context, req *Request, w ResponseWriter) error

// ServeHTTP calls f(ctx, req, w).
func (f HandlerFunc) ServeHTTP(ctx context.Context, req *Request, w ResponseWriter) error {
	return f(ctx, req, w)
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

// Request represents a server-side HTTP/2 request.
//
// Path is the raw :path pseudo-header value (RFC 7540 §8.1.2.3) and MAY
// include the query string, mirroring net/http.Request.URL.RequestURI().
// This is intentional for back-compat with chi/echo/gin-style routers
// that match routes by the full request line. Use RawQuery to access
// the query string separately, or url.ParseRequestURI(req.Path) for
// structured access.
type Request struct {
	Method     string
	Path       string // raw :path (e.g. "/api/v1/users?limit=10")
	RawQuery   string // query string without '?' (e.g. "limit=10"), "" if none
	Scheme     string // "https" or "http" (h2c)
	Authority  string // :authority pseudo-header
	Headers    []hpack.HeaderField
	Trailers   []hpack.HeaderField // trailing headers (after body)
	Body       []byte              // collected body; nil if streaming
	BodyReader io.ReadCloser       // streaming body reader; nil if collected

	streamID uint32 // internal: HTTP/2 stream identifier
}

// StreamID returns the HTTP/2 stream identifier for this request.
func (r *Request) StreamID() uint32 { return r.streamID }

// ---------------------------------------------------------------------------
// Internal stream writer abstraction
// ---------------------------------------------------------------------------

// streamWriter abstracts the underlying stream write operations so that
// tests can provide mocks without needing a real conn.ServerStream.
type streamWriter interface {
	sendHeaders(ctx context.Context, headers []hpack.HeaderField, endStream bool) error
	sendData(ctx context.Context, p []byte, endStream bool) error
	streamID() uint32
}

// connStreamWriter adapts *conn.ServerStream to the streamWriter interface.
type connStreamWriter struct {
	stream *conn.ServerStream
}

func (w *connStreamWriter) sendHeaders(ctx context.Context, headers []hpack.HeaderField, endStream bool) error {
	return w.stream.SendHeaders(ctx, headers, endStream)
}

func (w *connStreamWriter) sendData(ctx context.Context, p []byte, endStream bool) error {
	return w.stream.SendData(ctx, p, endStream)
}

func (w *connStreamWriter) streamID() uint32 {
	return w.stream.ID()
}

// canPush returns the underlying stream if it supports push.
// This satisfies the pusher interface used by responseWriter's Pusher methods.
func (w *connStreamWriter) canPush() (pushableStream, bool) {
	if w.stream == nil {
		return nil, false
	}
	return w.stream, true
}

// ---------------------------------------------------------------------------
// ResponseWriter
// ---------------------------------------------------------------------------

// ResponseWriter is the interface handlers and middleware receive. It exposes
// both the native (zero-allocation) Poseidon write path and the stdlib
// http.ResponseWriter path, so handlers may use either API.
//
// Because ResponseWriter is an interface, middleware can intercept the response
// by wrapping it — embedding a ResponseWriter and overriding the write methods
// (this is how the Gzip middleware buffers and compresses the body). The
// concrete implementation is unexported; obtain one via NewResponseWriter.
//
// Native (zero-allocation) path:
//
//	w.WriteHeaders(200, hpackHeaders)
//	w.WriteData(body)
//	w.WriteTrailers(trailers)
//
// Stdlib-compatible path:
//
//	w.Header().Set("Content-Type", "text/plain")
//	w.WriteHeader(200)
//	w.Write(body)
//
// Server Push is an optional capability exposed via the separate Pusher
// interface (mirroring net/http.Pusher). For a directly-supplied writer a type
// assertion works; to reach it reliably through middleware wrappers use the
// [PusherOf] finder, which walks the Unwrap() chain:
//
//	if p, ok := server.PusherOf(w); ok { p.Push("/style.css", nil) }
//
// The analogous [FlusherOf] returns the [http.Flusher] capability. Wrapping
// writers cooperate by implementing Unwrap() ResponseWriter (see responsewriter.go).
type ResponseWriter interface {
	http.ResponseWriter

	// WriteHeaders sends response headers with the given status and extra
	// HPACK fields. No-op if headers were already sent.
	WriteHeaders(status int, headers []hpack.HeaderField) error
	// WriteData sends a response body chunk, auto-sending a 200 if needed.
	WriteData(p []byte) error
	// WriteTrailers sends trailing headers and ends the stream.
	WriteTrailers(trailers []hpack.HeaderField) error

	// Status returns the status code set via WriteHeaders/WriteHeader (0 if unset).
	Status() int
	// StatusCode is an alias of Status kept for back-compat.
	StatusCode() int
	// Written reports whether headers have been sent.
	Written() bool
}

// Pusher is the optional HTTP/2 Server Push capability (RFC 7540 §8.2),
// implemented by ResponseWriters backed by a real stream. It is kept separate
// from ResponseWriter (as net/http keeps Pusher/Flusher/Hijacker separate) so
// the core interface stays small. Reach it with the [PusherOf] finder (which
// works through middleware wrappers) or, for a directly-supplied writer, a
// w.(Pusher) type assertion.
type Pusher interface {
	Push(promisePath string, promiseHeaders []hpack.HeaderField) (ResponseWriter, error)
	PushWithScheme(promisePath, promiseScheme string, promiseHeaders []hpack.HeaderField) (ResponseWriter, error)
	PushWithPriority(promisePath string, promiseHeaders []hpack.HeaderField, prio *frame.Priority) (ResponseWriter, error)
}

// responseWriter is the concrete ResponseWriter backed by a stream sink.
type responseWriter struct {
	sw streamWriter
	// ctx is cancelled when the stream is reset by the client or the connection
	// closes; every write uses it (via reqCtx) so a handler blocked writing to a
	// vanished client unblocks instead of hanging on context.Background().
	ctx     context.Context
	headers http.Header
	status  int
	written bool
	// req is the originating request; used to derive scheme for Push.
	// Nil when constructed via NewResponseWriter (e.g. from tests); Push
	// falls back to "https" in that case.
	req *Request
}

// NewResponseWriter creates a ResponseWriter backed by the given ServerStream.
func NewResponseWriter(stream *conn.ServerStream) ResponseWriter {
	return &responseWriter{
		sw:  &connStreamWriter{stream: stream},
		ctx: stream.Context(),
	}
}

// newConnResponseWriter creates the concrete writer for a stream, binding the
// originating request (used to derive the Push :scheme). Internal to the server.
func newConnResponseWriter(stream *conn.ServerStream, req *Request) *responseWriter {
	return &responseWriter{
		sw:  &connStreamWriter{stream: stream},
		ctx: stream.Context(),
		req: req,
	}
}

// newResponseWriterWithSW creates a responseWriter with a custom streamWriter
// (for testing).
func newResponseWriterWithSW(sw streamWriter) *responseWriter {
	return &responseWriter{sw: sw, ctx: context.Background()}
}

// reqCtx returns the context used for all writes: the stream's context
// (cancelled on reset / connection close), or context.Background() if unset.
func (w *responseWriter) reqCtx() context.Context {
	if w.ctx != nil {
		return w.ctx
	}
	return context.Background()
}

// StatusCode returns the HTTP status code that was set via WriteHeaders or
// WriteHeader. Returns 0 if no status has been set yet.
func (w *responseWriter) StatusCode() int { return w.status }

// Compile-time interface checks.
var (
	_ http.ResponseWriter = (*responseWriter)(nil)
	_ ResponseWriter      = (*responseWriter)(nil)
	_ Pusher              = (*responseWriter)(nil)
	_ http.Flusher        = (*responseWriter)(nil)
)

// --- Native Poseidon methods ------------------------------------------------

// WriteHeaders sends response headers with the given status code and extra
// HPACK header fields. If headers have already been sent this is a no-op.
func (w *responseWriter) WriteHeaders(status int, headers []hpack.HeaderField) error {
	if w.written {
		return nil
	}
	w.status = status
	w.written = true

	// Pre-computed :status value for common codes.
	statusVal := statusBytes(status)

	fields := make([]hpack.HeaderField, 0, 1+len(headers))
	fields = append(fields, hpack.HeaderField{
		Name:  sColonStatus,
		Value: statusVal,
	})
	fields = append(fields, headers...)

	return w.sw.sendHeaders(w.reqCtx(), fields, false)
}

// WriteData sends response body data. If headers have not been sent yet it
// auto-sends a 200 response before writing data.
func (w *responseWriter) WriteData(p []byte) error {
	if !w.written {
		if err := w.WriteHeaders(http.StatusOK, nil); err != nil {
			return err
		}
	}
	return w.sw.sendData(w.reqCtx(), p, false)
}

// WriteTrailers sends trailing headers using SendHeaders with endStream=true
// (the conn package does not expose a dedicated trailer method).
func (w *responseWriter) WriteTrailers(trailers []hpack.HeaderField) error {
	return w.sw.sendHeaders(w.reqCtx(), trailers, true)
}

// --- http.ResponseWriter interface ------------------------------------------

// Header returns the header map that will be sent by WriteHeader.
// Lazy-initialised on first access.
func (w *responseWriter) Header() http.Header {
	if w.headers == nil {
		w.headers = make(http.Header)
	}
	return w.headers
}

// Write sends response body data, implementing io.Writer. If headers have not
// been sent yet, auto-sends a 200 response.
func (w *responseWriter) Write(p []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	if err := w.sw.sendData(w.reqCtx(), p, false); err != nil {
		return 0, err
	}
	return len(p), nil
}

// WriteHeader sends an HTTP response header with the provided status code.
// If headers have already been sent (via WriteHeaders or a prior WriteHeader
// call) this is a no-op, matching stdlib behaviour.
func (w *responseWriter) WriteHeader(statusCode int) {
	if w.written {
		return
	}
	w.status = statusCode
	w.written = true

	// Build HPACK fields from stored http.Header.
	hdr := w.headers
	if hdr == nil {
		hdr = make(http.Header)
	}
	statusVal := statusBytes(statusCode)
	fields := make([]hpack.HeaderField, 0, 1+len(hdr))
	fields = append(fields, hpack.HeaderField{
		Name:  sColonStatus,
		Value: statusVal,
	})
	for k, vv := range hdr {
		lower := strings.ToLower(k)
		for _, v := range vv {
			fields = append(fields, hpack.HeaderField{
				Name:  []byte(lower),
				Value: []byte(v),
			})
		}
	}

	_ = w.sw.sendHeaders(w.reqCtx(), fields, false)
}

// Status returns the HTTP status code set via WriteHeaders or WriteHeader.
func (w *responseWriter) Status() int { return w.status }

// Written reports whether headers have been sent.
func (w *responseWriter) Written() bool { return w.written }

// Flush implements [http.Flusher]. The native HTTP/2 write path sends each
// WriteData/Write frame to the connection immediately (there is no
// response-side buffering on conn.ServerStream), so for the concrete writer
// Flush is a no-op kept for stdlib compatibility: handlers and middleware that
// type-assert http.Flusher (or call FlusherOf) get a writer that satisfies the
// contract. Buffering wrappers (e.g. the Gzip middleware) override Flush to
// drain their own buffer first.
func (w *responseWriter) Flush() {}

// ---------------------------------------------------------------------------
// Adapters: http.Handler ↔ Handler
// ---------------------------------------------------------------------------

// FromHTTPHandler adapts any [http.Handler] (chi.Router, http.ServeMux, etc.)
// to a Poseidon [Handler].
func FromHTTPHandler(h http.Handler) Handler {
	return HandlerFunc(func(ctx context.Context, req *Request, w ResponseWriter) error {
		httpReq, err := NewHTTPRequest(req)
		if err != nil {
			return err
		}
		httpReq = httpReq.WithContext(ctx)
		h.ServeHTTP(w, httpReq)
		return nil
	})
}

// ToHTTPHandler adapts a Poseidon [Handler] to [http.Handler].
//
// The Poseidon handler is run against a buffering streamWriter that captures
// the status, extra HPACK header fields, and the full response body. Once the
// handler returns, the captured values are replayed onto the real
// [http.ResponseWriter]: headers set via the stdlib path (w.Header()) are
// copied, any header fields emitted via the native WriteHeaders path are merged
// in, the status is written, and the buffered body bytes are flushed — so the
// response body round-trips instead of being discarded.
func ToHTTPHandler(h Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := HTTPRequestToRequest(r)
		buf := &bufferStreamWriter{}
		pw := newResponseWriterWithSW(buf)
		if err := h.ServeHTTP(r.Context(), req, pw); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Copy stdlib-path headers (set via w.Header()).
		for k, vv := range pw.Header() {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		// Merge native-path header fields (from WriteHeaders), skipping the
		// :status pseudo-header which is conveyed via WriteHeader below.
		for _, f := range buf.headerFields {
			name := string(f.Name)
			if name != "" && name[0] == ':' {
				continue
			}
			w.Header().Add(name, string(f.Value))
		}
		if pw.Status() > 0 {
			w.WriteHeader(pw.Status())
		}
		if len(buf.body) > 0 {
			_, _ = w.Write(buf.body)
		}
	})
}

// bufferStreamWriter is a streamWriter that buffers the status, header fields
// and body bytes a handler writes, for ToHTTPHandler to replay onto a real
// http.ResponseWriter. It is not concurrency-safe; one is used per request.
type bufferStreamWriter struct {
	headerFields []hpack.HeaderField
	body         []byte
}

func (b *bufferStreamWriter) sendHeaders(_ context.Context, headers []hpack.HeaderField, _ bool) error {
	// Capture the first (response) HEADERS frame's fields. Trailers (endStream)
	// also arrive here; appending them is harmless since the status comes from
	// responseWriter.Status() and pseudo-headers are filtered on replay.
	b.headerFields = append(b.headerFields, headers...)
	return nil
}

func (b *bufferStreamWriter) sendData(_ context.Context, p []byte, _ bool) error {
	b.body = append(b.body, p...)
	return nil
}

func (*bufferStreamWriter) streamID() uint32 { return 0 }

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

// NewHTTPRequest builds a standard [http.Request] from a Poseidon [Request].
func NewHTTPRequest(req *Request) (*http.Request, error) {
	scheme := req.Scheme
	if scheme == "" {
		scheme = schemeHTTP
	}
	host := req.Authority
	if host == "" {
		host = "localhost"
	}
	urlStr := scheme + "://" + host + req.Path
	httpReq, err := http.NewRequest(req.Method, urlStr, http.NoBody)
	if err != nil {
		return nil, err
	}
	// Body is never nil (http.NoBody from http.NewRequest), matching net/http.
	// Pick exactly one source: the buffered []byte, the streaming reader (when
	// the server runs with Options.StreamingBody), or neither (empty body).
	switch {
	case req.Body != nil:
		httpReq.Body = &closeableReader{data: req.Body}
		httpReq.ContentLength = int64(len(req.Body))
	case req.BodyReader != nil:
		httpReq.Body = req.BodyReader
		httpReq.ContentLength = -1 // unknown length for a stream
	}
	for _, h := range req.Headers {
		httpReq.Header.Add(string(h.Name), string(h.Value))
	}
	return httpReq, nil
}

// HTTPRequestToRequest converts a standard [http.Request] to a Poseidon [Request].
func HTTPRequestToRequest(r *http.Request) *Request {
	headers := make([]hpack.HeaderField, 0, len(r.Header))
	for k, vv := range r.Header {
		for _, v := range vv {
			headers = append(headers, hpack.HeaderField{
				Name:  []byte(k),
				Value: []byte(v),
			})
		}
	}
	return &Request{
		Method:    r.Method,
		Path:      r.URL.Path,
		RawQuery:  r.URL.RawQuery,
		Scheme:    r.URL.Scheme,
		Authority: r.URL.Host,
		Headers:   headers,
	}
}

// closeableReader wraps a byte slice as io.ReadCloser.
type closeableReader struct {
	data []byte
	pos  int
}

func (r *closeableReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *closeableReader) Close() error { return nil }

// ---------------------------------------------------------------------------
// Pre-computed byte slices for zero-allocation hot paths
// ---------------------------------------------------------------------------

var sColonStatus = []byte(":status")

// statusBytes returns pre-allocated byte slice for common HTTP status codes.
// Falls back to strconv.AppendInt for uncommon codes.
func statusBytes(code int) []byte {
	switch code {
	case 200:
		return sStatus200
	case 201:
		return sStatus201
	case 204:
		return sStatus204
	case 301:
		return sStatus301
	case 302:
		return sStatus302
	case 304:
		return sStatus304
	case 400:
		return sStatus400
	case 401:
		return sStatus401
	case 403:
		return sStatus403
	case 404:
		return sStatus404
	case 500:
		return sStatus500
	case 502:
		return sStatus502
	case 503:
		return sStatus503
	default:
		var buf [6]byte
		return strconv.AppendInt(buf[:0], int64(code), 10)
	}
}

var (
	sStatus200 = []byte("200")
	sStatus201 = []byte("201")
	sStatus204 = []byte("204")
	sStatus301 = []byte("301")
	sStatus302 = []byte("302")
	sStatus304 = []byte("304")
	sStatus400 = []byte("400")
	sStatus401 = []byte("401")
	sStatus403 = []byte("403")
	sStatus404 = []byte("404")
	sStatus500 = []byte("500")
	sStatus502 = []byte("502")
	sStatus503 = []byte("503")
)
