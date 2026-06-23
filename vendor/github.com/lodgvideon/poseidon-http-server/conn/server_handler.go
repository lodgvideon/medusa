package conn

import (
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// serverConnOps is the contract server_handler.go needs from ServerConn.
type serverConnOps interface {
	lookupStream(id uint32) *ServerStream
	// validateClientStreamID enforces RFC 9113 §5.1.1 (odd, strictly increasing)
	// for a newly opened client stream, returning a connError on violation.
	validateClientStreamID(id uint32) error
	// registerStream registers a new client stream, returning false if it was
	// refused for exceeding SETTINGS_MAX_CONCURRENT_STREAMS (RST_STREAM already
	// sent). The caller must not process a refused stream.
	registerStream(id uint32, s *ServerStream) bool
	markStreamDone(id uint32)
	writeSettingsAck() error
	writePingAck(payload [8]byte) error
	deliverPingAck(payload [8]byte)
	applyPeerSettings(s frame.SettingsParams) error
	onWindowUpdate(streamID, increment uint32) error
	onDataReceived(s *ServerStream, length uint32) error
	// onClientRSTStream accounts a client-initiated RST_STREAM for Rapid
	// Reset (CVE-2023-44487) detection. Returns a non-nil error when the
	// per-connection budget is exceeded; the reader loop then sends
	// GOAWAY(ENHANCE_YOUR_CALM) and tears the connection down.
	onClientRSTStream(streamID uint32, rapid bool) error
}

// defaultMaxHeaderBytes bounds the total compressed size of a single header
// block (the HEADERS frame plus all of its CONTINUATION frames) when the
// connection does not advertise SETTINGS_MAX_HEADER_LIST_SIZE. It defends
// against the CONTINUATION flood (CVE-2024-27316): an endless stream of
// CONTINUATION frames with no END_HEADERS would otherwise grow pendingBuf
// without bound until the process is OOM-killed.
const defaultMaxHeaderBytes = 1 << 20 // 1 MiB

// serverConnHandler bridges frame.Handler into per-ServerStream events.
type serverConnHandler struct {
	streams serverConnOps
	dec     *hpack.Decoder

	// maxHeaderBytes caps the accumulated compressed size of one header block.
	maxHeaderBytes int

	scratch          []hpack.HeaderField
	pendingStreamID  uint32
	pendingBuf       []byte
	pendingEndStream bool
	pendingTrailer   bool
	// pendingDiscard marks an open header block whose stream was refused
	// (over MaxConcurrentStreams): the block is still decoded to keep the
	// shared HPACK decoder in sync, but the result and stream are discarded.
	pendingDiscard bool
}

func newServerConnHandler(streams serverConnOps, dec *hpack.Decoder, maxHeaderBytes int) *serverConnHandler {
	if maxHeaderBytes <= 0 {
		maxHeaderBytes = defaultMaxHeaderBytes
	}
	return &serverConnHandler{
		streams:        streams,
		dec:            dec,
		maxHeaderBytes: maxHeaderBytes,
		scratch:        make([]hpack.HeaderField, 0, 16),
	}
}

// guardHeaderBlock enforces RFC 9113 §6.10: once a HEADERS frame without
// END_HEADERS opens a header block, the only frame permitted until END_HEADERS
// is a CONTINUATION on the same stream. Any other frame is a connection error
// of type PROTOCOL_ERROR. Invoked at the top of every non-CONTINUATION callback.
func (h *serverConnHandler) guardHeaderBlock() error {
	if h.pendingStreamID != 0 {
		return connError{code: frame.ErrCodeProtocolError, msg: "expected CONTINUATION for open header block"}
	}
	return nil
}

func (h *serverConnHandler) OnData(fh frame.FrameHeader, p []byte, _ uint8) error {
	if err := h.guardHeaderBlock(); err != nil {
		return err
	}
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil
	}
	if err := h.streams.onDataReceived(s, fh.Length); err != nil {
		return err
	}
	end := fh.Flags&frame.FlagDataEndStream != 0
	dataCopy := append([]byte(nil), p...)
	// Deliver the DATA (and its EOF) to the handler BEFORE releasing the stream:
	// markStreamDone cancels the stream context, and Recv must not drop the final
	// event in favour of cancellation.
	s.push(StreamEvent{Type: EventData, Data: dataCopy, EndStream: end})
	if end && s.markRemoteEnd() {
		// Release only once the server has also ended; until then the stream
		// stays registered (half-closed remote) so its WINDOW_UPDATE and
		// RST_STREAM still reach it (RFC 7540 §5.1).
		h.streams.markStreamDone(fh.StreamID)
	}
	return nil
}

func (h *serverConnHandler) OnHeaders(fh frame.FrameHeader, hb frame.HeaderBlock, prio *frame.Priority, _ uint8) error {
	if err := h.guardHeaderBlock(); err != nil {
		return err
	}
	// A client must never send HEADERS on an even (server-initiated) stream ID
	// (RFC 9113 §5.1.1) — including one colliding with a live push stream, which
	// would otherwise be treated as an existing stream and bypass validation.
	if fh.StreamID%2 == 0 {
		return connError{code: frame.ErrCodeProtocolError, msg: "client HEADERS on even (server-initiated) stream ID"}
	}
	end := fh.Flags&frame.FlagHeadersEndStream != 0
	endHeaders := fh.Flags&frame.FlagHeadersEndHeaders != 0

	s := h.streams.lookupStream(fh.StreamID)
	isNew := s == nil

	// refused: a new stream rejected for exceeding MaxConcurrentStreams
	// (RST_STREAM already sent by registerStream). The header block is still
	// decoded to keep the shared HPACK decoder's dynamic table in sync with the
	// client's persistent encoder — only stream registration and event delivery
	// are suppressed. Skipping the decode would desync HPACK and corrupt every
	// subsequent stream on the connection.
	refused := false
	if isNew {
		// RFC 9113 §5.1.1: reject an even or non-increasing client stream ID
		// (a connection PROTOCOL_ERROR) before allocating anything for it.
		if err := h.streams.validateClientStreamID(fh.StreamID); err != nil {
			return err
		}
		s = newServerStream(fh.StreamID, 8, nil, int32(connInitialRecvWindow))
		if !h.streams.registerStream(fh.StreamID, s) {
			refused = true
		}
	}

	// RFC 7540 §5.3: priority block is sent only on the first HEADERS
	// frame. Capture it once, before any CONTINUATION frames.
	if isNew && !refused && prio != nil {
		s.setPriority(prio)
	}

	if !endHeaders {
		if len(hb) > h.maxHeaderBytes {
			return connError{code: frame.ErrCodeProtocolError, msg: "header block exceeds max size"}
		}
		h.pendingStreamID = fh.StreamID
		h.pendingBuf = append(h.pendingBuf[:0], hb...)
		h.pendingEndStream = end
		h.pendingDiscard = refused
		h.pendingTrailer = !isNew && s.headersReceived
		// Only the first HEADERS carries priority; ignore any on trailers.
		return nil
	}

	if refused {
		// Single-frame refused block: decode-and-discard for HPACK sync.
		return h.discardHeaderBlock(hb)
	}

	isTrailer := false
	if !isNew {
		isTrailer = s.headersReceived
	}

	if !isTrailer {
		s.headersReceived = true
	}
	return h.emitHeaderBlock(s, hb, end, isTrailer)
}

// discardHeaderBlock decodes a header block solely to advance the shared HPACK
// decoder's dynamic table (keeping it in sync with the client's encoder) and
// throws the decoded fields away. Used for streams refused over
// MaxConcurrentStreams and for the defensive stream-vanished path: skipping the
// decode would desync the decoder and corrupt every later stream. Clears the
// open-block state so the interleaving guard re-admits other frames.
func (h *serverConnHandler) discardHeaderBlock(hb []byte) error {
	h.pendingStreamID = 0
	h.pendingDiscard = false
	return h.dec.DecodeBlock(hb, func(hpack.HeaderField) error { return nil })
}

func (h *serverConnHandler) OnContinuation(fh frame.FrameHeader, hb frame.HeaderBlock) error {
	// A CONTINUATION with no open header block, or one on a different stream
	// than the one awaiting it, is a connection PROTOCOL_ERROR (RFC 9113 §6.10).
	if h.pendingStreamID == 0 {
		return connError{code: frame.ErrCodeProtocolError, msg: "unexpected CONTINUATION"}
	}
	if fh.StreamID != h.pendingStreamID {
		return connError{code: frame.ErrCodeProtocolError, msg: "CONTINUATION on wrong stream"}
	}
	// Bound the accumulated compressed header block (CVE-2024-27316 defense).
	if len(h.pendingBuf)+len(hb) > h.maxHeaderBytes {
		return connError{code: frame.ErrCodeProtocolError, msg: "header block exceeds max size"}
	}
	h.pendingBuf = append(h.pendingBuf, hb...)
	if fh.Flags&frame.FlagContinuationEndHeaders == 0 {
		return nil
	}
	if h.pendingDiscard {
		// Refused stream: decode-and-discard the full block for HPACK sync.
		return h.discardHeaderBlock(h.pendingBuf)
	}
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		// Defensive (unreachable with a single reader goroutine): decode-and-
		// discard rather than drop, so the HPACK decoder cannot desync.
		return h.discardHeaderBlock(h.pendingBuf)
	}
	end := h.pendingEndStream
	isTrailer := h.pendingTrailer
	if !isTrailer {
		s.headersReceived = true
	}
	return h.emitHeaderBlock(s, h.pendingBuf, end, isTrailer)
}

func (h *serverConnHandler) emitHeaderBlock(s *ServerStream, hb []byte, endStream, isTrailer bool) error {
	// The header block is complete: clear the "awaiting CONTINUATION" state so
	// the interleaving guard re-admits other frame types. hb may alias
	// pendingBuf, so do NOT reset pendingBuf here — it is reused on the next
	// HEADERS via append(pendingBuf[:0], ...).
	h.pendingStreamID = 0
	h.scratch = h.scratch[:0]
	err := h.dec.DecodeBlock(hb, func(f hpack.HeaderField) error {
		h.scratch = append(h.scratch, f)
		return nil
	})
	if err != nil {
		_ = s.Close()
		return err
	}

	evType := EventHeaders
	if isTrailer {
		evType = EventTrailers
	}
	// Copy the decoded headers into a single right-sized backing slab the event
	// owns. The reader goroutine reuses h.scratch immediately after this returns,
	// and the handler goroutine may retain the fields (req.Headers alias the
	// slab), so a sync.Pool would be unsafe — like net/http we allocate per
	// request. Pre-sizing to the exact total means the appends never reallocate,
	// keeping the three-index sub-slices valid.
	total := 0
	for i := range h.scratch {
		total += len(h.scratch[i].Name) + len(h.scratch[i].Value)
	}
	slab := make([]byte, 0, total)
	copied := make([]hpack.HeaderField, len(h.scratch))
	for i, f := range h.scratch {
		nameOff := len(slab)
		slab = append(slab, f.Name...)
		valOff := len(slab)
		slab = append(slab, f.Value...)
		endOff := len(slab)
		copied[i] = hpack.HeaderField{
			Name:      slab[nameOff:valOff:valOff],
			Value:     slab[valOff:endOff:endOff],
			Sensitive: f.Sensitive,
		}
	}

	s.push(StreamEvent{
		Type:      evType,
		Headers:   copied,
		EndStream: endStream,
	})
	if endStream && s.markRemoteEnd() {
		// Release once both halves have ended (a half-closed-remote stream stays
		// registered for WINDOW_UPDATE/RST_STREAM, RFC 7540 §5.1). AFTER the push
		// so the handler observes the headers/EOF before the context is cancelled.
		h.streams.markStreamDone(s.id)
	}
	return nil
}

func (h *serverConnHandler) OnPriority(frame.FrameHeader, frame.Priority) error {
	return h.guardHeaderBlock()
}

func (h *serverConnHandler) OnRSTStream(fh frame.FrameHeader, code frame.ErrCode) error {
	if err := h.guardHeaderBlock(); err != nil {
		return err
	}
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		// RST_STREAM for an unknown/already-finished stream. A flood of
		// these (e.g. resetting streams the server already closed) is the
		// classic Rapid Reset signature, so still account it as rapid.
		return h.streams.onClientRSTStream(fh.StreamID, true)
	}
	// A reset is "rapid" (cheap-to-trigger, no useful work) when the
	// client tears the stream down before completing its request
	// (END_STREAM not yet observed). A reset arriving after the request
	// fully completed is a normal, benign cancellation.
	// Atomically end the remote half and learn whether the request was still
	// open (the rapid-reset signal), then hard-close the stream — RST is an
	// unconditional close regardless of the local half.
	rapid := s.markRemoteEndReset()
	// Deliver the reset event before releasing (and cancelling) the stream.
	s.push(StreamEvent{Type: EventReset, RSTCode: code, EndStream: true})
	h.streams.markStreamDone(fh.StreamID)
	return h.streams.onClientRSTStream(fh.StreamID, rapid)
}

func (h *serverConnHandler) OnSettings(fh frame.FrameHeader, s frame.SettingsParams) error {
	if err := h.guardHeaderBlock(); err != nil {
		return err
	}
	if fh.Flags&frame.FlagSettingsAck != 0 {
		return nil
	}
	if err := h.streams.applyPeerSettings(s); err != nil {
		return err
	}
	return h.streams.writeSettingsAck()
}

func (h *serverConnHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	// RFC 7540 §8.2: server must not receive PUSH_PROMISE from client.
	// Connection error of type PROTOCOL_ERROR.
	return connError{code: frame.ErrCodeProtocolError, msg: "server received PUSH_PROMISE"}
}

func (h *serverConnHandler) OnPing(fh frame.FrameHeader, payload [8]byte) error {
	if err := h.guardHeaderBlock(); err != nil {
		return err
	}
	if fh.Flags&frame.FlagPingAck != 0 {
		h.streams.deliverPingAck(payload)
		return nil
	}
	return h.streams.writePingAck(payload)
}

func (h *serverConnHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error {
	return h.guardHeaderBlock()
}

func (h *serverConnHandler) OnWindowUpdate(fh frame.FrameHeader, increment uint32) error {
	if err := h.guardHeaderBlock(); err != nil {
		return err
	}
	return h.streams.onWindowUpdate(fh.StreamID, increment)
}

// OnOrigin handles ORIGIN frames (RFC 8336). Silently ignored — server side.
func (h *serverConnHandler) OnOrigin(frame.FrameHeader, []string) error {
	return h.guardHeaderBlock()
}

func (h *serverConnHandler) OnAltSvc(frame.FrameHeader, []frame.AltSvcEntry) error {
	return h.guardHeaderBlock()
}

var _ frame.Handler = (*serverConnHandler)(nil)
