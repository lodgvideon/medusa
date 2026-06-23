package transport

import (
	"context"
	"errors"
	"sync"

	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
)

// ErrNoRoute is returned when no node is listening at the target address.
var ErrNoRoute = errors.New("medusa/transport: no node listening at address")

// ErrClosed is returned by operations on a closed transport.
var ErrClosed = errors.New("medusa/transport: transport closed")

// Switch is an in-process network of in-memory transports keyed by address.
// It lets a whole cluster run in one process with no sockets, which makes unit
// tests fast and deterministic.
type Switch struct {
	mu    sync.RWMutex
	nodes map[string]*inmem
}

// NewSwitch creates an empty in-process network.
func NewSwitch() *Switch {
	return &Switch{nodes: make(map[string]*inmem)}
}

// NewTransport returns a transport bound to addr within this switch.
func (s *Switch) NewTransport(addr string) Transport {
	return &inmem{addr: addr, sw: s}
}

func (s *Switch) lookup(addr string) (*inmem, bool) {
	s.mu.RLock()
	n, ok := s.nodes[addr]
	s.mu.RUnlock()
	return n, ok
}

type inmem struct {
	addr string
	sw   *Switch

	mu      sync.Mutex
	handler Handler
	closed  bool
}

func (t *inmem) Addr() string { return t.addr }

func (t *inmem) Listen(h Handler) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	t.handler = h
	t.sw.mu.Lock()
	t.sw.nodes[t.addr] = t
	t.sw.mu.Unlock()
	return nil
}

func (t *inmem) Send(ctx context.Context, addr string, reqType medusav1.MessageType, req, dst []byte) (medusav1.MessageType, []byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, dst, err
	}
	target, ok := t.sw.lookup(addr)
	if !ok {
		return 0, dst, ErrNoRoute
	}
	target.mu.Lock()
	h := target.handler
	closed := target.closed
	target.mu.Unlock()
	if closed || h == nil {
		return 0, dst, ErrNoRoute
	}

	// Copy the request so the handler never sees the caller's backing array,
	// mirroring the TCP transport where the request crosses a socket. This
	// faithfulness lets the shared test suite catch buffer-aliasing bugs.
	reqCopy := make([]byte, len(req))
	copy(reqCopy, req)

	respBuf := make([]byte, 0, 256) // fresh per call: faithful and concurrency-safe
	respType, resp, herr := h(reqType, reqCopy, respBuf)
	if herr != nil {
		return medusav1.MessageType_MESSAGE_TYPE_ERROR, dst, &RemoteError{Code: 1, Message: herr.Error()}
	}

	// Copy the response into the caller's buffer: after Send returns, the
	// handler is free to reuse the buffer backing resp.
	if cap(dst) < len(resp) {
		dst = make([]byte, len(resp))
	} else {
		dst = dst[:len(resp)]
	}
	copy(dst, resp)
	return respType, dst, nil
}

func (t *inmem) Close() error {
	t.mu.Lock()
	t.closed = true
	t.handler = nil
	t.mu.Unlock()
	t.sw.mu.Lock()
	delete(t.sw.nodes, t.addr)
	t.sw.mu.Unlock()
	return nil
}
