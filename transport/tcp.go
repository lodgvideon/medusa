package transport

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/lodgvideon/medusa/codec"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
)

// maxPoolPerAddr bounds idle client connections cached per remote address.
const maxPoolPerAddr = 8

// NewTCP returns a TCP transport. addr is the listen address; pass host:0 to
// let the OS choose a port (resolved and visible via Addr after Listen). A
// transport used only as a client need not call Listen.
func NewTCP(addr string) Transport {
	return &tcpTransport{
		addr:    addr,
		conns:   make(map[string]chan *clientConn),
		serving: make(map[net.Conn]struct{}),
	}
}

type tcpTransport struct {
	mu      sync.Mutex
	addr    string
	ln      net.Listener
	closed  bool
	conns   map[string]chan *clientConn // idle client connections, pooled per remote addr
	serving map[net.Conn]struct{}       // active inbound connections, closed on shutdown
	serveWG sync.WaitGroup
}

type clientConn struct {
	c   net.Conn
	hdr [headerSize]byte
}

func (t *tcpTransport) Addr() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.addr
}

func (t *tcpTransport) Listen(h Handler) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrClosed
	}
	ln, err := net.Listen("tcp", t.addr)
	if err != nil {
		t.mu.Unlock()
		return err
	}
	t.ln = ln
	t.addr = ln.Addr().String() // resolve host:0 to the chosen port
	t.mu.Unlock()

	t.serveWG.Add(1)
	go t.acceptLoop(ln, h)
	return nil
}

func (t *tcpTransport) acceptLoop(ln net.Listener, h Handler) {
	defer t.serveWG.Done()
	for {
		c, err := ln.Accept()
		if err != nil {
			return // listener closed
		}
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		t.serveWG.Add(1)
		go t.serveConn(c, h)
	}
}

func (t *tcpTransport) serveConn(c net.Conn, h Handler) {
	defer t.serveWG.Done()
	defer c.Close()

	// Register so Close can force this connection shut, unblocking the read
	// loop below. If we are already shutting down, leave immediately.
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.serving[c] = struct{}{}
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		delete(t.serving, c)
		t.mu.Unlock()
	}()

	var hdr [headerSize]byte
	buf := make([]byte, 0, 512)     // per-connection reusable request buffer
	respBuf := make([]byte, 0, 512) // per-connection reusable response buffer
	var errBuf []byte               // reused only on the (rare) error path

	for {
		reqType, payload, err := readFrame(c, hdr[:], buf)
		buf = payload
		if err != nil {
			return
		}

		respType, resp, herr := h(reqType, payload, respBuf)
		if herr != nil {
			em := medusav1.Error{Code: 1, Message: herr.Error()}
			errBuf, _ = codec.Marshal(errBuf, &em)
			if err := writeFrame(c, hdr[:], medusav1.MessageType_MESSAGE_TYPE_ERROR, errBuf); err != nil {
				return
			}
			continue
		}
		if err := writeFrame(c, hdr[:], respType, resp); err != nil {
			return
		}
		respBuf = resp[:0] // adopt any growth so the next response reuses it
	}
}

func (t *tcpTransport) Send(ctx context.Context, addr string, reqType medusav1.MessageType, req, dst []byte) (medusav1.MessageType, []byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, dst, err
	}
	cc, err := t.getConn(addr)
	if err != nil {
		return 0, dst, err
	}

	if dl, ok := ctx.Deadline(); ok {
		_ = cc.c.SetDeadline(dl)
	} else {
		_ = cc.c.SetDeadline(time.Time{})
	}

	if err := writeFrame(cc.c, cc.hdr[:], reqType, req); err != nil {
		_ = cc.c.Close()
		return 0, dst, err
	}
	respType, dst, err := readFrame(cc.c, cc.hdr[:], dst)
	if err != nil {
		_ = cc.c.Close()
		return respType, dst, err
	}
	t.putConn(addr, cc)

	if respType == medusav1.MessageType_MESSAGE_TYPE_ERROR {
		var e medusav1.Error
		if uerr := e.UnmarshalVT(dst); uerr != nil {
			return respType, dst, &RemoteError{Message: "undecodable remote error"}
		}
		return respType, dst, &RemoteError{Code: e.Code, Message: e.Message}
	}
	return respType, dst, nil
}

func (t *tcpTransport) getConn(addr string) (*clientConn, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, ErrClosed
	}
	ch := t.conns[addr]
	if ch == nil {
		ch = make(chan *clientConn, maxPoolPerAddr)
		t.conns[addr] = ch
	}
	t.mu.Unlock()

	select {
	case cc := <-ch:
		return cc, nil
	default:
	}

	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &clientConn{c: c}, nil
}

func (t *tcpTransport) putConn(addr string, cc *clientConn) {
	// Hold t.mu across the channel send. Close drains and replaces the pool under
	// t.mu, so if we released the lock first it could swap the map and drain the
	// channel in the gap, and our send would strand cc in an orphaned channel
	// that nothing ever closes. Serialising here means either Close runs first
	// (we observe closed and close cc) or we put cc into the live channel before
	// Close drains it (Close then closes it). The send is non-blocking.
	t.mu.Lock()
	defer t.mu.Unlock()
	ch := t.conns[addr]
	if t.closed || ch == nil {
		_ = cc.c.Close()
		return
	}
	select {
	case ch <- cc:
	default:
		_ = cc.c.Close() // pool full
	}
}

func (t *tcpTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	ln := t.ln
	conns := t.conns
	t.conns = make(map[string]chan *clientConn)
	serving := t.serving
	t.serving = make(map[net.Conn]struct{})
	t.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	// Force-close active inbound connections so their blocked reads return and
	// the serveConn goroutines exit; otherwise serveWG.Wait would deadlock
	// waiting on peers that have not closed their side.
	for c := range serving {
		_ = c.Close()
	}
	// Drain idle pooled connections. We never close the channels: a late
	// putConn re-reads the (now empty) map under the lock, finds no channel,
	// and closes its connection directly — so there is no send-on-closed race.
	for _, ch := range conns {
		for {
			select {
			case cc := <-ch:
				_ = cc.c.Close()
			default:
				goto nextPool
			}
		}
	nextPool:
	}
	t.serveWG.Wait()
	return nil
}
