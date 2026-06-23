package transport

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/lodgvideon/medusa/codec"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	pclient "github.com/lodgvideon/poseidon-http-client/client"
	pconn "github.com/lodgvideon/poseidon-http-client/conn"
	psconn "github.com/lodgvideon/poseidon-http-server/conn"
	pserver "github.com/lodgvideon/poseidon-http-server/server"
)

// headerMsgType carries the medusa message type on both request and response.
const headerMsgType = "m-type"

// initialWindow advertises a 16 MiB HTTP/2 stream window (vs. the 64 KiB
// default) on both ends. This is the effective per-message ceiling: the client
// chunks a body into 16 KiB DATA frames and the server refunds the connection
// window as it reads, but poseidon-http v0.3.0 does not refund the per-stream
// window mid-read, so a single message must fit within this window. 16 MiB is
// ample for data-grid values; raise it (max 2^31-1) for larger payloads.
const initialWindow uint32 = 16 << 20 // 16 MiB

// poseidonTransport implements Transport over the Poseidon HTTP/2 stack: the
// poseidon-http-client (h2c, prior-knowledge) for Send, and the
// poseidon-http-server (h2c) for Listen. Each medusa RPC is one HTTP/2 POST —
// the message type rides in the m-type header, the protobuf payload is the
// body. A handler error becomes a 500 carrying a protobuf Error, decoded back
// into a *RemoteError so callers see the same error type as the raw transport.
type poseidonTransport struct {
	mu       sync.Mutex
	addr     string
	listener net.Listener
	srv      *pserver.Server
	cancel   context.CancelFunc // cancels the Serve context on Close
	closed   bool
	serveWG  sync.WaitGroup

	clientMu sync.Mutex
	clients  map[string]*pclient.Client // one HTTP/2 client per target address
}

// NewPoseidon returns a Transport backed by the Poseidon HTTP/2 client and
// server over cleartext (h2c). addr is the listen address; host:0 is resolved
// after Listen. A client-only transport need not call Listen.
func NewPoseidon(addr string) Transport {
	return &poseidonTransport{addr: addr, clients: make(map[string]*pclient.Client)}
}

func (t *poseidonTransport) Addr() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.addr
}

func (t *poseidonTransport) Listen(h Handler) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	ln, err := net.Listen("tcp", t.addr)
	if err != nil {
		return err
	}
	srv, err := pserver.NewServer(pserver.Options{
		HTTPHandler: t.httpHandler(h),
		H2C:         true,
		ConnOpts: psconn.ServerConnOptions{
			AdvertisedSettings: psconn.AdvertisedSettings{InitialWindowSize: initialWindow},
		},
	})
	if err != nil {
		_ = ln.Close()
		return err
	}
	// Serve must get a cancelable context: Poseidon's shutdown watcher blocks on
	// ctx.Done(), and context.Background().Done() is nil, so Close could never
	// make Serve return. We cancel this in Close.
	ctx, cancel := context.WithCancel(context.Background())
	t.listener = ln
	t.addr = ln.Addr().String()
	t.srv = srv
	t.cancel = cancel

	t.serveWG.Add(1)
	go func() {
		defer t.serveWG.Done()
		_ = srv.Serve(ctx, ln)
	}()
	return nil
}

// httpHandler adapts a medusa Handler to a net/http handler served over h2c.
func (t *poseidonTransport) httpHandler(h Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqType, _ := strconv.Atoi(r.Header.Get(headerMsgType))
		// Poseidon's net/http compatibility layer leaves r.Body nil for an
		// empty-body request (unlike net/http, which guarantees a non-nil body).
		var body []byte
		if r.Body != nil {
			b, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read body", http.StatusBadRequest)
				return
			}
			body = b
		}

		respType, resp, herr := h(medusav1.MessageType(reqType), body, nil)
		if herr != nil {
			payload, _ := codec.Marshal(nil, &medusav1.Error{Code: 1, Message: herr.Error()})
			w.Header().Set(headerMsgType, strconv.Itoa(int(medusav1.MessageType_MESSAGE_TYPE_ERROR)))
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write(payload)
			return
		}
		w.Header().Set(headerMsgType, strconv.Itoa(int(respType)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(resp)
	})
}

func (t *poseidonTransport) clientFor(addr string) (*pclient.Client, error) {
	t.clientMu.Lock()
	defer t.clientMu.Unlock()
	if t.closed {
		return nil, ErrClosed
	}
	if c, ok := t.clients[addr]; ok {
		return c, nil
	}
	c, err := pclient.NewClient(pclient.ClientOptions{
		Addr:          addr,
		DefaultScheme: "http", // h2c
		ConnOpts: pconn.ConnOptions{
			Dialer:   &pconn.PlaintextDialer{},
			Settings: pconn.AdvertisedSettings{InitialWindowSize: initialWindow},
		},
	})
	if err != nil {
		return nil, err
	}
	t.clients[addr] = c
	return c, nil
}

func (t *poseidonTransport) Send(ctx context.Context, addr string, reqType medusav1.MessageType, req, dst []byte) (medusav1.MessageType, []byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, dst, err
	}
	cl, err := t.clientFor(addr)
	if err != nil {
		return 0, dst, err
	}

	httpReq := pclient.Request{
		Method:   http.MethodPost,
		Path:     "/m",
		Headers:  []pconn.HeaderField{{Name: []byte(headerMsgType), Value: []byte(strconv.Itoa(int(reqType)))}},
		Body:     req,
		WantBody: true,
	}
	var resp pclient.Response
	if err := cl.Do(ctx, &httpReq, &resp); err != nil {
		return 0, dst, err
	}

	respType := medusav1.MessageType(headerInt(resp.Headers, headerMsgType))
	if resp.Status != http.StatusOK {
		var e medusav1.Error
		_ = e.UnmarshalVT(resp.Body)
		return respType, dst, &RemoteError{Code: e.Code, Message: e.Message}
	}
	// resp.Body is only valid until resp.Reset(); copy into the caller's buffer.
	dst = append(dst[:0], resp.Body...)
	return respType, dst, nil
}

func (t *poseidonTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	srv := t.srv
	ln := t.listener
	cancel := t.cancel
	t.mu.Unlock()

	t.clientMu.Lock()
	for _, c := range t.clients {
		_ = c.Close()
	}
	t.clients = make(map[string]*pclient.Client)
	t.clientMu.Unlock()

	if cancel != nil {
		cancel() // unblock Poseidon's ctx.Done() shutdown watcher
	}
	if srv != nil {
		_ = srv.Close()
	}
	// Always close the listener: srv.Close() does not unblock the Accept loop
	// when no connection was ever made, so Serve would not return otherwise.
	if ln != nil {
		_ = ln.Close()
	}
	t.serveWG.Wait()
	return nil
}

// headerInt returns the integer value of the named header field, or 0.
func headerInt(fields []pconn.HeaderField, name string) int {
	for _, f := range fields {
		if string(f.Name) == name {
			n, _ := strconv.Atoi(string(f.Value))
			return n
		}
	}
	return 0
}
