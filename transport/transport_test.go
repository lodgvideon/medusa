package transport_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/transport"
)

// pairFactory builds a client transport plus the address of a server listening
// with handler h. Cleanup is registered on t.
type pairFactory func(t *testing.T, h transport.Handler) (client transport.Transport, serverAddr string)

func inmemPair(t *testing.T, h transport.Handler) (transport.Transport, string) {
	sw := transport.NewSwitch()
	srv := sw.NewTransport("server")
	if err := srv.Listen(h); err != nil {
		t.Fatalf("server Listen: %v", err)
	}
	cli := sw.NewTransport("client")
	t.Cleanup(func() { _ = srv.Close(); _ = cli.Close() })
	return cli, "server"
}

func tcpPair(t *testing.T, h transport.Handler) (transport.Transport, string) {
	srv := transport.NewTCP("127.0.0.1:0")
	if err := srv.Listen(h); err != nil {
		t.Fatalf("server Listen: %v", err)
	}
	cli := transport.NewTCP("127.0.0.1:0") // client never listens
	t.Cleanup(func() { _ = srv.Close(); _ = cli.Close() })
	return cli, srv.Addr()
}

func poseidonPair(t *testing.T, h transport.Handler) (transport.Transport, string) {
	srv := transport.NewPoseidon("127.0.0.1:0")
	if err := srv.Listen(h); err != nil {
		t.Fatalf("server Listen: %v", err)
	}
	cli := transport.NewPoseidon("127.0.0.1:0") // client never listens
	t.Cleanup(func() { _ = srv.Close(); _ = cli.Close() })
	return cli, srv.Addr()
}

var factories = []struct {
	name string
	pair pairFactory
}{
	{"inmem", inmemPair},
	{"tcp", tcpPair},
	{"poseidon", poseidonPair},
}

// echo returns the request payload back as a GET_RESPONSE, copied into the
// transport-supplied response buffer per the Handler contract.
func echo(reqType medusav1.MessageType, req, respBuf []byte) (medusav1.MessageType, []byte, error) {
	return medusav1.MessageType_MESSAGE_TYPE_GET_RESPONSE, append(respBuf[:0], req...), nil
}

func eachTransport(t *testing.T, fn func(t *testing.T, pair pairFactory)) {
	for _, f := range factories {
		t.Run(f.name, func(t *testing.T) { fn(t, f.pair) })
	}
}

func TestSendReceive(t *testing.T) {
	eachTransport(t, func(t *testing.T, pair pairFactory) {
		cli, addr := pair(t, echo)
		respType, resp, err := cli.Send(context.Background(),
			addr, medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST, []byte("ping"), nil)
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
		if respType != medusav1.MessageType_MESSAGE_TYPE_GET_RESPONSE {
			t.Errorf("respType = %v, want GET_RESPONSE", respType)
		}
		if !bytes.Equal(resp, []byte("ping")) {
			t.Errorf("resp = %q, want %q", resp, "ping")
		}
	})
}

func TestEmptyPayload(t *testing.T) {
	eachTransport(t, func(t *testing.T, pair pairFactory) {
		cli, addr := pair(t, echo)
		_, resp, err := cli.Send(context.Background(),
			addr, medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST, nil, nil)
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
		if len(resp) != 0 {
			t.Errorf("resp len = %d, want 0", len(resp))
		}
	})
}

func TestLargePayloadGrowsBuffer(t *testing.T) {
	eachTransport(t, func(t *testing.T, pair pairFactory) {
		cli, addr := pair(t, echo)
		big := bytes.Repeat([]byte("z"), 1<<16) // 64 KiB, larger than the 512 server buffer
		_, resp, err := cli.Send(context.Background(),
			addr, medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST, big, make([]byte, 0, 8))
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
		if !bytes.Equal(resp, big) {
			t.Errorf("resp len = %d, want %d", len(resp), len(big))
		}
	})
}

// TestPoseidonMegabyteRequest proves the Poseidon transport streams a 1 MiB
// request body (the shape of a Put of a large value): the client chunks it into
// 16 KiB DATA frames and the server refunds the connection window as it reads.
//
// It is deliberately one-directional (large request, tiny ack). medusa never
// echoes a large payload both ways; symmetric large transfers stress Poseidon's
// bidirectional flow control, which is separately known to be unstable. Each
// attempt has its own deadline so a stall fails fast rather than hanging, with
// a retry to ride out the occasional REFUSED_STREAM under concurrent churn.
func TestPoseidonMegabyteRequest(t *testing.T) {
	drain := func(medusav1.MessageType, []byte, []byte) (medusav1.MessageType, []byte, error) {
		return medusav1.MessageType_MESSAGE_TYPE_PUT_RESPONSE, nil, nil // ack only
	}
	cli, addr := poseidonPair(t, drain)
	payload := bytes.Repeat([]byte("v"), 1<<20) // 1 MiB

	var err error
	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		_, _, err = cli.Send(ctx, addr,
			medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST, payload, nil)
		cancel()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("1 MiB request after retries: %v", err)
	}
}

func TestRemoteError(t *testing.T) {
	eachTransport(t, func(t *testing.T, pair pairFactory) {
		failing := func(medusav1.MessageType, []byte, []byte) (medusav1.MessageType, []byte, error) {
			return 0, nil, errors.New("boom")
		}
		cli, addr := pair(t, failing)
		_, _, err := cli.Send(context.Background(),
			addr, medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST, []byte("x"), nil)
		if err == nil {
			t.Fatal("expected an error")
		}
		var re *transport.RemoteError
		if !errors.As(err, &re) {
			t.Fatalf("err type = %T, want *transport.RemoteError", err)
		}
		if re.Message != "boom" {
			t.Errorf("remote message = %q, want %q", re.Message, "boom")
		}
	})
}

func TestSendToUnknownAddrFails(t *testing.T) {
	eachTransport(t, func(t *testing.T, pair pairFactory) {
		cli, _ := pair(t, echo)
		_, _, err := cli.Send(context.Background(),
			"127.0.0.1:1", medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST, []byte("x"), nil)
		if err == nil {
			t.Fatal("expected an error sending to a dead address")
		}
	})
}

func TestCanceledContext(t *testing.T) {
	eachTransport(t, func(t *testing.T, pair pairFactory) {
		cli, addr := pair(t, echo)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _, err := cli.Send(ctx,
			addr, medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST, []byte("x"), nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
}

func TestCloseIsIdempotent(t *testing.T) {
	eachTransport(t, func(t *testing.T, pair pairFactory) {
		cli, _ := pair(t, echo)
		if err := cli.Close(); err != nil {
			t.Fatalf("first Close: %v", err)
		}
		if err := cli.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	})
}

func TestListenAfterCloseFails(t *testing.T) {
	t.Run("inmem", func(t *testing.T) {
		sw := transport.NewSwitch()
		tr := sw.NewTransport("n1")
		_ = tr.Close()
		if err := tr.Listen(echo); !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	})
	t.Run("tcp", func(t *testing.T) {
		tr := transport.NewTCP("127.0.0.1:0")
		_ = tr.Close()
		if err := tr.Listen(echo); !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	})
}

func TestSendWithDeadline(t *testing.T) {
	eachTransport(t, func(t *testing.T, pair pairFactory) {
		cli, addr := pair(t, echo)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, resp, err := cli.Send(ctx, addr,
			medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST, []byte("ping"), nil)
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
		if !bytes.Equal(resp, []byte("ping")) {
			t.Errorf("resp = %q, want ping", resp)
		}
	})
}

func TestRemoteErrorString(t *testing.T) {
	e := &transport.RemoteError{Code: 7, Message: "nope"}
	if !strings.Contains(e.Error(), "nope") || !strings.Contains(e.Error(), "7") {
		t.Errorf("Error() = %q, want it to mention code and message", e.Error())
	}
}

func TestInmemAddr(t *testing.T) {
	sw := transport.NewSwitch()
	tr := sw.NewTransport("xyz")
	if got := tr.Addr(); got != "xyz" {
		t.Errorf("Addr() = %q, want xyz", got)
	}
}

func TestTCPListenOnBusyPortFails(t *testing.T) {
	a := transport.NewTCP("127.0.0.1:0")
	if err := a.Listen(echo); err != nil {
		t.Fatalf("first Listen: %v", err)
	}
	defer a.Close()

	b := transport.NewTCP(a.Addr()) // same address, already bound
	if err := b.Listen(echo); err == nil {
		_ = b.Close()
		t.Fatal("expected Listen to fail on an in-use address")
	}
}

func TestConcurrentSends(t *testing.T) {
	eachTransport(t, func(t *testing.T, pair pairFactory) {
		cli, addr := pair(t, echo)
		const goroutines, perG = 16, 50
		var wg sync.WaitGroup
		errs := make(chan error, goroutines)
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(id byte) {
				defer wg.Done()
				dst := make([]byte, 0, 64) // each goroutine reuses its own buffer
				want := []byte{id, id, id}
				for i := 0; i < perG; i++ {
					var err error
					_, resp, err := cli.Send(context.Background(),
						addr, medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST, want, dst)
					if err != nil {
						errs <- err
						return
					}
					if !bytes.Equal(resp, want) {
						errs <- errors.New("response mismatch under concurrency")
						return
					}
					dst = resp
				}
			}(byte(g))
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent Send: %v", err)
		}
	})
}
