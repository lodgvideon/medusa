package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
)

// TestGetConnHonorsContext is the regression guard for the unbounded-dial stall
// found by the deep review: the connection dial used net.Dial, which ignores the
// context, so a connect to an unreachable peer blocked for the OS-level TCP
// timeout (~130s) — long enough to freeze the time-boxed rebalance loop (and with
// it gossip and failure detection). getConn now dials with DialContext, so a
// caller's deadline bounds the connect. With an already-elapsed deadline the dial
// must fail immediately with the context error rather than reaching out to the
// (here, reserved/unroutable) address.
func TestGetConnHonorsContext(t *testing.T) {
	tr := NewTCP("127.0.0.1:0").(*tcpTransport)
	t.Cleanup(func() { _ = tr.Close() })

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
	defer cancel()

	// 240.0.0.1 is in the reserved 240.0.0.0/4 block: a real dial would never
	// connect. With a context-honoring dial this returns at once with the context
	// error; the old net.Dial would ignore the context and attempt the connect.
	_, err := tr.getConn(ctx, "240.0.0.1:9")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("getConn err = %v, want context.DeadlineExceeded (the dial must honor ctx)", err)
	}
}

// TestSendBoundsDeadlinelessRequest is the regression guard for the failover
// stall: a Send whose context carries NO deadline must still be bounded (by
// defaultIOTimeout), so a peer that accepts but never replies — e.g. a stale
// pooled connection to a node that just died — cannot hang the request forever
// and starve failover to a backup. Before the fix, SetDeadline(time.Time{}) left
// the read unbounded and it blocked for the OS TCP timeout (minutes).
func TestSendBoundsDeadlinelessRequest(t *testing.T) {
	old := defaultIOTimeout
	defaultIOTimeout = 200 * time.Millisecond
	t.Cleanup(func() { defaultIOTimeout = old })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	// Accept connections but never reply, holding them open so the client's read
	// blocks (rather than seeing EOF) until the deadline fires.
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		mu.Lock()
		for _, c := range held {
			_ = c.Close()
		}
		mu.Unlock()
	})

	tr := NewTCP("127.0.0.1:0").(*tcpTransport)
	t.Cleanup(func() { _ = tr.Close() })

	start := time.Now()
	_, _, err = tr.Send(context.Background(), ln.Addr().String(),
		medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST, []byte("x"), nil)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("want a timeout error from a peer that never replies")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("Send hung %v with a deadline-less context; the default IO timeout was not applied", elapsed)
	}
}
