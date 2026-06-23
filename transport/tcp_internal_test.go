package transport

import (
	"context"
	"errors"
	"testing"
	"time"
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
