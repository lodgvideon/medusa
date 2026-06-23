// Package transport provides a synchronous request/response message transport
// between cluster nodes.
//
// A wire frame is [uint32 big-endian payload length][1 byte message type]
// [payload]. The single type byte lets the receiver decode the correct
// concrete vtprotobuf message without a wrapping oneof, keeping the hot path
// allocation-free.
//
// Two implementations satisfy Transport: an in-memory Switch for fast,
// deterministic tests, and a TCP transport for real networking. They share the
// same behavioural contract and test suite.
package transport

import (
	"context"
	"fmt"

	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
)

// Handler processes an inbound request and returns a response.
//
// req aliases a transport-owned buffer valid only for the duration of the
// call; a handler that retains the bytes must copy them.
//
// respBuf is a reusable response buffer owned by the transport. The handler
// must marshal its reply into it — typically via codec.Marshal(respBuf, msg) —
// and return the resulting slice as resp. The transport reuses resp's backing
// array for the next call, so a handler must NOT return a sub-slice of req or
// any other buffer it does not want overwritten. Returning a reply built from
// respBuf keeps the server response path allocation-free.
//
// Handlers must be safe for concurrent calls: the TCP transport serves each
// connection on its own goroutine and the in-memory transport invokes the
// handler directly from each caller's goroutine. Each concurrent call receives
// its own respBuf, so reuse is safe.
type Handler func(reqType medusav1.MessageType, req, respBuf []byte) (respType medusav1.MessageType, resp []byte, err error)

// Transport is a request/response message transport between nodes.
type Transport interface {
	// Addr is the address remote nodes use to reach this transport.
	Addr() string
	// Listen starts serving inbound requests with h. Call it at most once.
	Listen(h Handler) error
	// Send issues a request to addr and reads the response into dst, growing it
	// if necessary. The returned slice aliases dst and remains valid until the
	// caller next reuses dst.
	Send(ctx context.Context, addr string, reqType medusav1.MessageType, req, dst []byte) (medusav1.MessageType, []byte, error)
	// Close stops serving and releases resources. It is safe to call more than once.
	Close() error
}

// RemoteError is returned by Send when the remote handler reported an error.
type RemoteError struct {
	Code    uint32
	Message string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("medusa/transport: remote error %d: %s", e.Code, e.Message)
}
