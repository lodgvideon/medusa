package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/lodgvideon/poseidon-http-server/conn"
)

// ---------------------------------------------------------------------------
// h2c (HTTP/2 Cleartext) support — RFC 7540 §3.2, §3.4
// ---------------------------------------------------------------------------

// h2cPreface is the first bytes of the HTTP/2 client connection preface.
var h2cPreface = []byte("PRI * HTTP/2.0")

// detectAndServe performs h2c detection before passing to serveConn.
// If the client sends HTTP/1.1 Upgrade: h2c, we respond with 101 Switching.
// If the client sends the preface directly (prior knowledge), we pass through.
// Otherwise we respond with 400 Bad Request.
func (s *Server) detectAndServe(ctx context.Context, nc net.Conn) {
	br := bufio.NewReaderSize(nc, 1024)

	// Peek first bytes to determine protocol.
	head, err := br.Peek(len(h2cPreface))
	if err != nil {
		// Short read — likely connection closed or timeout.
		_ = nc.Close()
		return
	}

	if bytes.Equal(head, h2cPreface) {
		// Prior knowledge h2c — client speaks HTTP/2 directly.
		s.serveConnReader(ctx, nc, br)
		return
	}

	// Could be HTTP/1.1 with Upgrade: h2c, or plain HTTP/1.1.
	s.handleHTTP1Upgrade(ctx, nc, br)
}

// handleHTTP1Upgrade processes an HTTP/1.1 request that may contain
// an Upgrade: h2c header. If so, responds with 101 Switching Protocols
// and continues as HTTP/2. Otherwise returns 400.
func (s *Server) handleHTTP1Upgrade(ctx context.Context, nc net.Conn, br *bufio.Reader) {
	// Set a deadline to avoid hanging on malformed input.
	if deadline, ok := ctx.Deadline(); ok {
		_ = nc.SetReadDeadline(deadline)
	}

	req, err := http.ReadRequest(br)
	if err != nil {
		_ = nc.Close()
		return
	}

	// Check for Upgrade: h2c
	if !strings.EqualFold(req.Header.Get("Upgrade"), "h2c") &&
		!strings.EqualFold(req.Header.Get("Upgrade"), "h2") {
		// Not an upgrade request — respond 400.
		resp := fmt.Sprintf("HTTP/1.1 400 Bad Request\r\n"+
			"Content-Type: text/plain\r\n"+
			"Connection: close\r\n"+
			"Content-Length: 19\r\n\r\n"+
			"Only h2c supported\n")
		_, _ = nc.Write([]byte(resp))
		_ = nc.Close()
		return
	}

	// Send 101 Switching Protocols.
	_, _ = fmt.Fprintf(nc,
		"HTTP/1.1 101 Switching Protocols\r\n"+
			"Connection: Upgrade\r\n"+
			"Upgrade: h2c\r\n\r\n")

	// Now the client should send the HTTP/2 preface.
	s.serveConnReader(ctx, nc, br)
}

// serveConnReader wraps serveConn but uses a buffered reader that may
// have already consumed some bytes from the connection.
func (s *Server) serveConnReader(ctx context.Context, nc net.Conn, br *bufio.Reader) {
	// If the bufio reader has buffered data, we need to present a
	// combined reader to NewServerConn. Wrap with bufioReaderConn.
	rwc := &bufioConn{Conn: nc, Reader: br}
	opts := s.connOpts
	if opts.StreamEventBuffer <= 0 {
		opts.StreamEventBuffer = 8
	}

	sc, err := conn.NewServerConn(ctx, rwc, opts)
	if err != nil {
		s.logger.Printf("poseidon: h2c handshake failed for %s: %v", nc.RemoteAddr(), err)
		_ = nc.Close()
		return
	}

	s.trackConn(sc, true)
	defer s.trackConn(sc, false)

	s.acceptLoop(ctx, sc)
}

// bufioConn wraps a net.Conn with a bufio.Reader so that peeked bytes
// are not lost when passing to conn.NewServerConn.
type bufioConn struct {
	net.Conn
	Reader *bufio.Reader
}

func (c *bufioConn) Read(b []byte) (int, error) {
	return c.Reader.Read(b)
}

func (c *bufioConn) Write(b []byte) (int, error) {
	return c.Conn.Write(b)
}
