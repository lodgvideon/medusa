// Package conn implements the server-side HTTP/2 connection state machine.
//
// It reuses the frame and hpack codec layers from poseidon-http-client and
// adds server-specific logic: accepting inbound streams, server-perspective
// SETTINGS handshake, flow control for received data, and GOAWAY drain.
package conn
