package server

import (
	"context"
	"crypto/tls"
	"net"
)

// ---------------------------------------------------------------------------
// TLS + ALPN — RFC 7540 §3.3 (HTTP/2 over TLS)
// ---------------------------------------------------------------------------

// ListenAndServeTLS listens on the TCP address in Options.Addr and
// serves HTTPS (HTTP/2 over TLS with ALPN negotiation).
//
// The TLS config is configured with NextProtos = ["h2"] so that clients
// negotiating ALPN will select HTTP/2.
func (s *Server) ListenAndServeTLS(ctx context.Context, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h2"},
		MinVersion:   tls.VersionTLS12,
	}
	return s.ListenAndServeTLSConfig(ctx, tlsConfig)
}

// ListenAndServeTLSConfig listens with a custom *tls.Config.
// The caller must set NextProtos to include "h2".
func (s *Server) ListenAndServeTLSConfig(ctx context.Context, cfg *tls.Config) error {
	// Ensure "h2" is in NextProtos.
	hasH2 := false
	for _, p := range cfg.NextProtos {
		if p == "h2" {
			hasH2 = true
			break
		}
	}
	if !hasH2 {
		cfg.NextProtos = append(cfg.NextProtos, "h2")
	}

	ln, err := tls.Listen("tcp", s.opts.Addr, cfg)
	if err != nil {
		return err
	}
	s.logger.Printf("poseidon: TLS listening on %s", ln.Addr())
	return s.Serve(ctx, ln)
}

// ServeTLS serves on an existing TLS listener.
func (s *Server) ServeTLS(ctx context.Context, ln net.Listener) error {
	return s.Serve(ctx, ln)
}
