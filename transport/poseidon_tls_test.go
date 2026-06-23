package transport_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/transport"
)

// mtlsConfig builds a *tls.Config suitable for both ends of a mutually
// authenticated connection: a fresh self-signed CA certificate (valid for
// 127.0.0.1) is presented as the leaf and trusted as the root, and client
// certs are required and verified against it. Each call mints an independent
// trust domain, so two configs from separate calls do not trust each other.
func mtlsConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "medusa-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}
}

// TestPoseidonTLS proves an RPC round-trips over mutually-authenticated TLS:
// the server presents its cert, the client verifies it and presents its own,
// and the server verifies the client — all with the shared trust domain.
func TestPoseidonTLS(t *testing.T) {
	cfg := mtlsConfig(t)
	echo := func(rt medusav1.MessageType, body, _ []byte) (medusav1.MessageType, []byte, error) {
		return medusav1.MessageType_MESSAGE_TYPE_GET_RESPONSE, body, nil
	}
	srv := transport.NewPoseidonTLS("127.0.0.1:0", cfg)
	if err := srv.Listen(echo); err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	cli := transport.NewPoseidonTLS("127.0.0.1:0", cfg)
	t.Cleanup(func() { _ = cli.Close() })

	timeout := 10 * time.Second
	if raceEnabled {
		timeout = 40 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rt, resp, err := cli.Send(ctx, srv.Addr(),
		medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST, []byte("over-tls"), nil)
	if err != nil {
		t.Fatalf("send over TLS: %v", err)
	}
	if rt != medusav1.MessageType_MESSAGE_TYPE_GET_RESPONSE || string(resp) != "over-tls" {
		t.Fatalf("got type=%v body=%q, want GET_RESPONSE \"over-tls\"", rt, resp)
	}
}

// TestPoseidonTLSRejectsUntrusted proves verification is enforced: a client in a
// different trust domain cannot complete the handshake, so the RPC fails rather
// than silently downgrading.
func TestPoseidonTLSRejectsUntrusted(t *testing.T) {
	srv := transport.NewPoseidonTLS("127.0.0.1:0", mtlsConfig(t))
	if err := srv.Listen(func(rt medusav1.MessageType, b, _ []byte) (medusav1.MessageType, []byte, error) {
		return medusav1.MessageType_MESSAGE_TYPE_GET_RESPONSE, b, nil
	}); err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	// A client whose trust domain does not include the server's cert.
	cli := transport.NewPoseidonTLS("127.0.0.1:0", mtlsConfig(t))
	t.Cleanup(func() { _ = cli.Close() })

	timeout := 10 * time.Second
	if raceEnabled {
		timeout = 40 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if _, _, err := cli.Send(ctx, srv.Addr(),
		medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST, []byte("x"), nil); err == nil {
		t.Fatal("expected the TLS handshake to fail for an untrusted server cert")
	}
}
