// Command medusa-node runs a single medusa cluster member, configured from the
// environment, with an HTTP admin/health surface. It is the artifact deployed
// in containers and Kubernetes.
//
// Environment:
//
//	MEDUSA_ID         unique node id (default: hostname)
//	MEDUSA_ADDR       data-plane listen+advertise address peers dial
//	                  (default: ":7700"; in k8s set to "$(POD_IP):7700")
//	MEDUSA_HTTP_ADDR  admin/health listen address (default: ":8080")
//	MEDUSA_SEEDS      comma-separated seed addresses to join (optional)
//	MEDUSA_DISCOVERY  peer discovery: unset/"static" uses MEDUSA_SEEDS;
//	                  "dns:<host>" resolves <host> (e.g. a headless Service) to
//	                  peer IPs each tick; "dns:<host>:<port>" sets the port
//	MEDUSA_BACKUPS    backup copies per partition / replication factor − 1
//	                  (default: 1; values below 1 are treated as 1)
//	MEDUSA_MAX_ENTRIES soft per-node entry cap before eviction (0 = unbounded)
//	MEDUSA_AUTH_TOKEN bearer token required on the admin API (except the
//	                  /healthz and /readyz probes); unset disables auth
//	MEDUSA_TLS_CERT   PEM cert for the inter-node transport (enables TLS with KEY)
//	MEDUSA_TLS_KEY    PEM private key matching MEDUSA_TLS_CERT
//	MEDUSA_TLS_CA     PEM CA bundle; when set, peers are verified against it and
//	                  mutual TLS is required (client certs verified too)
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lodgvideon/medusa"
	"github.com/lodgvideon/medusa/discovery"
	"github.com/lodgvideon/medusa/httpapi"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	id := env("MEDUSA_ID", hostname())
	addr := env("MEDUSA_ADDR", ":7700")
	bindAddr := env("MEDUSA_BIND_ADDR", addr)
	httpAddr := env("MEDUSA_HTTP_ADDR", ":8080")
	dataDir := os.Getenv("MEDUSA_DATA_DIR")
	seeds := splitSeeds(os.Getenv("MEDUSA_SEEDS"))
	disco, discDesc := discovererFromEnv(os.Getenv("MEDUSA_DISCOVERY"), addr, seeds)
	backups := envInt("MEDUSA_BACKUPS", 0)        // 0 → node defaults it to 1
	maxEntries := envInt("MEDUSA_MAX_ENTRIES", 0) // 0 → unbounded (no eviction)

	tlsCfg, err := tlsConfigFromEnv()
	if err != nil {
		slog.Error("load TLS config", "err", err)
		os.Exit(1)
	}

	// Seeds are passed to the node, whose maintenance loop retries joining until
	// the cluster converges — so startup order does not matter. BindAddr lets a
	// node listen on ":7700" while advertising a stable DNS name. DataDir, when
	// set, persists a snapshot so the cluster survives a whole-cluster restart.
	// Backups sets how many copies of each partition the cluster keeps. TLS, when
	// configured, secures the inter-node transport.
	node, err := medusa.New(medusa.Config{ID: id, Addr: addr, BindAddr: bindAddr, Seeds: seeds, Discovery: disco, DataDir: dataDir, Backups: backups, MaxEntries: maxEntries, TLS: tlsCfg})
	if err != nil {
		slog.Error("start node", "err", err)
		os.Exit(1)
	}

	// MEDUSA_AUTH_TOKEN, when set, requires a bearer token on the admin API
	// (WithToken("") is a no-op, so passing it unconditionally is safe). The
	// timeouts bound how long a slow or stalled client can hold a connection,
	// preventing Slowloris-style goroutine/fd exhaustion on this cleartext port.
	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           httpapi.New(node, httpapi.WithToken(os.Getenv("MEDUSA_AUTH_TOKEN"))),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "err", err)
			os.Exit(1)
		}
	}()
	slog.Info("node listening", "id", id, "advertise", addr, "bind", node.Addr(), "http", httpAddr, "discovery", discDesc, "backups", node.BackupCount(), "tls", tlsCfg != nil)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	slog.Info("leaving cluster gracefully")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	// Hand off partitions and announce departure so peers rebalance before this
	// node disappears — no data is left stranded on a backup.
	_ = node.Leave(ctx)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envInt reads an integer env var, falling back to def when unset or malformed.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

// tlsConfigFromEnv builds the inter-node TLS config from MEDUSA_TLS_* env vars,
// or returns (nil, nil) to leave the transport on cleartext h2c. With a CA
// bundle it enables mutual TLS: the same cert is presented as both server and
// client cert, and peers in either direction are verified against the CA.
func tlsConfigFromEnv() (*tls.Config, error) {
	certFile, keyFile := os.Getenv("MEDUSA_TLS_CERT"), os.Getenv("MEDUSA_TLS_KEY")
	if certFile == "" || keyFile == "" {
		return nil, nil // TLS disabled
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if caFile := os.Getenv("MEDUSA_TLS_CA"); caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in %s", caFile)
		}
		cfg.RootCAs = pool   // verify peers this node dials
		cfg.ClientCAs = pool // verify peers that dial in (mutual TLS)
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// discovererFromEnv builds the peer-discovery strategy from MEDUSA_DISCOVERY:
//
//	(unset) / "static"   use the MEDUSA_SEEDS list (classic behaviour)
//	"dns:<host>"         resolve <host> (e.g. a headless Service) to peer IPs
//	                     each tick, dialing this node's own data-plane port
//	"dns:<host>:<port>"  as above with an explicit port
//
// It returns the Discoverer and a short description for the startup log.
func discovererFromEnv(spec, addr string, seeds []string) (discovery.Discoverer, string) {
	if host, ok := strings.CutPrefix(strings.TrimSpace(spec), "dns:"); ok && host != "" {
		port := portOf(addr, "7700")
		if h, p, err := net.SplitHostPort(host); err == nil {
			// host carried its own ":port". Keep the derived port when that port
			// is empty (e.g. a "dns:medusa:" typo) rather than clobbering it with
			// "" — which would build invalid "ip:" dial targets that fail silently.
			host = h
			if p != "" {
				port = p
			}
		}
		return discovery.NewDNS(host, port), "dns:" + net.JoinHostPort(host, port)
	}
	return discovery.Static(seeds), "static"
}

// portOf returns the port of a "host:port" address, or def when it has none.
func portOf(addr, def string) string {
	if _, p, err := net.SplitHostPort(addr); err == nil && p != "" {
		return p
	}
	return def
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "medusa-node"
	}
	return h
}

func splitSeeds(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
