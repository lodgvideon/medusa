// Package httpapi exposes a medusa Node over a small REST + health surface,
// used by the node binary for liveness/readiness probes and for driving the
// cluster from outside (tests, kubectl port-forward, debugging).
package httpapi

import (
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/lodgvideon/medusa"
	"github.com/lodgvideon/medusa/imap"
	"github.com/lodgvideon/medusa/metrics"
)

// maxBodyBytes caps an admin-API request body so a single request cannot drive
// unbounded allocation (OOM/DoS). It matches the transport's per-message window,
// the largest value the grid stores anyway.
const maxBodyBytes = 16 << 20 // 16 MiB

// Option configures the HTTP API.
type Option func(*config)

type config struct {
	token string
}

// WithToken requires every request except the liveness/readiness probes to
// carry an "Authorization: Bearer <token>" header. The probes stay open so a
// Kubernetes kubelet can still check health without credentials. An empty token
// disables authentication (the default).
func WithToken(token string) Option {
	return func(c *config) { c.token = token }
}

// New returns an http.Handler serving:
//
//	GET    /healthz                  liveness — always 200 while serving (unauthenticated)
//	GET    /readyz                   readiness — 200 once the node has members (unauthenticated)
//	GET    /metrics                  Prometheus metrics (text exposition format)
//	GET    /stats                    JSON {members, localEntries, backups}
//	GET    /members                  JSON array of cluster members
//	GET    /v1/maps/{map}            cluster-wide live entry count for the map
//	                                 (502 + X-Medusa-Partial-Count header if a
//	                                 member is unreachable — count is a lower bound)
//	GET    /v1/maps/{map}?agg=<name> cluster-wide aggregation (count/sum/min/max or
//	                                 a registered custom one); 400 if the aggregator
//	                                 is unknown, 502 + X-Medusa-Partial-Result if a
//	                                 member is unreachable
//	GET    /v1/maps/{map}/{key}      fetch a value (404 if absent)
//	PUT    /v1/maps/{map}/{key}      store the request body as the value
//	DELETE /v1/maps/{map}/{key}      remove a key (404 if absent)
//	DELETE /v1/maps/{map}            clear the whole map cluster-wide (502 if a
//	                                 member is unreachable — clear is then partial)
//
// Pass WithToken to require bearer-token auth on every route but the probes.
func New(node *medusa.Node, opts ...Option) http.Handler {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeText(w, http.StatusOK, "ok")
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if len(node.Members()) == 0 {
			writeText(w, http.StatusServiceUnavailable, "no members")
			return
		}
		writeText(w, http.StatusOK, "ready")
	})

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		metrics.WriteProm(w, metrics.Gauges{
			Members:      len(node.Members()),
			LocalEntries: node.LocalEntryCount(),
		})
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats{
			Members:      len(node.Members()),
			LocalEntries: node.LocalEntryCount(),
			Backups:      node.BackupCount(),
		})
	})

	mux.HandleFunc("GET /members", func(w http.ResponseWriter, _ *http.Request) {
		members := node.Members()
		out := make([]member, len(members))
		for i, m := range members {
			out[i] = member{ID: m.ID, Addr: m.Addr}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	mux.HandleFunc("GET /v1/maps/{map}", func(w http.ResponseWriter, r *http.Request) {
		m := node.Map(r.PathValue("map"))
		// ?agg=<name> runs a cluster-wide aggregation instead of a plain count.
		if agg := r.URL.Query().Get("agg"); agg != "" {
			res, err := m.Aggregate(r.Context(), agg)
			if errors.Is(err, imap.ErrUnknownAggregator) {
				writeText(w, http.StatusBadRequest, err.Error())
				return
			}
			body := renderAggregate(res)
			if err != nil {
				// A member was unreachable: res covers only the reachable members.
				// Mirror Size's degraded contract — 502, partial value in a header.
				w.Header().Set("X-Medusa-Partial-Result", body)
				writeText(w, http.StatusBadGateway, err.Error())
				return
			}
			writeText(w, http.StatusOK, body)
			return
		}
		n, err := m.Size(r.Context())
		if err != nil {
			// A member was unreachable, so n is only a lower bound over the
			// reachable members. Signal the degraded state with 502, but expose the
			// partial count in a header rather than discarding it — mirroring the
			// Go API's (lower-bound, error) contract.
			w.Header().Set("X-Medusa-Partial-Count", strconv.FormatUint(n, 10))
			writeText(w, http.StatusBadGateway, err.Error())
			return
		}
		writeText(w, http.StatusOK, strconv.FormatUint(n, 10))
	})

	mux.HandleFunc("GET /v1/maps/{map}/{key}", func(w http.ResponseWriter, r *http.Request) {
		v, ok, err := node.Map(r.PathValue("map")).Get(r.Context(), []byte(r.PathValue("key")))
		if err != nil {
			writeText(w, http.StatusBadGateway, err.Error())
			return
		}
		if !ok {
			writeText(w, http.StatusNotFound, "not found")
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(v)
	})

	mux.HandleFunc("PUT /v1/maps/{map}/{key}", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			writeText(w, http.StatusBadRequest, err.Error())
			return
		}
		m := node.Map(r.PathValue("map"))
		key := []byte(r.PathValue("key"))
		// Optional ?ttl=<duration> (e.g. 5s, 500ms) sets an entry expiry.
		if ttlStr := r.URL.Query().Get("ttl"); ttlStr != "" {
			ttl, perr := time.ParseDuration(ttlStr)
			if perr != nil {
				writeText(w, http.StatusBadRequest, "invalid ttl: "+perr.Error())
				return
			}
			err = m.PutTTL(r.Context(), key, body, ttl)
		} else {
			err = m.Put(r.Context(), key, body)
		}
		if err != nil {
			writeText(w, http.StatusBadGateway, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// Execute a server-side processor: POST /v1/maps/{map}/{key}/execute?proc=incr
	// with the argument as the request body; the result is the response body.
	mux.HandleFunc("POST /v1/maps/{map}/{key}/execute", func(w http.ResponseWriter, r *http.Request) {
		proc := r.URL.Query().Get("proc")
		if proc == "" {
			writeText(w, http.StatusBadRequest, "missing ?proc=")
			return
		}
		arg, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			writeText(w, http.StatusBadRequest, err.Error())
			return
		}
		out, err := node.Map(r.PathValue("map")).Execute(r.Context(), []byte(r.PathValue("key")), proc, arg)
		if err != nil {
			writeText(w, http.StatusBadGateway, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(out)
	})

	mux.HandleFunc("DELETE /v1/maps/{map}", func(w http.ResponseWriter, r *http.Request) {
		if err := node.Map(r.PathValue("map")).Clear(r.Context()); err != nil {
			// A member was unreachable, so the map may not be fully cleared.
			writeText(w, http.StatusBadGateway, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("DELETE /v1/maps/{map}/{key}", func(w http.ResponseWriter, r *http.Request) {
		existed, err := node.Map(r.PathValue("map")).Remove(r.Context(), []byte(r.PathValue("key")))
		if err != nil {
			writeText(w, http.StatusBadGateway, err.Error())
			return
		}
		if !existed {
			writeText(w, http.StatusNotFound, "not found")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	if cfg.token == "" {
		return mux
	}
	return requireToken(cfg.token, mux)
}

// requireToken wraps h so every request but the unauthenticated probes must
// present "Authorization: Bearer <token>". The token is compared in constant
// time to avoid leaking it through response-timing differences.
func requireToken(token string, h http.Handler) http.Handler {
	want := []byte(token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			h.ServeHTTP(w, r) // probes stay open for the kubelet
			return
		}
		const prefix = "Bearer "
		got := r.Header.Get("Authorization")
		if len(got) < len(prefix) || got[:len(prefix)] != prefix ||
			subtle.ConstantTimeCompare([]byte(got[len(prefix):]), want) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeText(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		h.ServeHTTP(w, r)
	})
}

type member struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type stats struct {
	Members      int `json:"members"`
	LocalEntries int `json:"localEntries"`
	Backups      int `json:"backups"`
}

func writeText(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, msg)
}

// renderAggregate turns an aggregator result into text for the HTTP layer. The
// numeric built-ins (count/sum/min/max) encode a big-endian int64, rendered as a
// decimal; an empty result (min/max over an empty map) renders as the empty
// string; any other length is returned verbatim for custom aggregators.
func renderAggregate(res []byte) string {
	switch len(res) {
	case 0:
		return ""
	case 8:
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(res)), 10)
	default:
		return string(res)
	}
}
