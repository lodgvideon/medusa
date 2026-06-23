// Package httpapi exposes a medusa Node over a small REST + health surface,
// used by the node binary for liveness/readiness probes and for driving the
// cluster from outside (tests, kubectl port-forward, debugging).
package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/lodgvideon/medusa"
	"github.com/lodgvideon/medusa/metrics"
)

// New returns an http.Handler serving:
//
//	GET    /healthz                  liveness — always 200 while serving
//	GET    /readyz                   readiness — 200 once the node has members
//	GET    /members                  JSON array of cluster members
//	GET    /v1/maps/{map}/{key}      fetch a value (404 if absent)
//	PUT    /v1/maps/{map}/{key}      store the request body as the value
//	DELETE /v1/maps/{map}/{key}      remove a key (404 if absent)
func New(node *medusa.Node) http.Handler {
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
		body, err := io.ReadAll(r.Body)
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
		arg, err := io.ReadAll(r.Body)
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

	return mux
}

type member struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type stats struct {
	Members      int `json:"members"`
	LocalEntries int `json:"localEntries"`
}

func writeText(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, msg)
}
