package server

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// ---------------------------------------------------------------------------
// HTTP health endpoints — liveness (/healthz) and readiness (/readyz)
//
// These mirror the Kubernetes probe convention and are intentionally DISTINCT:
//
//   - Liveness (/healthz): "is the process alive?" — returns 200 as long as the
//     server is serving, even while draining. A failing liveness probe causes
//     k8s to RESTART the pod, so it must NOT depend on readiness/drain state.
//   - Readiness (/readyz): "should traffic be routed here?" — returns 200 when
//     ready and 503 once draining begins. A failing readiness probe causes k8s
//     to REMOVE the pod from Service endpoints (stops routing) WITHOUT a restart,
//     which is exactly what we want at the start of graceful shutdown.
//
// Wire HealthState.SetNotReady into Options.OnDrainStart (called at the very
// start of Server.Shutdown) so k8s stops routing new traffic before in-flight
// streams are drained.
// ---------------------------------------------------------------------------

// Default health endpoint paths.
const (
	// LivenessPath is the default liveness probe path.
	LivenessPath = "/healthz"
	// ReadinessPath is the default readiness probe path.
	ReadinessPath = "/readyz"
)

// HealthState holds the readiness flag for a server. It is safe for concurrent
// use. Liveness is not tracked here: a live process answers /healthz with 200
// unconditionally; only readiness toggles.
//
// A freshly constructed HealthState is READY, so a server becomes routable as
// soon as it starts serving. Call SetNotReady (typically from the drain hook)
// to take the instance out of rotation.
type HealthState struct {
	// ready is 1 when ready, 0 when not. Stored as int32 for atomic access.
	ready atomic.Int32
}

// NewHealthState returns a HealthState that is ready by default.
func NewHealthState() *HealthState {
	hs := &HealthState{}
	hs.ready.Store(1)
	return hs
}

// SetReady sets readiness to the given value.
func (h *HealthState) SetReady(ready bool) {
	if ready {
		h.ready.Store(1)
		return
	}
	h.ready.Store(0)
}

// SetNotReady marks the instance as not ready (draining). Equivalent to
// SetReady(false); provided as a clearer call site at drain start.
func (h *HealthState) SetNotReady() { h.ready.Store(0) }

// Ready reports whether the instance is currently ready to serve traffic.
func (h *HealthState) Ready() bool { return h.ready.Load() == 1 }

// HealthHandler returns a Handler that serves the liveness (/healthz) and
// readiness (/readyz) probes from the given HealthState:
//
//   - GET /healthz → 200 (always, while serving)
//   - GET /readyz  → 200 when ready, 503 when draining
//   - anything else → 404
//
// The handler matches on the path component only, ignoring any query string,
// so probes may append diagnostics query params. A nil HealthState is treated
// as always-ready.
func HealthHandler(hs *HealthState) Handler {
	return HandlerFunc(func(_ context.Context, req *Request, w ResponseWriter) error {
		path, _ := splitPathQuery(req.Path)
		switch path {
		case LivenessPath:
			return writeHealthStatus(w, http.StatusOK, "ok")
		case ReadinessPath:
			if hs == nil || hs.Ready() {
				return writeHealthStatus(w, http.StatusOK, "ready")
			}
			return writeHealthStatus(w, http.StatusServiceUnavailable, "draining")
		default:
			return writeHealthStatus(w, http.StatusNotFound, "not found")
		}
	})
}

// writeHealthStatus writes a tiny text/plain body with the given status.
func writeHealthStatus(w ResponseWriter, status int, body string) error {
	if err := w.WriteHeaders(status, healthHeaders); err != nil {
		return err
	}
	return w.WriteData([]byte(body))
}

// healthHeaders is the shared, immutable header slice for health responses.
var healthHeaders = []hpack.HeaderField{
	{Name: []byte("content-type"), Value: []byte("text/plain; charset=utf-8")},
}
