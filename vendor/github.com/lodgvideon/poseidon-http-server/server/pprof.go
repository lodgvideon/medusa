package server

import (
	"net/http"
	"net/http/pprof"
)

// ---------------------------------------------------------------------------
// pprof — opt-in Go runtime profiling endpoints
// ---------------------------------------------------------------------------

// PprofHandler returns a [Handler] that serves the standard Go runtime
// profiling endpoints from net/http/pprof:
//
//	/debug/pprof/         — the HTML index, plus the named profiles
//	                        (heap, goroutine, allocs, block, mutex, threadcreate)
//	/debug/pprof/cmdline  — the running program's command line
//	/debug/pprof/profile  — a 30s (or ?seconds=N) CPU profile
//	/debug/pprof/symbol   — symbol lookups for program counters
//	/debug/pprof/trace    — an execution trace
//
// It is OPT-IN: the server never mounts it automatically, because pprof
// exposes sensitive runtime internals (memory contents, goroutine stacks,
// command line) and must never be reachable publicly. Mount it explicitly
// behind authentication / a private listener, e.g.:
//
//	srv.Handle("/debug/pprof/", server.PprofHandler())
//
// The handler is built over an isolated [http.ServeMux] (it does NOT use
// http.DefaultServeMux), so importing this package — or calling this
// function — has no global side effects beyond what net/http/pprof's own
// init already registers on the default mux.
func PprofHandler() Handler {
	mux := http.NewServeMux()

	// Index handles "/debug/pprof/" and dispatches the named profiles
	// (heap, goroutine, allocs, block, mutex, threadcreate) by trailing path.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return FromHTTPHandler(mux)
}
