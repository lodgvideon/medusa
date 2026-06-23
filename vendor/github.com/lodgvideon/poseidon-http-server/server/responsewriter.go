package server

import (
	"net/http"
)

// ---------------------------------------------------------------------------
// Unwrap convention + capability finders (the net/http ResponseController model)
// ---------------------------------------------------------------------------
//
// A ResponseWriter that WRAPS another writer (e.g. the Gzip or SecurityHeaders
// middleware) should implement:
//
//	Unwrap() ResponseWriter
//
// returning the next writer in the chain. This mirrors the convention used by
// net/http's ResponseController (https://pkg.go.dev/net/http#ResponseController):
// optional capabilities such as Server Push (Pusher) or Flush (http.Flusher)
// are not part of the core ResponseWriter interface, so a wrapper that does not
// itself provide a capability would otherwise hide a base writer that does.
//
// Rather than re-implementing every optional method on every wrapper, a wrapper
// exposes Unwrap() and callers use the finders below — PusherOf / FlusherOf —
// to walk the chain and locate the capability. This keeps wrappers small and
// keeps new capabilities from requiring edits to every wrapper.
//
// Direct (unwrapped) writers continue to satisfy w.(Pusher) and w.(http.Flusher)
// as before; only THROUGH a wrapper must one use the finders.

// unwrapper is implemented by a wrapping ResponseWriter that exposes the next
// writer in the chain. It is unexported because the method set (a single
// Unwrap() ResponseWriter) is the entire contract; wrappers just declare it.
type unwrapper interface {
	Unwrap() ResponseWriter
}

// maxUnwrapDepth caps how far the finders walk an Unwrap() chain, guarding
// against an accidental cycle (a wrapper whose Unwrap returns itself or forms a
// loop). Real middleware stacks are only a handful deep.
const maxUnwrapDepth = 64

// PusherOf returns the [Pusher] capability reachable from w, checking w itself
// first and then walking the Unwrap() chain of any wrapping writers. It returns
// (nil, false) when no writer in the chain supports push.
//
// Use this instead of a direct w.(Pusher) type assertion in handlers and
// middleware, so push still works when w is wrapped by middleware (gzip,
// security headers, etc.) that does not itself implement Pusher.
func PusherOf(w ResponseWriter) (Pusher, bool) {
	for depth := 0; w != nil && depth < maxUnwrapDepth; depth++ {
		if p, ok := w.(Pusher); ok {
			return p, true
		}
		u, ok := w.(unwrapper)
		if !ok {
			break
		}
		next := u.Unwrap()
		if next == w {
			break // self-referential Unwrap; stop.
		}
		w = next
	}
	return nil, false
}

// FlusherOf returns the [http.Flusher] capability reachable from w, checking w
// itself first and then walking the Unwrap() chain. It returns (nil, false)
// when no writer in the chain supports flushing.
//
// Use this instead of a direct w.(http.Flusher) type assertion so flushing
// still reaches the base writer through middleware wrappers.
func FlusherOf(w ResponseWriter) (http.Flusher, bool) {
	for depth := 0; w != nil && depth < maxUnwrapDepth; depth++ {
		if f, ok := w.(http.Flusher); ok {
			return f, true
		}
		u, ok := w.(unwrapper)
		if !ok {
			break
		}
		next := u.Unwrap()
		if next == w {
			break // self-referential Unwrap; stop.
		}
		w = next
	}
	return nil, false
}
