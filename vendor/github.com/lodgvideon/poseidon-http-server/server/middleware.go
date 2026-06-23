package server

// Middleware wraps a Handler with before/after logic.
type Middleware func(Handler) Handler

// Chain composes middlewares: Chain(m1, m2)(h) = m1(m2(h)).
// Execution order: m1 → m2 → handler → m2 → m1 (onion model).
// If no middlewares are provided, returns the handler unchanged.
func Chain(mw ...Middleware) Middleware {
	return func(next Handler) Handler {
		for i := len(mw) - 1; i >= 0; i-- {
			next = mw[i](next)
		}
		return next
	}
}
