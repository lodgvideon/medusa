// Package server provides a high-level HTTP/2 server built on the conn package.
//
// It handles TLS + h2c listening, accepts connections, dispatches inbound
// streams to registered handlers, and manages graceful shutdown.
//
// # Drop-in compatibility
//
// The server package implements net/http.Handler compatibility so that any
// chi.Router, http.Handler, or http.HandlerFunc can be used directly:
//
//	r := chi.NewRouter()
//	r.Get("/hello", func(w http.ResponseWriter, r *http.Request) {
//	    w.Write([]byte("hi!"))
//	})
//	srv, _ := server.NewServer(server.Options{
//	    Addr:       ":8443",
//	    HTTPHandler: r,
//	})
//	srv.ListenAndServe(context.Background())
//
// # Design (SOLID)
//
//   - S: owns only the accept loop + routing
//   - O: extensible via Handler interface and Middleware chain
//   - L: any Handler implementation is interchangeable
//   - I: small interfaces — Handler, Middleware, Listener
//   - D: depends on Listener and Handler interfaces, not concrete types
package server
