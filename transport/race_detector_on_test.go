//go:build race

package transport_test

// raceEnabled is true when the test binary is built with -race. The race
// detector slows execution by roughly 10–20×, so timing-sensitive transport
// tests — notably the 1 MiB Poseidon transfer, whose h2c connection-window
// round-trips are the slow part — give themselves a much larger per-attempt
// budget under it. This dev environment has no C compiler, so -race only ever
// runs in CI; the larger budget keeps that run from flaking on slowness.
const raceEnabled = true
