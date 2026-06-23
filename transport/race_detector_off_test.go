//go:build !race

package transport_test

// raceEnabled is false in ordinary (non -race) builds, where the transport
// timing tests use their tight default budgets for fast feedback.
const raceEnabled = false
