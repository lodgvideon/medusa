//go:build k8s

package medusa_test

import (
	"os/exec"
	"testing"
)

// TestK8sE2E runs the Kubernetes end-to-end suite in k8s/e2e.sh. It is gated
// behind the `k8s` build tag so the default `go test ./...` never invokes it:
//
//	go test -tags k8s -run TestK8sE2E -timeout 15m .
//
// The script skips cleanly (success) when no cluster or Docker is reachable.
func TestK8sE2E(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	out, err := exec.Command("bash", "k8s/e2e.sh").CombinedOutput()
	t.Logf("k8s e2e output:\n%s", out)
	if err != nil {
		t.Fatalf("k8s e2e failed: %v", err)
	}
}
