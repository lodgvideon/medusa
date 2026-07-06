#!/usr/bin/env bash
#
# Health check for the self-hosted GitHub Actions runner (k8s/runner.yaml).
# Verifies the Deployment rolled out, the dind sidecar is up, and the runner
# agent connected to GitHub ("Listening for Jobs").
#
# Skips (exit 0) when there is no cluster or the runner is not deployed, so it is
# safe to invoke unconditionally. Run from anywhere:
#
#     bash k8s/runner-check.sh        # or: make runner-check
#
set -uo pipefail

PASS=0
FAIL=0
ok()  { echo "  PASS: $1"; PASS=$((PASS + 1)); }
bad() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

# ---- preflight (skip cleanly when prerequisites are absent) ----
if ! command -v kubectl >/dev/null 2>&1 || ! kubectl cluster-info >/dev/null 2>&1; then
  echo "SKIP: no reachable Kubernetes cluster"; exit 0
fi
if ! kubectl get deploy/github-runner >/dev/null 2>&1; then
  echo "SKIP: github-runner not deployed (kubectl apply -f k8s/runner.yaml)"; exit 0
fi

# ---- test: the Deployment is available ----
echo "=== test: runner rollout ==="
if kubectl rollout status deploy/github-runner --timeout=120s >/dev/null 2>&1; then
  ok "github-runner Deployment rolled out"
else
  bad "github-runner did not become available -> $(kubectl get pods -l app=github-runner -o wide 2>&1)"
fi

pod=$(kubectl get pods -l app=github-runner -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

# ---- test: the runner reaches the dind Docker daemon ----
# Exec in the runner container (DOCKER_HOST points at the shared socket) so this
# checks the exact path the `docker build` job uses, end to end.
echo "=== test: DinD reachable from runner ==="
if [ -n "$pod" ] && kubectl exec "$pod" -c runner -- docker info >/dev/null 2>&1; then
  ok "runner reaches the dind Docker daemon over the shared socket"
else
  bad "runner cannot reach docker -> $(kubectl logs "$pod" -c dind --tail=5 2>&1)"
fi

# ---- test: the agent registered with GitHub ----
# myoung34/github-runner logs "Listening for Jobs" once config.sh + run.sh connect.
echo "=== test: runner registration ==="
if [ -n "$pod" ] && kubectl logs "$pod" -c runner --tail=50 2>/dev/null | grep -q "Listening for Jobs"; then
  ok "runner registered and is listening for jobs"
else
  bad "runner not listening -> $(kubectl logs "$pod" -c runner --tail=10 2>&1)"
fi

echo "=== runner-check summary: $PASS passed, $FAIL failed ==="
[ "$FAIL" = "0" ]
