#!/usr/bin/env bash
#
# End-to-end Kubernetes test for medusa. Deploys a 3-node cluster, verifies
# clustering, the configured replication factor, the cross-pod distributed map,
# scale-out partition migration, zero-data-loss rolling restart, and write-ahead
# log recovery after an ungraceful crash, then tears everything down.
#
# Skips (exit 0) when no Kubernetes cluster or Docker is reachable, so it is
# safe to invoke unconditionally. Run from anywhere:
#
#     bash k8s/e2e.sh
#
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

IMAGE="medusa:e2e-$(date +%s)"
PASS=0
FAIL=0
ok()  { echo "  PASS: $1"; PASS=$((PASS + 1)); }
bad() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

# ---- preflight (skip cleanly when prerequisites are absent) ----
if ! command -v kubectl >/dev/null 2>&1 || ! kubectl cluster-info >/dev/null 2>&1; then
  echo "SKIP: no reachable Kubernetes cluster"; exit 0
fi
if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "SKIP: Docker not available"; exit 0
fi

MANIFEST="$(mktemp)"
cleanup() {
  echo "=== teardown ==="
  kubectl delete -f "$MANIFEST" --now >/dev/null 2>&1
  kubectl delete pvc -l app=medusa --now >/dev/null 2>&1 # StatefulSet PVCs are retained otherwise
  rm -f "$MANIFEST"
  docker rmi "$IMAGE" >/dev/null 2>&1
}
trap cleanup EXIT

# Run a shell snippet inside a throwaway curl pod and echo its stdout.
incluster() {
  kubectl delete pod medusa-e2e-curl --now >/dev/null 2>&1
  kubectl run medusa-e2e-curl --image=curlimages/curl:8.11.1 --restart=Never \
    --command -- sh -c "$1" >/dev/null 2>&1
  for _ in $(seq 1 45); do
    ph=$(kubectl get pod medusa-e2e-curl -o jsonpath='{.status.phase}' 2>/dev/null)
    { [ "$ph" = "Succeeded" ] || [ "$ph" = "Failed" ]; } && break
    sleep 1
  done
  kubectl logs medusa-e2e-curl 2>/dev/null
  kubectl delete pod medusa-e2e-curl --now >/dev/null 2>&1
}

# ---- build + deploy (unique tag forces a fresh image into the cluster) ----
echo "=== build $IMAGE ==="
docker build -t "$IMAGE" . >/dev/null 2>&1 || { echo "docker build failed"; exit 1; }
sed "s|image: medusa:.*|image: $IMAGE|" k8s/medusa.yaml > "$MANIFEST"

echo "=== deploy ==="
# Clean slate: a StatefulSet's volumeClaimTemplates are immutable, so a leftover
# one from an interrupted run must be removed before apply.
kubectl delete statefulset medusa --ignore-not-found --now >/dev/null 2>&1
kubectl delete pvc -l app=medusa --ignore-not-found --now >/dev/null 2>&1
kubectl apply -f "$MANIFEST" >/dev/null
kubectl rollout status statefulset/medusa --timeout=120s >/dev/null || { echo "rollout failed"; exit 1; }
sleep 12 # let the maintenance loop converge

# ---- test: cluster formation via DNS auto-discovery ----
# The manifest configures MEDUSA_DISCOVERY=dns:medusa:7700 (no seed list), so
# reaching members=3 proves the pods found each other by resolving the headless
# Service.
echo "=== test: cluster formation (DNS auto-discovery) ==="
out=$(incluster 'for n in 0 1 2; do curl -s medusa-$n.medusa:8080/stats; echo; done')
if [ "$(echo "$out" | grep -c '"members":3')" = "3" ]; then
  ok "all 3 pods report members=3 (discovered via dns:medusa:7700)"
else
  bad "members != 3 -> $out"
fi

# ---- test: configured replication factor ----
echo "=== test: replication factor ==="
# The manifest sets MEDUSA_BACKUPS=1, so every pod's /stats must report it.
out=$(incluster 'for n in 0 1 2; do curl -s medusa-$n.medusa:8080/stats; echo; done')
if [ "$(echo "$out" | grep -c '"backups":1')" = "3" ]; then
  ok "all 3 pods report the configured backups=1"
else
  bad "backups != 1 -> $out"
fi

# ---- test: cross-pod distributed map ----
echo "=== test: cross-pod put/get ==="
out=$(incluster '
  for i in $(seq 1 30); do curl -s -o /dev/null -X PUT --data-binary "v$i" medusa-0.medusa:8080/v1/maps/g/k$i; done
  m=0; for i in $(seq 1 30); do v=$(curl -s medusa-$((i % 3)).medusa:8080/v1/maps/g/k$i); [ "$v" = "v$i" ] || m=$((m + 1)); done
  echo "miss=$m"')
if echo "$out" | grep -q "miss=0"; then
  ok "30 keys written via medusa-0 are readable from all pods"
else
  bad "cross-pod get -> $out"
fi

# ---- test: Prometheus metrics endpoint ----
echo "=== test: metrics endpoint ==="
out=$(incluster 'curl -s medusa-0.medusa:8080/metrics | grep -c "^medusa_"')
if [ "${out:-0}" -ge 5 ] 2>/dev/null; then
  ok "metrics endpoint exposes $out medusa_* series"
else
  bad "metrics endpoint -> $out"
fi

# ---- test: feature metrics present ----
# Anti-entropy, max-size eviction, and entry-event delivery are cluster-visible
# behaviors; assert their counters are exported (named explicitly so dropping one
# from WriteProm fails the suite, not just a generic series count). Entry-event
# counters stay 0 in the node binary — no listener is registered there — so this
# verifies the series is wired, not that it fired.
echo "=== test: anti-entropy + eviction + entry-event metrics ==="
out=$(incluster 'curl -s medusa-0.medusa:8080/metrics | grep -cE "^medusa_(entries_(reconciled|evicted)|events_(emitted|dropped))_total "')
if [ "${out:-0}" -ge 4 ] 2>/dev/null; then
  ok "anti-entropy + eviction + entry-event counters exported"
else
  bad "expected reconciled/evicted/events_emitted/events_dropped counters -> $out"
fi

# ---- test: TTL expiry ----
echo "=== test: TTL expiry ==="
out=$(incluster '
  curl -s -o /dev/null -X PUT --data-binary ttlval "medusa-0.medusa:8080/v1/maps/g/ttlkey?ttl=3s"
  before=$(curl -s medusa-1.medusa:8080/v1/maps/g/ttlkey)
  sleep 6
  after=$(curl -s -o /dev/null -w "%{http_code}" medusa-2.medusa:8080/v1/maps/g/ttlkey)
  echo "before=$before after=$after"')
if echo "$out" | grep -q "before=ttlval" && echo "$out" | grep -q "after=404"; then
  ok "TTL entry expired across the cluster"
else
  bad "TTL -> $out"
fi

# ---- test: distributed compute (EntryProcessor atomic append) ----
echo "=== test: EntryProcessor (atomic concurrent append) ==="
out=$(incluster '
  for i in $(seq 1 40); do curl -s -o /dev/null -X POST --data-binary "x" "medusa-0.medusa:8080/v1/maps/g/appendkey/execute?proc=append" & done
  wait
  v=$(curl -s medusa-1.medusa:8080/v1/maps/g/appendkey)
  echo "len=${#v}"')
if echo "$out" | grep -q "len=40"; then
  ok "40 concurrent atomic appends all landed (no lost updates)"
else
  bad "EntryProcessor -> $out"
fi

# ---- test: coordination primitive (putIfAbsent) ----
# First put-if-absent stores; a second with a different value must be a no-op, so
# a cross-pod GET still returns the first writer's value.
echo "=== test: putIfAbsent (distributed lock primitive) ==="
out=$(incluster '
  curl -s -o /dev/null -X POST --data-binary first  "medusa-0.medusa:8080/v1/maps/g/lockkey/execute?proc=putifabsent"
  curl -s -o /dev/null -X POST --data-binary second "medusa-2.medusa:8080/v1/maps/g/lockkey/execute?proc=putifabsent"
  curl -s medusa-1.medusa:8080/v1/maps/g/lockkey')
if [ "$out" = "first" ]; then
  ok "putIfAbsent stored once; the racing put-if-absent was a no-op"
else
  bad "putIfAbsent -> $out (want \"first\")"
fi

# ---- test: cluster-wide Map.Size ----
# Put 6 distinct keys via one pod; the size queried from another pod must total 6
# across the cluster (each entry counted once by its owner, backups excluded).
echo "=== test: Map.Size (cluster-wide count) ==="
out=$(incluster '
  for i in 1 2 3 4 5 6; do curl -s -o /dev/null -X PUT --data-binary v "medusa-0.medusa:8080/v1/maps/sized/k$i"; done
  curl -s medusa-1.medusa:8080/v1/maps/sized')
if [ "$out" = "6" ]; then
  ok "Map.Size totalled 6 entries cluster-wide from a different pod"
else
  bad "Map.Size -> $out (want 6)"
fi

# ---- test: cluster-wide Map.Clear ----
# Clear the map via one pod (DELETE on the map root); the size from another pod
# must then be 0 — every member dropped its copies (owner and backup).
echo "=== test: Map.Clear (cluster-wide) ==="
out=$(incluster '
  curl -s -o /dev/null -X DELETE medusa-2.medusa:8080/v1/maps/sized
  curl -s medusa-0.medusa:8080/v1/maps/sized')
if [ "$out" = "0" ]; then
  ok "Map.Clear emptied the map cluster-wide (size 0 from another pod)"
else
  bad "Map.Clear -> $out (want 0)"
fi

# ---- test: fenced lock (acquire / contend) ----
# Acquiring returns an 8-byte fence token; a contending holder on another pod
# gets an empty body (lock held). Assert byte counts, which are printable.
echo "=== test: fenced lock (acquire + contention) ==="
out=$(incluster '
  a=$(curl -s -X POST --data-binary node-1 "medusa-0.medusa:8080/v1/maps/g/mutexkey/execute?proc=lockacquire" | wc -c)
  b=$(curl -s -X POST --data-binary node-2 "medusa-2.medusa:8080/v1/maps/g/mutexkey/execute?proc=lockacquire" | wc -c)
  echo "token=$a contended=$b"')
if echo "$out" | grep -q "token=8 contended=0"; then
  ok "fenced lock acquired (8-byte token); cross-pod contender was refused"
else
  bad "fenced lock -> $out (want token=8 contended=0)"
fi

# ---- test: scale-out migration (3 -> 5) ----
echo "=== test: scale-out migration ==="
kubectl scale statefulset/medusa --replicas=5 >/dev/null
kubectl rollout status statefulset/medusa --timeout=120s >/dev/null
sleep 14
out=$(incluster 'for n in 3 4; do echo -n "m$n="; curl -s medusa-$n.medusa:8080/stats | grep -o "localEntries[^,}]*"; echo; done')
if echo "$out" | grep -qE 'localEntries":[1-9]'; then
  ok "new pods received migrated data -> $(echo "$out" | tr '\n' ' ')"
else
  bad "no data migrated to scaled-out pods -> $out"
fi
kubectl scale statefulset/medusa --replicas=3 >/dev/null
kubectl rollout status statefulset/medusa --timeout=120s >/dev/null
sleep 8

# ---- test: zero-data-loss rolling restart ----
echo "=== test: rolling-restart data survival ==="
kubectl rollout restart statefulset/medusa >/dev/null
kubectl rollout status statefulset/medusa --timeout=150s >/dev/null
sleep 15
out=$(incluster 'm=0; for i in $(seq 1 30); do v=$(curl -s medusa-1.medusa:8080/v1/maps/g/k$i); [ "$v" = "v$i" ] || m=$((m + 1)); done; echo "miss=$m"')
if echo "$out" | grep -q "miss=0"; then
  ok "all 30 keys survived a rolling restart"
else
  bad "rolling restart lost data -> $out"
fi

# ---- test: whole-cluster restart (persistence) ----
echo "=== test: whole-cluster restart (persistence) ==="
# Delete every pod at once: graceful Close persists a snapshot to each PVC, and
# the recreated pods reload it — there is no peer to migrate to.
kubectl delete pods -l app=medusa --wait=true >/dev/null 2>&1
kubectl rollout status statefulset/medusa --timeout=150s >/dev/null
sleep 16
out=$(incluster 'm=0; for i in $(seq 1 30); do v=$(curl -s medusa-2.medusa:8080/v1/maps/g/k$i); [ "$v" = "v$i" ] || m=$((m + 1)); done; echo "miss=$m"')
if echo "$out" | grep -q "miss=0"; then
  ok "all 30 keys survived a whole-cluster restart (reloaded from disk)"
else
  bad "persistence -> $out"
fi

# ---- test: write-ahead log survives an ungraceful crash ----
echo "=== test: WAL crash durability ==="
# Write a fresh key, then SIGKILL every pod (--grace-period=0 --force) so no
# graceful snapshot is taken. The key, younger than the snapshot interval, lives
# only in the fsync'd WAL; replaying it on restart must recover the write.
out=$(incluster '
  curl -s -o /dev/null -X PUT --data-binary walval medusa-0.medusa:8080/v1/maps/g/walkey
  echo done')
kubectl delete pods -l app=medusa --grace-period=0 --force >/dev/null 2>&1
kubectl rollout status statefulset/medusa --timeout=150s >/dev/null
sleep 16
out=$(incluster 'curl -s -o /dev/null -w "%{http_code} " medusa-1.medusa:8080/v1/maps/g/walkey; curl -s medusa-1.medusa:8080/v1/maps/g/walkey')
if echo "$out" | grep -q "walval"; then
  ok "key written just before a SIGKILL crash was recovered from the WAL"
else
  bad "WAL crash durability -> $out"
fi

echo "=== e2e summary: $PASS passed, $FAIL failed ==="
[ "$FAIL" = "0" ]
