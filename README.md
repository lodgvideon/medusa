# medusa

[![ci](https://github.com/lodgvideon/medusa/actions/workflows/ci.yml/badge.svg)](https://github.com/lodgvideon/medusa/actions/workflows/ci.yml)

A small, **zero-allocation-oriented distributed in-memory data grid** in Go — a
Hazelcast-style cluster of nodes that together host partitioned, replicated maps
and talk to each other over **protobuf**.

Built test-first, with a ≥90% coverage gate on the hand-written packages and
allocation assertions baked into the test suite.

```
metric                                       value
───────────────────────────────────────     ──────────────────────────────
coverage (hand-written packages)             90.9%   0 allocs/op on hot paths
BenchmarkMarshal      (encode, warm buffer)   7.1 ns/op   0 allocs/op
BenchmarkMapGetLocal  (local owned read)     27.8 ns/op   0 allocs/op
```

Coverage is measured with `go test -coverpkg=./...` so cross-package exercise
counts (e.g. the migration paths driven from the top-level integration tests),
excluding the generated `genproto/` and the thin `cmd/medusa-node` main.

## What it does

- **Distributed map (`IMap`)** — a named key/value store whose entries are
  spread across 271 partitions. Any node can serve any key by routing to the
  partition owner. `Map.Size` returns the cluster-wide live entry count via a
  scatter-gather that sums each owner's share (also `GET /v1/maps/{map}`), and
  `Map.Clear` empties the map cluster-wide by broadcasting a drop to every member
  (also `DELETE /v1/maps/{map}`).
- **Distributed compute (EntryProcessor)** — `Map.Execute(key, "incr", arg)`
  runs a named server-side processor *atomically on the key's owner* (read-
  modify-write under the shard lock), so e.g. an atomic distributed counter
  has no lost updates under concurrency — no data movement, one round trip.
  Built-ins: `incr`, `append`, `getset`, `delete`; register your own. Also over
  HTTP: `POST /v1/maps/{m}/{k}/execute?proc=incr`.
- **Atomic coordination primitives** — built on the same atomic owner-side
  read-modify-write: `Map.PutIfAbsent(key, value)` stores only if the key is
  absent (returns whether it won — a distributed-lock / leader-election building
  block), and `Map.CompareAndSwap(key, expected, new)` sets only if the current
  value matches (optimistic concurrency / compare-and-set). Both are exposed as
  the `putifabsent` and `cas` processors too. Like all `Execute` ops they are
  at-least-once under owner failover (a lost response + retry can report a false
  negative for a write that actually applied), so for strict locking use a
  caller-unique value and read it back to confirm ownership.
- **Fenced locks** — `Map.Lock(key, holder)` returns a **fence token** (and
  whether it was acquired); `Map.Unlock(key, holder)` releases it. The token lets
  a holder prove ownership to a downstream service so a stale holder (one paused
  past its turn) is detected. Acquiring a lock you already hold returns your
  existing token (idempotent), so a failover retry is safe rather than a false
  negative. Built on the same atomic owner-side processors
  (`lockacquire`/`lockrelease`). **Caveat:** it is a single-owner lock, and the
  fence is strictly monotonic only while one owner serves the key uncontended —
  *not* across an ungraceful owner crash (a promoted backup may have missed the
  last acquire) nor a partition migration (an acquire routed to the old owner on
  a stale table during the handoff is not propagated to the new owner, which
  reissues the token). Both stem from the AP, best-effort-replication model;
  strict fencing needs synchronous/consensus replication or a quiescent handoff
  (roadmap). The `holder` is a cooperative id, not authenticated.
- **Replication (configurable factor)** — every write is synchronously copied to
  `Backups` distinct backup owners (default 1; env `MEDUSA_BACKUPS`). A cluster
  tolerates that many simultaneous holder failures with no data loss: when the
  owner is unreachable, reads and writes transparently fail over through the
  backups in replica order until one responds, so losing the owner *and* the
  first backup still succeeds when a second backup exists. The factor is capped
  to however many distinct backups the cluster can supply.
- **Active anti-entropy (digest-gated)** — beneath the synchronous-replication
  fast path, each owner continuously reconciles a rotating slice of its
  partitions with their backups (the maintenance loop cycles through all 271
  over ~100s). It is **digest-gated**: the owner sends each backup a per-partition
  content hash and transfers data *only* to backups that report a mismatch, so a
  steady-state pass over in-sync replicas costs one tiny digest RPC per backup
  and moves nothing. A backup that missed a write during a transient blip is
  detected by the digest and healed without waiting for a rebalance. It is
  push-only (heals missing/stale values; reconciling a key a backup kept after
  missing a delete needs the roadmapped replace semantics), and the
  `medusa_entries_reconciled_total` metric counts the re-pushed entries.
- **Elastic scaling** — when a node joins, the partitions it now owns migrate to
  it automatically (verified in k8s: scaling 3→5 redistributes data and the new
  pods serve their share). Each rebalance is time-boxed to one maintenance
  interval so a slow or unreachable peer can't freeze gossip and failure
  detection; an incomplete pass retries on the next tick (the hand-off is
  idempotent), and a node drops the data it hands off only after recording the
  removal in its WAL atomically with the drop — so a crash mid-rebalance neither
  loses a racing write nor resurrects handed-off data.
- **Zero-data-loss rolling restart** — on SIGTERM a node hands off its partitions
  and announces its departure before exiting, so peers rebalance with the data
  already in place (verified in k8s: a rolling restart of all 3 pods preserves
  100% of entries).
- **Entry TTL** — `Map.PutTTL` (or `PUT …?ttl=5s`) sets a per-entry expiry;
  expired entries read as absent (lazy) and are reclaimed by a background sweeper
  (active). TTL is replicated to backups and preserved across migration.
- **Bounded memory (max-size eviction)** — `Config.MaxEntries` (env
  `MEDUSA_MAX_ENTRIES`, default 0 = unbounded) caps the entries a node *owns*.
  When over it the maintenance loop evicts a bounded batch of owned entries (a
  roughly random selection — no per-access bookkeeping, so the hot read path
  stays alloc-free), replicating each removal so backups stay consistent and
  anti-entropy won't resurrect them. The cap is on owned entries (backups can't
  be evicted), so per-node memory is bounded to about `MaxEntries × (1+Backups)`.
  It is cache-style: eviction deletes entries cluster-wide, so enable it only
  where losing entries under pressure is acceptable. `medusa_entries_evicted_total`
  counts them.
- **Observability** — a Prometheus `GET /metrics` endpoint (hand-rolled, no
  dependency) exposes op counts, members, entries, evictions, migrations,
  anti-entropy re-pushes, and TTL sweeps; structured logs via stdlib `slog`
  (JSON in the node binary).
- **Security** — set `MEDUSA_AUTH_TOKEN` (or `httpapi.WithToken`) to require an
  `Authorization: Bearer <token>` header on every admin/data route (the
  `/healthz` and `/readyz` probes stay open for the kubelet; the token is
  compared in constant time). The inter-node data plane can run over **mutual
  TLS** — set `Config.TLS` (or `MEDUSA_TLS_CERT` / `MEDUSA_TLS_KEY` /
  `MEDUSA_TLS_CA`) and node-to-node RPC upgrades from cleartext h2c to HTTP/2
  over TLS, with peers verified in both directions. The admin API also caps
  request bodies (16 MiB) and the node binary sets HTTP read/write/idle timeouts,
  so an oversized or slow client cannot exhaust memory or goroutines.
- **Persistence (snapshot + write-ahead log)** — with `Config.DataDir` (env
  `MEDUSA_DATA_DIR`, a PVC in k8s) each node snapshots its store to disk
  periodically and on shutdown, *and* appends every mutation to an fsync'd
  write-ahead log before acknowledging it. On start it reloads the snapshot and
  replays the WAL on top, so even an *ungraceful* whole-cluster crash loses no
  acknowledged write; a checkpoint truncates the WAL after each snapshot to
  bound replay. Verified in k8s: delete all pods, data reloads from each PVC.
- **Cluster membership** — nodes join via seeds, gossip their views to
  converge, and a heartbeat detector evicts a peer after several missed beats
  (tombstoned so gossip can't resurrect it; an explicit rejoin clears it). No
  coordinator: each node derives an identical partition table from the (sorted)
  member set. Crashes are survived with zero data loss (verified in k8s by
  force-killing a pod).
- **Node auto-discovery** — instead of a hand-maintained seed list, point a node
  at a DNS name (`MEDUSA_DISCOVERY=dns:medusa`) and the maintenance loop resolves
  it to the current peer set every tick. Aimed at a Kubernetes headless Service:
  the cluster self-assembles and scaling the StatefulSet up or down needs no
  config change. A static seed list stays available behind the same
  `discovery.Discoverer` interface (`Config.Discovery`), so the mechanism is
  pluggable and the default is unchanged.
- **Poseidon HTTP/2 transport** — node-to-node RPC runs over the
  [poseidon-http-client](https://github.com/lodgvideon/poseidon-http-client) /
  [poseidon-http-server](https://github.com/lodgvideon/poseidon-http-server)
  stack (h2c cleartext by default, or HTTP/2 over TLS with `Config.TLS`). A
  custom length-prefixed raw-TCP transport and an in-memory transport sit behind
  the same interface for large payloads and fast tests respectively.

## Architecture

```
                         ┌──────────────────────────┐
   client code  ───────► │  medusa.Node             │   public API
                         │  (dispatch by msg type)  │
                         └────────────┬─────────────┘
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                         ▼
      ┌──────────────┐      ┌──────────────────┐      ┌──────────────────┐
      │  cluster     │      │  imap            │      │  transport       │
      │  membership  │◄────►│  Service + Map   │◄────►│  TCP / in-memory │
      │  + table     │      │  + sharded store │      │  framed protobuf │
      └──────┬───────┘      └────────┬─────────┘      └──────────────────┘
             │                       │
             ▼                       ▼
      ┌──────────────┐      ┌──────────────────┐
      │  partition   │      │  codec           │
      │  hash+table  │      │  vtproto 0-alloc │
      └──────────────┘      └──────────────────┘
```

| Package      | Responsibility |
|--------------|----------------|
| `codec`      | Zero-alloc marshal/unmarshal helpers over vtprotobuf; reusable buffer pool. |
| `transport`  | `Transport` interface with three implementations: Poseidon HTTP/2 (default), raw framed TCP, and an in-memory `Switch`. |
| `partition`  | `For(key)` hash and the deterministic partition→owner/backup `Table`. |
| `cluster`    | `Membership`: join, gossip, heartbeat eviction; derives the partition table. |
| `discovery`  | `Discoverer` interface for finding peers: a `Static` seed list or `DNS` resolution (e.g. a headless Service). |
| `imap`       | The distributed map: owner routing, backup replication, sharded local store. |
| `medusa`     | `Node`: wires the layers together and dispatches inbound frames. |
| `genproto/`  | Generated protobuf code (committed; regenerate with `make gen`). |

## Wire protocol

A medusa RPC is one request/response carrying a message type and a protobuf
payload; the `Transport` interface hides how that crosses the network.

- **Poseidon transport (default):** each RPC is an HTTP/2 `POST` over h2c — the
  message type rides in an `m-type` header, the protobuf payload is the body,
  and a handler error becomes a `500` carrying a protobuf `Error`.
- **Raw TCP transport:** a frame is `[uint32 big-endian payload length][1 byte
  message type][payload]`. The single type byte tells the receiver which
  vtprotobuf message to decode — no wrapping `oneof`, which would add a heap
  allocation per RPC.

Schemas live in [`proto/medusa/v1`](proto/medusa/v1).

### Poseidon transport sizing (v0.3.0)

The per-message ceiling is the advertised HTTP/2 **stream window**, which medusa
sets to **16 MiB** on both ends. The client chunks a body into 16 KiB DATA
frames and the server refunds the connection window as it reads, so values up to
several MiB round-trip fine (verified to 1 MiB in tests; the cap is reached only
*at* the 16 MiB window because v0.3.0 doesn't refund the per-stream window
mid-read). Raise `initialWindow` in `transport/poseidon.go` (max 2³¹−1) for
larger values, or inject the raw TCP transport via
`Config.Transport: transport.NewTCP(addr)`.

Two integration gotchas worth noting (both worked around in `transport/poseidon.go`):

- `Serve` must receive a **cancelable** context — it blocks on `ctx.Done()`, and
  `context.Background().Done()` is nil, so `Close` would hang. The listener must
  also be closed explicitly; `srv.Close()` alone doesn't unblock `Accept` when no
  connection was ever made.
- The net/http compatibility layer leaves `r.Body == nil` for empty-body
  requests (stdlib guarantees non-nil), so the handler guards before reading.

## On "zero allocation"

Standard `google.golang.org/protobuf` allocates on every `Marshal`. We use
[vtprotobuf](https://github.com/planetscale/vtprotobuf) (`marshal+unmarshal+size`)
to marshal into buffers we own and reuse. Concretely:

- **Encoding** into a warm buffer: **0 allocs** (asserted by `TestMarshalZeroAlloc`).
- **Decoding** value-carrying messages into a reused struct: **0 allocs**
  (`TestUnmarshalZeroAlloc`).
- **Local map read** (`Map.Get` on a locally-owned key): **0 allocs**
  (`TestLocalGetZeroAlloc`) — `m[string(key)]` lookups are alloc-free by the Go
  compiler, and the stored value slice is returned directly (read-only).
- **Server read handler** marshals the value straight into the connection's
  reusable response buffer.

Honest caveats: **writes allocate** (the value must be copied into the store);
a *remote* read incurs a small bounded number of allocations from protobuf
`string` fields (e.g. the map name) on the receiver; and the **Poseidon HTTP/2
transport allocates** as any HTTP/2 stack does (HPACK, framing, header maps).
The zero-alloc guarantees are scoped to serialization, local reads, and the
in-memory transport — not the cross-node HTTP/2 path. The string-field
allocations can be eliminated by interning map names — see the roadmap.

## Quick start

```go
package main

import (
	"context"
	"fmt"

	"github.com/lodgvideon/medusa"
)

func main() {
	ctx := context.Background()

	// Seed node.
	a, _ := medusa.New(medusa.Config{ID: "a", Addr: "127.0.0.1:7701"})
	defer a.Close()

	// Second node joins via the seed.
	b, _ := medusa.New(medusa.Config{ID: "b", Addr: "127.0.0.1:7702"})
	defer b.Close()
	_ = b.Join(ctx, []string{"127.0.0.1:7701"})

	// Write on one node, read it back from the other — it routes to the owner.
	_ = a.Map("users").Put(ctx, []byte("alice"), []byte("active"))
	v, ok, _ := b.Map("users").Get(ctx, []byte("alice"))
	fmt.Printf("alice=%s found=%v\n", v, ok) // alice=active found=true
}
```

## Building & testing

Requires **Go 1.26+**. The protobuf toolchain installs via `go install` (no
manual protoc download — `buf` bundles its own compiler):

```bash
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@latest
```

```bash
make gen     # regenerate protobuf code (buf lint + generate)
make test    # run all tests
make cover   # coverage report (source total excludes generated code)
make bench   # benchmarks
```

> **Race detector:** `go test -race` needs cgo + a C compiler, which is not
> required to build or test medusa. Install one (e.g. mingw-w64 gcc) to enable
> `-race`; the concurrency tests still validate behavior without it.

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs gofmt, `go vet`,
`go build`, `go test -race` (the runner has a C compiler), a coverage report, and
a `docker build` on every push and pull request.

## Running on Kubernetes

A 3-node cluster ships in [`k8s/medusa.yaml`](k8s/medusa.yaml) (headless Service
for peer DNS, ClusterIP Service for the admin API, StatefulSet with health
probes). The node binary is `cmd/medusa-node`, configured via env
(`MEDUSA_ID`, `MEDUSA_ADDR`, `MEDUSA_DISCOVERY`, `MEDUSA_BACKUPS`,
`MEDUSA_MAX_ENTRIES`, `MEDUSA_DATA_DIR`); nodes self-assemble via a background
maintenance loop that
retries joining until the cluster converges.

Peers are found by **auto-discovery**: the manifest sets
`MEDUSA_DISCOVERY=dns:medusa`, so each node resolves the headless Service to the
current pod set every tick (no seed list — scaling needs no manifest change).
The Service sets `publishNotReadyAddresses: true` so a pod is discoverable the
instant it starts. Set `MEDUSA_SEEDS` and drop `MEDUSA_DISCOVERY` to fall back to
a static seed list.

```bash
docker build -t medusa:dev .
kubectl apply -f k8s/medusa.yaml
kubectl rollout status statefulset/medusa
kubectl port-forward svc/medusa-http 8080:8080
curl -X PUT --data-binary hello localhost:8080/v1/maps/grid/k   # store on one node
curl localhost:8080/v1/maps/grid/k                              # read from any node
```

Nodes advertise their stable pod DNS name (`medusa-N.medusa:7700`) while binding
on `:7700`, so membership survives restarts even as pod IPs change — a rolling
restart re-converges to a full cluster. Scaling out migrates data to new pods
(`/stats` exposes each node's `localEntries`). Graceful-leave handoff (so a
scale-down or rolling restart never loses data) and persistence are on the
roadmap.

An automated end-to-end suite ([`k8s/e2e.sh`](k8s/e2e.sh)) deploys a fresh
cluster and asserts formation, cross-pod get, scale-out migration, and
zero-data-loss rolling restart, then tears down. It skips cleanly when no
cluster is reachable:

```bash
bash k8s/e2e.sh            # or: go test -tags k8s -run TestK8sE2E -timeout 15m .
```

> On Docker Desktop's (kind-based) Kubernetes, a rebuilt image under the same
> tag is not re-synced into the cluster — bump the tag (`medusa:v2`, …); the
> e2e script uses a fresh tag each run.

## Design decisions & trade-offs

- **Rendezvous (HRW) partition assignment.** Each partition ranks members by a
  deterministic weight `hash(memberID, p)`; the top member owns it and the next
  few own its backups. A membership change reassigns only the partitions whose
  top-ranked member changed — about `Count/n` of them — so elastic scaling moves
  minimal data. Trade-off vs. a plain `owner = p mod n` round-robin: balance is
  statistical (≈`Count/n` per member) rather than exact.
- **Configurable synchronous backups.** `Config.Backups` (default 1) sets how
  many distinct peers each write is replicated to before the cluster keeps it,
  so it tolerates that many simultaneous failures. Replication is synchronous
  and fan-out style (no quorum, no read-repair yet); more backups cost
  proportionally more write traffic and memory.
- **Simple failure detector.** A single missed heartbeat evicts. Fine for small
  clusters and tests; a production detector needs N consecutive misses
  (phi-accrual) to ride out transient blips.
- **Read-only returned slices.** Local `Get` returns the stored slice directly
  to stay alloc-free; callers must not mutate it.
- **Poseidon HTTP/2 as the default transport.** Node-to-node RPC dogfoods the
  Poseidon client/server stack over h2c, behind the same `Transport` interface
  as the raw-TCP and in-memory implementations. Trade-off: HTTP/2 allocates
  (HPACK, framing) and a single message must fit the advertised 16 MiB stream
  window; raw TCP remains available for anything larger. Setting `Config.TLS`
  switches the same transport to HTTP/2 over TLS (ALPN `h2`) for mutual
  authentication between nodes, at the cost of the TLS handshake per connection.

### Roadmap

- Replace-semantics reconciliation: extend the digest-gated anti-entropy so that
  on a mismatch the backup also *removes* keys the owner no longer holds (missed
  deletes), not just receives missing/stale values — the push-only residual.
  Doing it safely needs a per-entry version so a freshly-replicated write is not
  pruned by a slightly-stale reconcile manifest.
- Strict fence monotonicity: synchronous/consensus replication or a quiescent
  partition handoff so the fenced-lock token stays strictly increasing even
  across an ungraceful crash or a migration window (today it can be reissued in
  those cases — see the `Map.Lock` caveat).
- Intern map names (or use integer map handles) to make the remote read path
  fully zero-alloc.
- Phi-accrual failure detection with a background heartbeat loop.
