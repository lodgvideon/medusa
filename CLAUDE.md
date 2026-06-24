# medusa — project rules

## Keep documentation in sync with the code

Documentation is part of "done", not a follow-up. In the **same change** that
alters behavior, update the affected docs before considering the work complete —
never let docs drift from the code. A change is incomplete until its docs *and*
a test/e2e assertion exist; deferring doc updates "for later" is not acceptable.

Update docs whenever a change touches any of:

- **Feature set or public API** — `medusa.Node`, `imap.Map`, `Config` fields,
  EntryProcessors → update the feature list and examples in [README.md](README.md).
- **Configuration** — env vars (`MEDUSA_*`), ports, defaults → README + the
  Kubernetes manifests in [k8s/](k8s/).
- **HTTP surface** (`httpapi`) — endpoints, query params, status codes → README.
- **Operational behavior** — metrics names, log events, health/readiness.
- **Deployment** — `k8s/medusa.yaml` and the e2e suite `k8s/e2e.sh` (its
  assertions are living documentation; extend them when behavior changes).
- **Build/run** — `Makefile`, the toolchain notes in README.

Touchpoints to check before finishing: `README.md`, `k8s/e2e.sh`, `k8s/medusa.yaml`,
`Makefile`, and the package doc comments.

## Other conventions

- **Test-first, ≥90% coverage** on hand-written packages; assert `0 allocs/op`
  on hot paths (`AllocsPerRun`). Generated `genproto/` is excluded from coverage.
- **Verify in Kubernetes**: new cluster-visible behavior gets an assertion in
  `k8s/e2e.sh` (run with `make e2e`; it skips cleanly without a cluster).
- Regenerate protobuf with `make gen` after editing `proto/`; commit the result.
- The race detector needs a C compiler (cgo). It is available locally via
  mingw-w64 gcc at `C:\msys64\mingw64\bin` — prepend it to PATH and run
  `make race` (or `CGO_ENABLED=1 go test -race ./...`). Use it for any change
  touching concurrency.
