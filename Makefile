# medusa developer tasks. Requires the buf + protoc-gen-go + protoc-gen-go-vtproto
# toolchain on PATH (see "go install" lines in README.md).

.PHONY: gen test cover bench fmt vet check e2e

gen: ## regenerate protobuf code from proto/
	buf lint
	buf generate

test: ## run all tests
	go test ./... -count=1 -timeout 60s

cover: ## run tests with coverage, excluding generated code from the total
	go test ./... -count=1 -coverprofile=coverage.out -timeout 60s
	@grep -v "genproto" coverage.out > coverage.src.out || true
	go tool cover -func=coverage.src.out | tail -1

bench: ## run benchmarks (allocation-sensitive paths assert 0 allocs/op in tests)
	go test ./... -run=^$$ -bench=. -benchmem -timeout 120s

fmt: ## format all Go source
	gofmt -w .

vet: ## static checks
	go vet ./...

e2e: ## run the Kubernetes end-to-end suite (skips if no cluster)
	bash k8s/e2e.sh

check: fmt vet test ## format, vet, and test
