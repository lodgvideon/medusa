# medusa developer tasks. Requires the buf + protoc-gen-go + protoc-gen-go-vtproto
# toolchain on PATH (see "go install" lines in README.md).

.PHONY: gen test cover bench fmt vet check e2e runner runner-check

gen: ## regenerate protobuf code from proto/
	buf lint
	buf generate

test: ## run all tests
	go test ./... -count=1 -timeout 60s

cover: ## run tests with cross-package coverage, excluding generated + cmd main
	go test ./... -count=1 -coverpkg=./... -coverprofile=coverage.out -timeout 120s
	@grep -vE 'genproto/|cmd/medusa-node/' coverage.out > coverage.src.out || true
	go tool cover -func=coverage.src.out | tail -1

bench: ## run benchmarks (allocation-sensitive paths assert 0 allocs/op in tests)
	go test ./... -run=^$$ -bench=. -benchmem -timeout 120s

fmt: ## format all Go source
	gofmt -w .

vet: ## static checks
	go vet ./...

e2e: ## run the Kubernetes end-to-end suite (skips if no cluster)
	bash k8s/e2e.sh

runner: ## deploy the self-hosted GitHub Actions runner (needs the github-runner Secret)
	kubectl apply -f k8s/runner.yaml

runner-check: ## verify the self-hosted runner registered (skips without a cluster)
	bash k8s/runner-check.sh

check: fmt vet test ## format, vet, and test
