GOLANGCI=github.com/golangci/golangci-lint/cmd/golangci-lint@v1.57.1
gofumpt=mvdan.cc/gofumpt@latest

# Generate labels for all language runtimes
.PHONY: generate
generate: fmt

.PHONY: test
test:
	go test -race -v -count 1 ./...

.PHONY: lint
lint: fmt ## Run linters on all go files
	go run $(GOLANGCI) run -v ./...

install-tools:
	go install gotest.tools/gotestsum@v1.10.0
	go install golang.org/x/tools/cmd/goimports@latest

.PHONY: fmt
fmt: install-tools ## Formats all go files
	go run $(gofumpt) -l -w -extra  .
	find . -type f -name '*.go' -exec grep -L -E 'Code generated by .*\. DO NOT EDIT.' {} + | xargs goimports -format-only -w -local=github.com/rudderlabs
