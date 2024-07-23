#
# Tool prereqs check
#
BUILD_PREREQUISITES = git go
VALIDATION_PREREQUISITES = golangci-lint

TARGET=target
BIN=$(TARGET)/bin


.PHONY: lint
lint: clean validation_deps
	@echo "Linting application"
	@golangci-lint run --timeout=10m ./...

test-setup:
	@which gotestsum 2>&1 > /dev/null || go install gotest.tools/gotestsum@latest

.PHONY: clean
clean:
	@ rm -rf target
	@ go mod tidy

.PHONY: build_deps
build_deps: clean
	@ printf $(foreach exec,$(BUILD_PREREQUISITES), \
	$(if $(shell which $(exec)),"", \
	$(error "No $(exec) in PATH.  Prequisites are $(BUILD_PREREQUISITES)")))

.PHONY: validation_deps
validation_deps: clean
	@ printf $(foreach exec,$(VALIDATION_PREREQUISITES), \
	$(if $(shell which $(exec)),"", \
	$(error "No $(exec) in PATH.  Prequisites are $(VALIDATION_PREREQUISITES)")))

.PHONY: test
test: clean build_deps test-setup unit

.PHONY: unit
unit:
	@echo "Running tests without race detector"
	@ CGO_ENABLED=0 gotestsum --format testname -- -cover -coverprofile=coverage.out ./...
	@echo "Running tests with race detector"
	@ CGO_ENABLED=1 gotestsum --format testname -- -race ./...

.PHONY: build
build: build_deps
	@echo "Building application"
	@ go build -o $(BIN)/s3migration
