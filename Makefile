GO ?= go
PACKAGES ?= ./...

.PHONY: build lint test

build:
	$(GO) build $(PACKAGES)

lint:
	@files="$$(rg --files -g '*.go')"; \
	unformatted="$$(gofmt -l $$files)"; \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt check failed:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	$(GO) vet $(PACKAGES)

test:
	$(GO) test $(PACKAGES)
