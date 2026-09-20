GO ?= go
PACKAGES ?= ./...

.PHONY: build test lint

build:
	$(GO) build $(PACKAGES)

test:
	$(GO) test $(PACKAGES)

lint:
	$(GO) vet $(PACKAGES)
