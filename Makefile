.PHONY: dev-cli dev-gui dev-web

# OpenTuttiVM Go-module validation. Caches point at /tmp so nothing lands
# in system directories.
GOMODCACHE := /tmp/gomod
GOCACHE := /tmp/gocache
GOPATH := /tmp/gopath
OT_GOWORK := off

.PHONY: check-server check-room-sync check-workspace check-fs

check-server:
	cd services/open-tutti-server && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) GOWORK=$(OT_GOWORK) test -z "$$(gofmt -l .)" && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) GOWORK=$(OT_GOWORK) go vet ./... && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) GOWORK=$(OT_GOWORK) go test ./...

check-room-sync:
	cd services/open-tutti-room-sync && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) GOWORK=$(OT_GOWORK) test -z "$$(gofmt -l .)" && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) GOWORK=$(OT_GOWORK) go vet ./... && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) GOWORK=$(OT_GOWORK) go test ./...

check-workspace:
	cd packages/workspace/vm-sync && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) go test .
	cd packages/agent/borrow && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) go test .
	cd packages/workspace/vm-protocol && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) go test .
	cd packages/workspace/vm-cas && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) go test .
	cd packages/workspace/vm-roomfs && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) GOWORK=off go test .
	test -z "$$(gofmt -l packages/workspace)"

check-fs:
	cd services/open-tutti-fs && GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) GOWORK=$(OT_GOWORK) go test ./...
	GOOS=linux GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) GOWORK=$(OT_GOWORK) go -C services/open-tutti-fs build ./...
	GOOS=linux GOMODCACHE=$(GOMODCACHE) GOCACHE=$(GOCACHE) GOPATH=$(GOPATH) GOWORK=$(OT_GOWORK) go -C services/open-tutti-fs vet ./...
	test -z "$$(gofmt -l services/open-tutti-fs)"

NODE_PATH_PREFIX := $(shell node_major="$$(tr -d '[:space:]' < .node-version)"; node_dir="$$(find "$$HOME/.nvm/versions/node" -maxdepth 1 -type d -name "v$${node_major}.*" 2>/dev/null | awk -F/ '{ path = $$0; version = $$NF; sub(/^v/, "", version); split(version, parts, "."); printf "%d %d %d %s\n", parts[1], parts[2], parts[3], path }' | sort -n -k1,1 -k2,2 -k3,3 | tail -n 1 | cut -d ' ' -f 4-)"; if [ -n "$$node_dir" ] && [ -x "$$node_dir/bin/node" ]; then printf '%s:' "$$node_dir/bin"; fi)
PNPM ?= PATH="$(NODE_PATH_PREFIX)$(PATH)" corepack pnpm@10.11.0


dev-cli:
	@$(PNPM) dev:cli

dev-gui:
	@bash ./tools/scripts/dev-gui.sh

dev-web:
	@$(PNPM) dev:web
