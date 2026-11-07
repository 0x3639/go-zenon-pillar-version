#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
command -v docker >/dev/null || { echo 'Docker is required; dependency code never runs on the host.' >&2; exit 1; }
docker run --rm --user "$(id -u):$(id -g)" --cap-drop ALL --security-opt no-new-privileges \
  --mount "type=bind,src=$(pwd),dst=/src" -w /src \
  -e GOMODCACHE=/src/.work/gomod -e GOCACHE=/src/.work/gocache \
  golang:1.26-bookworm@sha256:5de7c99eb78ca5e515a4c340235d0955ec5964a5071c492c15503d7c267da473 \
  go mod download
docker run --rm --user "$(id -u):$(id -g)" --network none --cap-drop ALL --security-opt no-new-privileges \
  --mount "type=bind,src=$(pwd),dst=/src" -w /src \
  -e GOMODCACHE=/src/.work/gomod -e GOCACHE=/src/.work/gocache \
  golang:1.26-bookworm@sha256:5de7c99eb78ca5e515a4c340235d0955ec5964a5071c492c15503d7c267da473 \
  sh -c 'go test -race -count=1 ./... && go test -tags upstream_integration -count=1 ./internal/runner && go vet ./... && go build -mod=readonly -trimpath -o .work/observer ./cmd/observer'
