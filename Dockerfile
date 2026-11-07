FROM golang:1.26-bookworm@sha256:5de7c99eb78ca5e515a4c340235d0955ec5964a5071c492c15503d7c267da473 AS build
RUN useradd --uid 10001 --create-home builder && mkdir /src && chown builder:builder /src
USER 10001:10001
WORKDIR /src
COPY --chown=10001:10001 go.mod go.sum ./
RUN go mod download
COPY --chown=10001:10001 cmd ./cmd
COPY --chown=10001:10001 internal ./internal
RUN go build -mod=readonly -trimpath -o /tmp/observer ./cmd/observer

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc AS runtime
RUN mkdir -p /data && chown 10001:10001 /data
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /tmp/observer /usr/local/bin/observer
COPY LICENSE /usr/share/doc/zenon-p2p-observer/LICENSE
COPY third_party /usr/share/doc/zenon-p2p-observer/third_party
USER 10001:10001
ENV DATA_DIR=/data GOMEMLIMIT=320MiB
ENTRYPOINT ["/usr/local/bin/observer"]
