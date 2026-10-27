# syntax=docker/dockerfile:1

# Multi-platform index digests from the Docker Official Images registry.
ARG GO_IMAGE=golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd
ARG RUNTIME_IMAGE=debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
COPY third_party/etcd-server/go.mod third_party/etcd-server/go.sum ./third_party/etcd-server/
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY third_party/etcd-server ./third_party/etcd-server
COPY container/collect-licenses.sh /collect-licenses.sh
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags='-s -w' -o /out/ ./cmd/...
RUN --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    sh /collect-licenses.sh /licenses

FROM ${RUNTIME_IMAGE}
ARG VERSION=dev
ARG REVISION=unknown
ARG SOURCE=https://github.com/yckao/etcd-trend
LABEL org.opencontainers.image.title="etcd trend" \
      org.opencontainers.image.description="Passive WAL recording, offline etcd replay, analysis, Web UI and TUI" \
      org.opencontainers.image.source=${SOURCE} \
      org.opencontainers.image.version=${VERSION} \
      org.opencontainers.image.revision=${REVISION}
COPY --from=build /out/ /usr/local/bin/
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /licenses/ /usr/share/licenses/etcd-trend/
COPY --chmod=0755 container/entrypoint.sh /usr/local/bin/etcd-trend
COPY third_party/etcd-server/LICENSE /usr/share/licenses/etcd-server/LICENSE
COPY third_party/etcd-server/REPLAY_PATCHES.md /usr/share/licenses/etcd-server/REPLAY_PATCHES.md
# Numeric IDs also work when callers supply a host UID:GID at runtime.
RUN mkdir -p /data && chown 65532:65532 /data
ENV HOME=/tmp
WORKDIR /data
USER 65532:65532
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/etcd-trend"]
CMD ["help"]
