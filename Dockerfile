ARG BUILDER_VERSION=master
FROM ghcr.io/estuary/flow-builder:$(BUILDER_VERSION) as builder

RUN mkdir /workspace
WORKDIR /workspace

ARG FLOW_VERSION
ENV FLOW_VERSION=$FLOW_VERSION

COPY . /workspace/

ARG TARGETOS
ARG TARGETARCH

ENV GOOS=$(TARGETOS) GOARCH=$(TARGETARCH) LDDEBUG=1

RUN go mod download
RUN set -ex; echo "targetOs=$TARGETOS, targetArch=$TARGETARCH"; \
	case "$TARGETARCH" in \
		amd64) RUST_TARGET_TRIPLE='x86_64' ;; \
		arm64) RUST_TARGET_TRIPLE='aarch64' ;; \
		*) echo >&2 "unsupported target architecture: ${TARGETARCH}"; exit 1 ;; \
	esac; \
	case "$TARGETOS" in \
		linux) export RUST_TARGET_TRIPLE="${RUST_TARGET_TRIPLE}-unknown-linux-gnu" ;; \
		darwin) export RUST_TARGET_TRIPLE="${RUST_TARGET_TRIPLE}-apple-darwin" ;; \
		*) echo >&2 "unsupported target OS: ${TARGETOS}"; exit 1 ;; \
	esac; \
	make package RUST_TARGET_TRIPLE="$RUST_TARGET_TRIPLE" CARGO_TARGET_DIR="target/${RUST_TARGET_TRIPLE}"

FROM debian:bullseye-slim

ENV PATH=/flow/bin:$PATH
COPY --from=builder /workspace/.build/package/* /flow/bin/

