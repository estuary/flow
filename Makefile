##########################################################################
# Configuration:

# Git version & date which are injected into built binaries.
export FLOW_VERSION = $(shell git describe --dirty --tags)
DATE    = $(shell date +%F-%T-%Z)
# Number of available processors for parallel builds.
NPROC := $(if ${NPROC},${NPROC},$(shell nproc))
# Configured Rust installation path of built release targets.
# Caller may override with a CARGO_TARGET_DIR environment variable.
# See: https://doc.rust-lang.org/cargo/reference/environment-variables.html
CARGO_TARGET_DIR ?= target
UNAME := $(shell uname -sp)

# Unfortunately, cargo's build cache get's completely invalidated when you switch between the
# default target and an explicit --target argument. We work around this by setting an explicit
# target. Thus, when running `cargo build` (without an
# explicit --target), the artifacts will be output to target/$TARGET/. This allows
# developers to omit the --target in most cases, and still be able to run make commands that can use
# the same build cache.
# See: https://github.com/rust-lang/cargo/issues/8899
ifeq ($(UNAME),Darwin arm)
export CARGO_BUILD_TARGET=aarch64-apple-darwin
PACKAGE_ARCH=arm64-darwin
ETCD_ARCH=darwin-arm64
ETCD_SHASUM=33094133a771b2d086dc04f2ede41c249258947042de72132af127972880171f
ETCD_EXT=zip
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS=-C linker=musl-gcc
else ifeq ($(UNAME),Darwin i386)
export CARGO_BUILD_TARGET=x86_64-apple-darwin
PACKAGE_ARCH=x86-darwin
ETCD_ARCH=darwin-amd64
ETCD_SHASUM=8bd279948877cfb730345ecff2478f69eaaa02513c2a43384ba182c9985267bd
ETCD_EXT=zip
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS=-C linker=musl-gcc
else
export CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu
PACKAGE_ARCH=x86-linux
ETCD_ARCH=linux-amd64
ETCD_SHASUM=7910a2fdb1863c80b885d06f6729043bff0540f2006bf6af34674df2636cb906
ETCD_EXT=tar.gz
endif
RUSTBIN = ${CARGO_TARGET_DIR}/${CARGO_BUILD_TARGET}/release
RUST_MUSL_BIN = ${CARGO_TARGET_DIR}/x86_64-unknown-linux-musl/release

# Location to place intermediate files and output artifacts
# during the build process. Note the go tool ignores directories
# with leading '.' or '_'.
WORKDIR  = $(realpath .)/.build
# Packaged build outputs.
PKGDIR = ${WORKDIR}/package

# Etcd release we pin within Flow distributions.
ETCD_VERSION = v3.5.5

# PROTOC_INC_GO_MODULES are Go modules which must be resolved and included
# with `protoc` invocations
PROTOC_INC_GO_MODULES = \
	github.com/golang/protobuf \
	github.com/gogo/protobuf \
	go.gazette.dev/core

# Targets of Go protobufs which must be compiled.
GO_PROTO_TARGETS = \
	./go/protocols/capture/capture.pb.go \
	./go/protocols/flow/flow.pb.go \
	./go/protocols/materialize/materialize.pb.go

# GO_MODULE_PATH expands a $(module), like "go.gazette.dev/core", to the local path
# of its respository as currently specified by go.mod. The `go list` tool
# is used to map submodules to corresponding go.mod versions and paths.
GO_MODULE_PATH = $(shell go list -f '{{ .Dir }}' -m $(module))

##########################################################################
# Configure Go build & test behaviors.

# Tell the go-sqlite3 package to link against a pre-built library
# rather than statically compiling in its own definition.
# This will use the static library definitions provided by libbindings.a.
GO_BUILD_TAGS += libsqlite3

# Targets which Go targets rely on in order to build.
GO_BUILD_DEPS = \
	${RUSTBIN}/libbindings.a \
	${RUSTBIN}/librocks-exp/librocksdb.a \
	crates/bindings/flow_bindings.h

##########################################################################
# Build rules:

.PHONY: default
default: package

# Rules for protocols
.PHONY: protoc-gen-gogo
protoc-gen-gogo:
	go mod download
	go install github.com/gogo/protobuf/protoc-gen-gogo

# Run the protobuf compiler to generate message and gRPC service implementations.
# Invoke protoc with local and third-party include paths set.
%.pb.go: %.proto protoc-gen-gogo
	PATH=$$PATH:$(shell go env GOPATH)/bin ;\
	protoc -I . $(foreach module, $(PROTOC_INC_GO_MODULES), -I$(GO_MODULE_PATH)) \
		--gogo_out=paths=source_relative,plugins=grpc:. $*.proto

go-protobufs: $(GO_PROTO_TARGETS)


# `etcd` is used for testing, and packaged as a release artifact.
${PKGDIR}/bin/etcd:
		curl -L -o /tmp/etcd.${ETCD_EXT} \
										https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-${ETCD_ARCH}.${ETCD_EXT} \
						&& echo "${ETCD_SHASUM} /tmp/etcd.${ETCD_EXT}" | sha256sum -c - \
						&& if [ "${ETCD_EXT}" = "zip" ]; then \
								unzip /tmp/etcd.${ETCD_EXT} -d /tmp; \
							else \
								tar --extract --file /tmp/etcd.${ETCD_EXT} --directory /tmp/; \
						fi \
						&& mkdir -p ${PKGDIR}/bin/ \
						&& mv /tmp/etcd-${ETCD_VERSION}-${ETCD_ARCH}/etcd /tmp/etcd-${ETCD_VERSION}-${ETCD_ARCH}/etcdctl ${PKGDIR}/bin/ \
						&& chown ${UID}:${UID} ${PKGDIR}/bin/etcd ${PKGDIR}/bin/etcdctl \
						&& rm -r /tmp/etcd-${ETCD_VERSION}-${ETCD_ARCH}/ \
						&& rm /tmp/etcd.${ETCD_EXT} \
						&& $@ --version; \

# Rule for building Go targets.
# go-install rules never correspond to actual files, and are always re-run each invocation.
go-install/%: ${RUSTBIN}/libbindings.a crates/bindings/flow_bindings.h
	MBP=go.gazette.dev/core/mainboilerplate ;\
	./go.sh \
		build -o ${PKGDIR}/bin/$(@F) \
	  -v --tags "${GO_BUILD_TAGS}" \
		-ldflags "-X $${MBP}.Version=${FLOW_VERSION} -X $${MBP}.BuildDate=${DATE}" $*

${PKGDIR}/bin/gazette: go-install/go.gazette.dev/core/cmd/gazette
${PKGDIR}/bin/gazctl:  go-install/go.gazette.dev/core/cmd/gazctl
${PKGDIR}/bin/flowctl-go: $(GO_BUILD_DEPS) $(GO_PROTO_TARGETS) go-install/github.com/estuary/flow/go/flowctl-go

# `sops` is used for encrypt/decrypt of connector configurations.
${PKGDIR}/bin/sops:
	go install go.mozilla.org/sops/v3/cmd/sops@v3.7.3
	cp $(shell go env GOPATH)/bin/sops $@

########################################################################
# Rust outputs:

# The & here declares that this single invocation will produce all of the files on the left hand
# side. flow_bindings.h is generated by the bindings build.rs.
.PHONY: ${RUSTBIN}/libbindings.a crates/bindings/flow_bindings.h
${RUSTBIN}/libbindings.a crates/bindings/flow_bindings.h &:
	cargo build --release --locked -p bindings

.PHONY: ${RUSTBIN}/librocks-exp/librocksdb.a
${RUSTBIN}/librocks-exp/librocksdb.a:
	cargo build --release --locked -p librocks-exp

.PHONY: ${RUSTBIN}/flowctl-admin
${RUSTBIN}/flowctl-admin:
	cargo build --release --locked -p flowctl-admin

# Statically linked binaries using MUSL:

.PHONY: ${RUST_MUSL_BIN}/flow-connector-proxy
${RUST_MUSL_BIN}/flow-connector-proxy:
	cargo build --target x86_64-unknown-linux-musl --release --locked -p connector_proxy

.PHONY: ${RUST_MUSL_BIN}/flow-network-tunnel
${RUST_MUSL_BIN}/flow-network-tunnel:
	cargo build --target x86_64-unknown-linux-musl --release --locked -p network-tunnel

.PHONY: ${RUST_MUSL_BIN}/flow-parser
${RUST_MUSL_BIN}/flow-parser:
	cargo build --target x86_64-unknown-linux-musl --release --locked -p parser

.PHONY: ${RUST_MUSL_BIN}/flow-schema-inference
${RUST_MUSL_BIN}/flow-schema-inference:
	cargo build --target x86_64-unknown-linux-musl --release --locked -p schema-inference

.PHONY: ${RUST_MUSL_BIN}/flow-schemalate
${RUST_MUSL_BIN}/flow-schemalate:
	cargo build --target x86_64-unknown-linux-musl --release --locked -p schemalate


########################################################################
# Final output packaging:

RUST_TARGETS = \
	${PKGDIR}/bin/etcd \
	${PKGDIR}/bin/flowctl-admin \
	${PKGDIR}/bin/flowctl-go \
	${PKGDIR}/bin/gazette \
	${PKGDIR}/bin/sops

MUSL_TARGETS = \
	${PKGDIR}/bin/flow-connector-proxy \
	${PKGDIR}/bin/flow-network-tunnel \
	${PKGDIR}/bin/flow-parser \
	${PKGDIR}/bin/flow-schema-inference \
	${PKGDIR}/bin/flow-schemalate

.PHONY: rust-binaries
rust-binaries: $(RUST_TARGETS)

.PHONY: musl-binaries
musl-binaries: $(MUSL_TARGETS)

${PKGDIR}/flow-$(PACKAGE_ARCH).tar.gz: $(RUST_TARGETS) $(MUSL_TARGETS)
	rm -f $@
	cd ${PKGDIR}/bin && tar -zcf ../flow-$(PACKAGE_ARCH).tar.gz *

.PHONY: package
package: ${PKGDIR}/flow-$(PACKAGE_ARCH).tar.gz

${PKGDIR}:
	mkdir -p ${PKGDIR}/bin
	mkdir ${PKGDIR}/lib
${PKGDIR}/bin/flowctl-admin: ${RUSTBIN}/flowctl-admin | ${PKGDIR}
	cp ${RUSTBIN}/flowctl-admin $@

# The following binaries are statically linked, so come from a different subdirectory
${PKGDIR}/bin/flow-connector-proxy: ${RUST_MUSL_BIN}/flow-connector-proxy | ${PKGDIR}
	cp ${RUST_MUSL_BIN}/flow-connector-proxy $@

${PKGDIR}/bin/flow-network-tunnel: ${RUST_MUSL_BIN}/flow-network-tunnel | ${PKGDIR}
	cp ${RUST_MUSL_BIN}/flow-network-tunnel $@

${PKGDIR}/bin/flow-parser: ${RUST_MUSL_BIN}/flow-parser | ${PKGDIR}
	cp ${RUST_MUSL_BIN}/flow-parser $@

${PKGDIR}/bin/flow-schema-inference: ${RUST_MUSL_BIN}/flow-schema-inference | ${PKGDIR}
	cp ${RUST_MUSL_BIN}/flow-schema-inference $@

${PKGDIR}/bin/flow-schemalate: ${RUST_MUSL_BIN}/flow-schemalate | ${PKGDIR}
	cp ${RUST_MUSL_BIN}/flow-schemalate $@

##########################################################################
# Make targets used by CI:

# We use LLVM for faster linking. See .cargo/config.
.PHONY: extra-ci-runner-setup
extra-ci-runner-setup:
	sudo apt install -y \
		libprotobuf-dev \
		libssl-dev \
		musl-tools \
		pkg-config \
		protobuf-compiler
	sudo ln --force --symbolic /usr/bin/ld.lld-12 /usr/bin/ld.lld

.PHONY: print-versions
print-versions:
	echo "Resolved repository version: ${FLOW_VERSION}" \
		&& /usr/bin/ld.lld --version \
		&& cargo version --verbose \
		&& docker --version \
		&& gcloud info \
		&& go version \
		&& jq --version \
		&& node --version \
		&& npm --version \
		&& rustc --version \

.PHONY: install-tools
install-tools: ${PKGDIR}/bin/etcd ${PKGDIR}/bin/sops

.PHONY: rust-test
rust-test:
	cargo test --release --locked --workspace --exclude parser --exclude network-tunnel --exclude schemalate --exclude connector_proxy

.PHONY: musl-test
musl-test:
	cargo test --release --locked --target x86_64-unknown-linux-musl --package parser --package network-tunnel --package schemalate --package connector_proxy

# `go` test targets must have PATH-based access to tools (etcd & sops),
# because the `go` tool compiles tests as binaries within a temp directory,
# and these binaries cannot expect `sops` to be co-located alongside.

.PHONY: go-test-fast
go-test-fast: $(GO_BUILD_DEPS) | ${PKGDIR}/bin/etcd ${PKGDIR}/bin/sops
	PATH=${PKGDIR}/bin:$$PATH ;\
	./go.sh test -p ${NPROC} --tags "${GO_BUILD_TAGS}" ./go/...

.PHONY: go-test-ci
go-test-ci:   $(GO_BUILD_DEPS) | ${PKGDIR}/bin/etcd ${PKGDIR}/bin/sops ${PKGDIR}/bin/flow-connector-proxy ${PKGDIR}/bin/flowctl-admin ${PKGDIR}/bin/flowctl-go
	PATH=${PKGDIR}/bin:$$PATH ;\
	GORACE="halt_on_error=1" ;\
	./go.sh test -p ${NPROC} --tags "${GO_BUILD_TAGS}" --race --count=15 --failfast ./go/...

.PHONY: catalog-test
catalog-test: | ${PKGDIR}/bin/flowctl-admin ${PKGDIR}/bin/flowctl-go ${PKGDIR}/bin/gazette ${PKGDIR}/bin/etcd ${PKGDIR}/bin/sops flow.schema.json
	${PKGDIR}/bin/flowctl-admin test --source examples/local-sqlite.flow.yaml $(ARGS)

.PHONY: end-to-end-test
end-to-end-test: | ${PKGDIR}/bin/flowctl-admin ${PKGDIR}/bin/flowctl-go ${PKGDIR}/bin/flow-connector-proxy ${PKGDIR}/bin/gazette ${PKGDIR}/bin/etcd ${PKGDIR}/bin/sops
	./tests/run-all.sh

flow.schema.json: | ${PKGDIR}/bin/flowctl-admin ${PKGDIR}/bin/flowctl-go
	${PKGDIR}/bin/flowctl-admin json-schema > $@

# These docker targets intentionally don't depend on any upstream targets. This is because the
# upstream targes are all PHONY as well, so there would be no way to prevent them from running twice if you
# invoke e.g. `make package` followed by `make docker-image`. If the `docker-image` target depended
# on the `package` target, it would not skip the package step when you invoke `docker-image`.
# For now, the github action workflow manually invokes make to perform each of these tasks.
.PHONY: docker-image
docker-image:
	docker build \
		--file .devcontainer/release.Dockerfile \
		--tag ghcr.io/estuary/flow:${FLOW_VERSION} \
		--tag ghcr.io/estuary/flow:dev \
		${PKGDIR}/

.PHONY: docker-push
docker-push:
	docker push ghcr.io/estuary/flow:${FLOW_VERSION}

# This is used by the GH Action workflow to push the 'dev' tag.
# It is invoked only for builds on the master branch.
.PHONY: docker-push-dev
docker-push-dev:
	docker push ghcr.io/estuary/flow:dev
