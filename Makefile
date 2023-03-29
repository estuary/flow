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

ifeq ($(UNAME),Darwin arm)
DENO_ARCH=aarch64-apple-darwin
ETCD_ARCH=darwin-arm64
ETCD_EXT=zip
ETCD_SHASUM=33094133a771b2d086dc04f2ede41c249258947042de72132af127972880171f
PACKAGE_ARCH=arm64-darwin
else ifeq ($(UNAME),Darwin i386)
DENO_ARCH=x86_64-apple-darwin
ETCD_ARCH=darwin-amd64
ETCD_EXT=zip
ETCD_SHASUM=8bd279948877cfb730345ecff2478f69eaaa02513c2a43384ba182c9985267bd
PACKAGE_ARCH=x86-darwin
else
DENO_ARCH=x86_64-unknown-linux-gnu
ETCD_ARCH=linux-amd64
ETCD_EXT=tar.gz
ETCD_SHASUM=7910a2fdb1863c80b885d06f6729043bff0540f2006bf6af34674df2636cb906
PACKAGE_ARCH=x86-linux
endif
RUSTBIN = ${CARGO_TARGET_DIR}/release

# Packaged build outputs.
PKGDIR = $(realpath .)/.build/package

# Deno and Etcd release we pin within Flow distributions.
DENO_VERSION = v1.32.1
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
	./go/protocols/derive/derive.pb.go \
	./go/protocols/flow/flow.pb.go \
	./go/protocols/materialize/materialize.pb.go \
	./go/protocols/ops/ops.pb.go \
	./go/protocols/runtime/runtime.pb.go

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
%.pb.go: %.proto
	PATH=$$PATH:$(shell go env GOPATH)/bin ;\
	protoc -I . $(foreach module, $(PROTOC_INC_GO_MODULES), -I$(GO_MODULE_PATH)) \
		--gogo_out=paths=source_relative,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,plugins=grpc:. $*.proto

go-protobufs: $(GO_PROTO_TARGETS)

# `deno` is used for running user TypeScript derivations.
${PKGDIR}/bin/deno:
	curl -L -o /tmp/deno.zip \
			https://github.com/denoland/deno/releases/download/${DENO_VERSION}/deno-${DENO_ARCH}.zip \
		&& unzip /tmp/deno.zip -d /tmp \
		&& rm /tmp/deno.zip \
		&& mkdir -p ${PKGDIR}/bin/ \
		&& mv /tmp/deno ${PKGDIR}/bin/

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

# `sops` is used for encrypt/decrypt of connector configurations.
${PKGDIR}/bin/sops:
	go install go.mozilla.org/sops/v3/cmd/sops@v3.7.3
	cp $(shell go env GOPATH)/bin/sops $@

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

########################################################################
# Rust outputs:

RUST_TARGETS = \
	${RUSTBIN}/agent \
	${RUSTBIN}/flow-connector-init \
	${RUSTBIN}/flow-network-tunnel \
	${RUSTBIN}/flow-parser \
	${RUSTBIN}/flow-schema-inference \
	${RUSTBIN}/flow-schemalate \
	${RUSTBIN}/flowctl \

# The & here declares that this single invocation will produce all of the files on the left hand
# side. flow_bindings.h is generated by the bindings build.rs.
$(RUST_TARGETS) $(GO_BUILD_DEPS) &:
	cargo build --release --locked --workspace --exclude flow-web

# CARGO_DOCKER is an alias for running cargo within a Linux container.
# It's run only locally, to support developer machines with are not x64 Linux,
# or having different GLIBC versions than that of our official CI runner.
CARGO_DOCKER := docker run -it --rm \
	--user "$$(id -u)":"$$(id -g)" \
	-v "$(realpath .)":/opt/workspace \
	-v "$$HOME"/.cargo:/usr/local/cargo \
	--workdir /opt/workspace \
	-e SKIP_PROTO_BUILD=1 \
	-e CARGO_TARGET_DIR=target/docker \
	rust:1.67 cargo

.PHONY: local-docker-binaries
local-docker-binaries:
	$(CARGO_DOCKER) build --release --offline --locked -p connector-init -p parser -p schemalate
	cp target/docker/release/flow-connector-init ${PKGDIR}/bin/flow-connector-init
	cp target/docker/release/flow-parser ${PKGDIR}/bin/flow-parser
	cp target/docker/release/flow-schemalate ${PKGDIR}/bin/flow-schemalate

########################################################################
# Final output packaging:

ALL_BINARIES = \
	${PKGDIR}/bin/agent \
	${PKGDIR}/bin/deno \
	${PKGDIR}/bin/etcd \
	${PKGDIR}/bin/flow-connector-init \
	${PKGDIR}/bin/flow-network-tunnel \
	${PKGDIR}/bin/flow-parser \
	${PKGDIR}/bin/flow-schema-inference \
	${PKGDIR}/bin/flow-schemalate \
	${PKGDIR}/bin/flowctl \
	${PKGDIR}/bin/flowctl-go \
	${PKGDIR}/bin/gazette \
	${PKGDIR}/bin/sops

.PHONY: all-binaries
all-binaries: $(ALL_BINARIES)

${PKGDIR}/flow-$(PACKAGE_ARCH).tar.gz:
	rm -f $@
	cd ${PKGDIR}/bin && tar -zcf ../flow-$(PACKAGE_ARCH).tar.gz *

.PHONY: package
package: ${PKGDIR}/flow-$(PACKAGE_ARCH).tar.gz

${PKGDIR}:
	mkdir -p ${PKGDIR}/bin
	mkdir ${PKGDIR}/lib

${PKGDIR}/bin/flow-connector-init: ${RUSTBIN}/flow-connector-init | ${PKGDIR}
	cp ${RUSTBIN}/flow-connector-init $@

${PKGDIR}/bin/flow-network-tunnel: ${RUSTBIN}/flow-network-tunnel | ${PKGDIR}
	cp ${RUSTBIN}/flow-network-tunnel $@

${PKGDIR}/bin/flow-parser: ${RUSTBIN}/flow-parser | ${PKGDIR}
	cp ${RUSTBIN}/flow-parser $@

${PKGDIR}/bin/flow-schema-inference: ${RUSTBIN}/flow-schema-inference | ${PKGDIR}
	cp ${RUSTBIN}/flow-schema-inference $@

${PKGDIR}/bin/flow-schemalate: ${RUSTBIN}/flow-schemalate | ${PKGDIR}
	cp ${RUSTBIN}/flow-schemalate $@

${PKGDIR}/bin/flowctl: ${RUSTBIN}/flowctl | ${PKGDIR}
	cp ${RUSTBIN}/flowctl $@

${PKGDIR}/bin/agent: ${RUSTBIN}/agent | ${PKGDIR}
	cp ${RUSTBIN}/agent $@

##########################################################################
# Make targets used by CI:

# We use LLVM for faster linking. See RUSTFLAGS in .github/workflows/main.yml
.PHONY: extra-ci-runner-setup
extra-ci-runner-setup:
	sudo apt install -y \
		libssl-dev \
		pkg-config
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
install-tools: ${PKGDIR}/bin/deno ${PKGDIR}/bin/etcd ${PKGDIR}/bin/sops protoc-gen-gogo

.PHONY: rust-test
rust-test:
	cargo test --release --locked --workspace

# `go` test targets must have PATH-based access to tools (etcd & sops),
# because the `go` tool compiles tests as binaries within a temp directory,
# and these binaries cannot expect `sops` to be co-located alongside.

.PHONY: go-test-fast
go-test-fast: $(GO_BUILD_DEPS) | ${PKGDIR}/bin/deno ${PKGDIR}/bin/etcd ${PKGDIR}/bin/sops
	PATH=${PKGDIR}/bin:$$PATH ;\
	./go.sh test -p ${NPROC} --tags "${GO_BUILD_TAGS}" ./go/...

.PHONY: go-test-ci
go-test-ci:
	PATH=${PKGDIR}/bin:$$PATH ;\
	GORACE="halt_on_error=1" ;\
	./go.sh test -p ${NPROC} --tags "${GO_BUILD_TAGS}" --race --count=15 --failfast ./go/...

.PHONY: data-plane-test-setup
data-plane-test-setup:

ifeq ($(SKIP_BUILD),true)
data-plane-test-setup:
	@echo "testing using pre-built binaries:"
	@ls -al ${PKGDIR}/bin/
	${PKGDIR}/bin/flowctl-go json-schema > flow.schema.json
else
data-plane-test-setup: ${PKGDIR}/bin/flowctl-go ${PKGDIR}/bin/flowctl ${PKGDIR}/bin/flow-connector-init ${PKGDIR}/bin/gazette ${PKGDIR}/bin/deno ${PKGDIR}/bin/etcd ${PKGDIR}/bin/sops flow.schema.json
endif


.PHONY: catalog-test
catalog-test: data-plane-test-setup
	${PKGDIR}/bin/flowctl-go test --source examples/flow.yaml $(ARGS)

.PHONY: end-to-end-test
end-to-end-test: data-plane-test-setup
	./tests/run-all.sh

flow.schema.json: |  ${PKGDIR}/bin/flowctl-go
	${PKGDIR}/bin/flowctl-go json-schema > $@

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
