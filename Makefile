##########################################################################
# Configuration:

# Git version & date which are injected into built binaries.
VERSION = $(shell git describe --dirty --tags)
DATE    = $(shell date +%F-%T-%Z)
# Number of available processors for parallel builds.
NPROC := $(if ${NPROC},${NPROC},$(shell nproc))
# Repository root (the directory of the invoked Makefile).
ROOTDIR  = $(abspath $(dir $(firstword $(MAKEFILE_LIST))))
# Configured Go installation path of built targets.
GOBIN = $(shell go env GOPATH)/bin
# Configured Rust installation path of built release targets.
# Caller may override with a CARGO_TARGET_DIR environment variable.
# See: https://doc.rust-lang.org/cargo/reference/environment-variables.html
CARGO_TARGET_DIR ?= ${ROOTDIR}/target
RUSTBIN = ${CARGO_TARGET_DIR}/release
# Location to place intermediate files and output artifacts
# during the build process. Note the go tool ignores directories
# with leading '.' or '_'.
WORKDIR  = ${ROOTDIR}/.build
# Tools used during build & test.
TOOLBIN = ${WORKDIR}/tools
# Packaged build outputs.
PKGDIR = ${WORKDIR}/package
# All invocations can reference installed tools, Rust, and Go binaries.
# Each takes precedence over the configured $PATH
PATH := ${TOOLBIN}:${RUSTBIN}:${GOBIN}:${PATH}

# Extra apt packages that we require.
EXTRA_APT_PACKAGES = \
	libbz2-dev \
	liblz4-dev \
	libprotobuf-dev \
	libsnappy-dev \
	libzstd-dev \
	protobuf-compiler

# Etcd release we pin within Flow distributions.
ETCD_VERSION = v3.4.13
ETCD_SHA256 = 2ac029e47bab752dacdb7b30032f230f49e2f457cbc32e8f555c2210bb5ff107

# Version of Rocks to build against.
ROCKSDB_VERSION = 6.11.4
# Name of built RocksDB library.
# Must match major & minor of ROCKSDB_VERSION.
LIBROCKS = librocksdb.so.6.11
# Location of RocksDB source under $WORKDIR.
ROCKSDIR = ${WORKDIR}/rocksdb-v${ROCKSDB_VERSION}

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

PACKAGE_TARGETS = \
	${PKGDIR}/bin/etcd \
	${PKGDIR}/bin/flowctl \
	${PKGDIR}/bin/gazctl \
	${PKGDIR}/bin/gazette \
	${PKGDIR}/lib/${LIBROCKS}

##########################################################################
# Configure Go build & test behaviors.

# Enable the sqlite3 JSON extension.
GO_BUILD_TAGS += json1

# Configure for building & linking against our vendored RocksDB library.
export CGO_CFLAGS      = -I${ROCKSDIR}/include
export CGO_CPPFLAGS    = -I${ROCKSDIR}/include
export CGO_LDFLAGS     = -L${ROCKSDIR} -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd
export LD_LIBRARY_PATH =   ${ROCKSDIR}

# Variable used by librocksdb-sys to discover dynamic RocksDB library.
# TODO(johnny): Ideally this would be specified as cargo configuration:
#  https://github.com/rust-lang/cargo/pull/8839
export ROCKSDB_LIB_DIR = ${ROCKSDIR}

##########################################################################
# Build rules:

# `protoc-gen-gogo` is used to compile Go protobufs.
${TOOLBIN}/protoc-gen-gogo:
	go mod download github.com/golang/protobuf
	go build -o $@ github.com/gogo/protobuf/protoc-gen-gogo

# `etcd` is used for testing, and packaged as a release artifact.
${TOOLBIN}/etcd:
	mkdir -p ${TOOLBIN} \
		&& curl -L -o /tmp/etcd.tgz \
			https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz \
		&& echo "${ETCD_SHA256} /tmp/etcd.tgz" | sha256sum -c - \
		&& tar --extract \
			--file /tmp/etcd.tgz \
			--directory /tmp/ \
		&& mv /tmp/etcd-${ETCD_VERSION}-linux-amd64/etcd /tmp/etcd-${ETCD_VERSION}-linux-amd64/etcdctl ${TOOLBIN}/ \
		&& chown ${UID}:${UID} ${TOOLBIN}/etcd ${TOOLBIN}/etcdctl \
		&& rm -r /tmp/etcd-${ETCD_VERSION}-linux-amd64/ \
		&& rm /tmp/etcd.tgz \
		&& $@ --version

# librocksdb.so fetches and builds the version of RocksDB identified by
# the rule stem (eg, 5.17.2). We require a custom rule to build RocksDB as
# it's necessary to build with run-time type information (USE_RTTI=1), which
# is not enabled in Debian packaging.
${WORKDIR}/rocksdb-v%/${LIBROCKS}:
	# Fetch RocksDB source.
	mkdir -p ${WORKDIR}/rocksdb-v$*
	curl -L -o ${WORKDIR}/tmp.tgz https://github.com/facebook/rocksdb/archive/v$*.tar.gz
	tar xzf ${WORKDIR}/tmp.tgz -C ${WORKDIR}/rocksdb-v$* --strip-components=1
	rm ${WORKDIR}/tmp.tgz
	# TODO(johnny): We should remove PORTABLE, and instead restrict CI to compatible hardware.
	@# PORTABLE=1 prevents rocks from passing `-march=native`. This is important because it will cause gcc
	@# to automatically use avx512 extensions if they're available, which would cause it to break on CPUs
	@# that don't support it.
	PORTABLE=1 USE_SSE=1 DEBUG_LEVEL=0 USE_RTTI=1 \
		LZ4=1 ZSTD=1 \
		$(MAKE) -C $(dir $@) shared_lib -j${NPROC}
	strip --strip-all $@

	# Cleanup for less disk use / faster CI caching.
	rm -rf $(dir $@)/shared-objects
	find $(dir $@) -name "*.[oda]" -exec rm -f {} \;

# Run the protobuf compiler to generate message and gRPC service implementations.
# Invoke protoc with local and third-party include paths set.
%.pb.go: %.proto ${TOOLBIN}/protoc-gen-gogo
	protoc -I . $(foreach module, $(PROTOC_INC_GO_MODULES), -I$(GO_MODULE_PATH)) \
	--gogo_out=paths=source_relative,plugins=grpc:. $*.proto

# Rule for building Go targets.
# go-install rules never correspond to actual files, and are always re-run each invocation.
go-install/%: ${RUSTBIN}/libbindings.a crates/bindings/flow_bindings.h
	MBP=go.gazette.dev/core/mainboilerplate ;\
	go install -v --tags "${GO_BUILD_TAGS}" \
		-ldflags "-X $${MBP}.Version=${VERSION} -X $${MBP}.BuildDate=${DATE}" $*

${GOBIN}/flow-ingester: go-install/github.com/estuary/flow/go/flow-ingester $(GO_PROTO_TARGETS)
${GOBIN}/sql-driver: 	go-install/github.com/estuary/flow/go/sql-driver $(GO_PROTO_TARGETS)
${GOBIN}/flow-consumer: go-install/github.com/estuary/flow/go/flow-consumer $(GO_PROTO_TARGETS) ${ROCKSDIR}/${LIBROCKS}
${GOBIN}/gazette:       go-install/go.gazette.dev/core/cmd/gazette
${GOBIN}/gazctl:        go-install/go.gazette.dev/core/cmd/gazctl
${GOBIN}/flowctl:    	go-install/github.com/estuary/flow/go/flowctl $(GO_PROTO_TARGETS) ${ROCKSDIR}/${LIBROCKS}

# The & here declares that this single invocation will produce all of the files on the left hand
# side. flow_bindings.h is generated by the bindings build.rs.
${RUSTBIN}/libbindings.a crates/bindings/flow_bindings.h &: ${ROCKSDIR}/${LIBROCKS}
	FLOW_VERSION=${VERSION} cargo build --release --locked -p bindings

${PKGDIR}:
	mkdir -p ${PKGDIR}/bin
	mkdir ${PKGDIR}/lib
${PKGDIR}/bin/etcd: ${PKGDIR} ${TOOLBIN}/etcd
	cp ${TOOLBIN}/etcd $@
${PKGDIR}/bin/flowctl:     ${PKGDIR} ${GOBIN}/flowctl
	cp ${GOBIN}/flowctl $@
${PKGDIR}/bin/gazctl: ${PKGDIR} ${GOBIN}/gazctl
	cp ${GOBIN}/gazctl $@
${PKGDIR}/bin/gazette: ${PKGDIR} ${GOBIN}/gazette
	cp ${GOBIN}/gazette $@
${PKGDIR}/lib/${LIBROCKS}:     ${PKGDIR} ${ROCKSDIR}/${LIBROCKS}
	cp ${ROCKSDIR}/${LIBROCKS} $@

##########################################################################
# Make targets used by CI:

# We use LLVM for faster linking. See .cargo/config.
.PHONY: extra-ci-setup
extra-ci-runner-setup:
	sudo apt install -y $(EXTRA_APT_PACKAGES)
	sudo ln --force --symbolic /usr/bin/ld.lld-11 /usr/bin/ld.lld

.PHONY: print-versions
print-versions:
	echo "Resolved repository version: ${VERSION}" \
		&& cargo version --verbose \
		&& rustc --version \
		&& npm --version \
		&& node --version \
		&& go version \
		&& docker --version \
		&& /usr/bin/ld.lld --version

.PHONY: install-tools
install-tools: ${TOOLBIN}/protoc-gen-gogo ${TOOLBIN}/etcd

.PHONY: rocks-build
rocks-build: ${ROCKSDIR}/${LIBROCKS}

.PHONY: rust-build
rust-build: ${ROCKSDIR}/${LIBROCKS}
	FLOW_VERSION=${VERSION} cargo build --release --locked -p bindings

.PHONY: rust-test
rust-test: ${ROCKSDIR}/${LIBROCKS}
	FLOW_VERSION=${VERSION} cargo test --release --locked

.PHONY: go-test-fast
go-test-fast: $(GO_PROTO_TARGETS) ${RUSTBIN}/libbindings.a crates/bindings/flow_bindings.h ${TOOLBIN}/etcd ${ROCKSDIR}/${LIBROCKS}
	go test -p ${NPROC} --tags "${GO_BUILD_TAGS}" ./go/...

.PHONY: go-test-ci
go-test-ci:   $(GO_PROTO_TARGETS) ${RUSTBIN}/libbindings.a crates/bindings/flow_bindings.h ${TOOLBIN}/etcd ${ROCKSDIR}/${LIBROCKS}
	GORACE="halt_on_error=1" \
	go test -p ${NPROC} --tags "${GO_BUILD_TAGS}" --race --count=15 --failfast ./go/...

.PHONY: test-pg-driver
test-pg-driver: ${RUSTBIN}/libbindings.a crates/bindings/flow_bindings.h
	go test -v --tags pgdrivertest,${GO_BUILD_TAGS} ./go/materialize/driver/postgres/test

.PHONY: catalog-test
catalog-test: ${GOBIN}/flowctl ${TOOLBIN}/etcd
	${GOBIN}/flowctl test --source ${ROOTDIR}/examples/local-sqlite.flow.yaml $(ARGS)

.PHONY: package
package: $(PACKAGE_TARGETS)

# These docker targets intentionally don't depend on any upstream targets. This is because the
# upstream targes are all PHONY as well, so there would be no way to prevent them from runnign twice if you
# invoke e.g. `make package` followed by `make docker-image`. If the `docker-image` target depended
# on the `package` target, it would not skip the package step when you invoke `docker-image`.
# For now, the github action workflow manually invokes make to perform each of these tasks.
.PHONY: docker-image
docker-image:
	docker build \
		--file ${ROOTDIR}/.devcontainer/release.Dockerfile \
		--tag docker.pkg.github.com/estuary/flow/bin:${VERSION} \
		--tag quay.io/estuary/flow:${VERSION} \
		--tag quay.io/estuary/flow:dev \
		${PKGDIR}/

.PHONY: docker-push-to-quay
docker-push-to-quay:
	docker push quay.io/estuary/flow:${VERSION}

# This is used by the GH Action workflow to push the 'dev' tag.
# It is invoked only for builds on the master branch.
.PHONY: docker-push-quay-dev
docker-push-quay-dev:
	docker push quay.io/estuary/flow:dev

# This works , but is currently disabled. See comment in .github/workflows/main.yml.
# .PHONY: docker-push-to-github
# docker-push-to-github: docker-image
# 	docker push docker.pkg.github.com/estuary/flow/bin:${VERSION}

##########################################################################
# Make targets used for development:

.PHONY: develop
develop: ${GOBIN}/flowctl ${TOOLBIN}/etcd
	${GOBIN}/flowctl develop --source ${ROOTDIR}/examples/local-sqlite.flow.yaml --log.level info $(ARGS)
