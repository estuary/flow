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
	protobuf-compiler \
	sqlite3-pcre

# Apt packages that we remove.
# We'll manually install the sqlite CLI; this version is too old.
REMOVE_APT_PACKAGES = \
	sqlite3

# Etcd release we pin within Flow distributions.
ETCD_VERSION = v3.4.13
ETCD_SHA256 = 2ac029e47bab752dacdb7b30032f230f49e2f457cbc32e8f555c2210bb5ff107

# We require a more recent sqlite3 than that provided by 18.04.
# Our Go and Rust libraries provide their own recent built-in versions,
# so this is only a concern with the `sqlite3` tool itself.
SQLITE_VERSION = 3330000
SQLITE_SHA256 = b34f4c0c0eefad9a7e515c030c18702e477f4ef7d8ade6142bdab8011b487ac6

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
	./go/protocols/flow/flow.pb.go \
	./go/protocols/materialize/materialize.pb.go
# GO_MODULE_PATH expands a $(module), like "go.gazette.dev/core", to the local path
# of its respository as currently specified by go.mod. The `go list` tool
# is used to map submodules to corresponding go.mod versions and paths.
GO_MODULE_PATH = $(shell go list -f '{{ .Dir }}' -m $(module))

PACKAGE_TARGETS = \
	${PKGDIR}/bin/etcd \
	${PKGDIR}/bin/flow-consumer \
	${PKGDIR}/bin/sql-driver \
	${PKGDIR}/bin/flow-ingester \
	${PKGDIR}/bin/flowctl \
	${PKGDIR}/bin/gazctl \
	${PKGDIR}/bin/gazette \
	${PKGDIR}/bin/sqlite3 \
	${PKGDIR}/bin/websocat \
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

# `sqlite3` is used for catalog tests, and packaged as a release artifact.
# We build our own, more recent binary with enabled feature vs relying on
# what the package manager provides (which on 18.04, is pretty old).
${TOOLBIN}/sqlite3:
	mkdir -p ${TOOLBIN} \
		&& curl -L -o /tmp/sqlite.zip \
			https://www.sqlite.org/2020/sqlite-amalgamation-${SQLITE_VERSION}.zip \
		&& echo "${SQLITE_SHA256} /tmp/sqlite.zip" | sha256sum -c - \
		&& mkdir /tmp/sqlite \
		&& unzip -j -d /tmp/sqlite /tmp/sqlite.zip \
		&& gcc -Os \
			-DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_FTS4 \
			-DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_JSON1 \
			-DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_EXPLAIN_COMMENTS \
			-DHAVE_USLEEP -DHAVE_READLINE \
			/tmp/sqlite/shell.c /tmp/sqlite/sqlite3.c -lpthread -ldl -lreadline -lncurses -lm -o $@ \
		&& rm -r /tmp/sqlite/ \
		&& rm /tmp/sqlite.zip \
		&& $@ --version

# Websocat is a command-line utility for working with WebSocket APIs.
${TOOLBIN}/websocat:
	mkdir -p ${TOOLBIN} \
		&& curl -L -o /tmp/websocat https://github.com/vi/websocat/releases/download/v1.6.0/websocat_amd64-linux \
		&& echo "cec0d7d05252dcadb09a5afb8851cf9f3a8997bba44334eee5f7db70ca72aa0b /tmp/websocat" | sha256sum -c - \
		&& chmod +x /tmp/websocat \
		&& mv /tmp/websocat $@ \
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
${GOBIN}/sql-driver: go-install/github.com/estuary/flow/go/sql-driver $(GO_PROTO_TARGETS)
${GOBIN}/flow-consumer: go-install/github.com/estuary/flow/go/flow-consumer $(GO_PROTO_TARGETS) ${ROCKSDIR}/${LIBROCKS}
${GOBIN}/gazette:       go-install/go.gazette.dev/core/cmd/gazette
${GOBIN}/gazctl:        go-install/go.gazette.dev/core/cmd/gazctl

${RUSTBIN}/flowctl: ${ROCKSDIR}/${LIBROCKS}
	FLOW_VERSION=${VERSION} cargo build --release --locked -p flowctl

# The & here declares that this single invocation will produce all of the files on the left hand
# side. flow_bindings.h is generated by the bindings build.rs.
${RUSTBIN}/libbindings.a crates/bindings/flow_bindings.h &: ${ROCKSDIR}/${LIBROCKS}
	FLOW_VERSION=${VERSION} cargo build --release --locked -p bindings

${ROOTDIR}/catalog.db: ${RUSTBIN}/flowctl
	flowctl build -v --source ${ROOTDIR}/examples/flow.yaml

${PKGDIR}:
	mkdir -p ${PKGDIR}/bin
	mkdir ${PKGDIR}/lib
${PKGDIR}/bin/etcd: ${PKGDIR} ${TOOLBIN}/etcd
	cp ${TOOLBIN}/etcd $@
${PKGDIR}/bin/flow-consumer: ${PKGDIR} ${GOBIN}/flow-consumer
	cp ${GOBIN}/flow-consumer $@
${PKGDIR}/bin/sql-driver: ${PKGDIR} ${GOBIN}/sql-driver
	cp ${GOBIN}/sql-driver $@
${PKGDIR}/bin/flow-ingester: ${PKGDIR} ${GOBIN}/flow-ingester
	cp ${GOBIN}/flow-ingester $@
${PKGDIR}/bin/flowctl:     ${PKGDIR} ${RUSTBIN}/flowctl
	cp ${RUSTBIN}/flowctl $@
${PKGDIR}/bin/gazctl: ${PKGDIR} ${GOBIN}/gazctl
	cp ${GOBIN}/gazctl $@
${PKGDIR}/bin/gazette: ${PKGDIR} ${GOBIN}/gazette
	cp ${GOBIN}/gazette $@
${PKGDIR}/lib/${LIBROCKS}:     ${PKGDIR} ${ROCKSDIR}/${LIBROCKS}
	cp ${ROCKSDIR}/${LIBROCKS} $@
${PKGDIR}/bin/sqlite3: ${PKGDIR} ${TOOLBIN}/sqlite3
	cp ${TOOLBIN}/sqlite3 $@
${PKGDIR}/bin/websocat: ${PKGDIR} ${TOOLBIN}/websocat
	cp ${TOOLBIN}/websocat $@

##########################################################################
# Make targets used by CI:

.PHONY: extra-ci-setup
extra-ci-runner-setup:
	sudo apt install -y $(EXTRA_APT_PACKAGES)
	sudo apt remove -y $(REMOVE_APT_PACKAGES)
	sudo ln --force --symbolic /usr/bin/ld.lld-9 /usr/bin/ld.lld

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
install-tools: ${TOOLBIN}/protoc-gen-gogo ${TOOLBIN}/etcd ${TOOLBIN}/sqlite3

.PHONY: sql-test
sql-test: ${TOOLBIN}/sqlite3
	${ROOTDIR}/crates/catalog/src/test_catalog.sh

.PHONY: rocks-build
rocks-build: ${ROCKSDIR}/${LIBROCKS}

.PHONY: rust-build
rust-build: ${ROCKSDIR}/${LIBROCKS}
	FLOW_VERSION=${VERSION} cargo build --release --locked -p flowctl
	FLOW_VERSION=${VERSION} cargo build --release --locked -p bindings

.PHONY: rust-test
rust-test: ${TOOLBIN}/sqlite3 ${ROCKSDIR}/${LIBROCKS}
	FLOW_VERSION=${VERSION} cargo test --release --locked

.PHONY: build-test-catalog
build-test-catalog: ${ROOTDIR}/catalog.db

.PHONY: go-test-fast
go-test-fast: $(GO_PROTO_TARGETS) ${RUSTBIN}/libbindings.a crates/bindings/flow_bindings.h ${TOOLBIN}/etcd ${ROCKSDIR}/${LIBROCKS} ${ROOTDIR}/catalog.db
	go test -p ${NPROC} --tags "${GO_BUILD_TAGS}" ./...

.PHONY: go-test-ci
go-test-ci:   $(GO_PROTO_TARGETS) ${RUSTBIN}/libbindings.a crates/bindings/flow_bindings.h ${TOOLBIN}/etcd ${ROCKSDIR}/${LIBROCKS} ${ROOTDIR}/catalog.db
	GORACE="halt_on_error=1" \
	go test -p ${NPROC} --tags "${GO_BUILD_TAGS}" --race --count=15 --failfast ./...

.PHONY: catalog-test
catalog-test: ${RUSTBIN}/flowctl ${GOBIN}/flow-ingester ${GOBIN}/flow-consumer ${GOBIN}/gazette ${TOOLBIN}/etcd
	RUST_BACKTRACE=full flowctl -v test --source ${ROOTDIR}/examples/flow.yaml

.PHONY: package
package: $(PACKAGE_TARGETS)

.PHONY: docker-image
docker-image: package
	docker build \
		--file ${ROOTDIR}/.devcontainer/release.Dockerfile \
		--tag docker.pkg.github.com/estuary/flow/bin:${VERSION} \
		--tag quay.io/estuary/flow:${VERSION} \
		${PKGDIR}/

.PHONY: docker-push-to-quay
docker-push-to-quay: docker-image
	docker push quay.io/estuary/flow:${VERSION}

# This works , but is currently disabled. See comment in .github/workflows/main.yml.
# .PHONY: docker-push-to-github
# docker-push-to-github: docker-image
# 	docker push docker.pkg.github.com/estuary/flow/bin:${VERSION}

##########################################################################
# Make targets used for development:

.PHONY: catalog-test
develop: ${GOBIN}/flow-ingester ${GOBIN}/flow-consumer ${GOBIN}/gazette ${TOOLBIN}/etcd
	RUST_BACKTRACE=full flowctl -v develop --source ${ROOTDIR}/examples/flow.yaml
