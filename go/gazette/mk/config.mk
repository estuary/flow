# Git version & date which are injected into built binaries.
VERSION = $(shell git describe --dirty)
DATE    = $(shell date +%F-%T-%Z)
# Number of available processors for parallel builds.
NPROC := $(if ${NPROC},${NPROC},$(shell nproc))

# Version of Rocks to build against.
# - This is tightly coupled github.com/tecbot/gorocksdb (update them together).
# - Also update .circleci/config.yml
ROCKSDB_VERSION = 5.17.2

# Gazette repository root.
ROOTDIR  = $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
# Location to place intermediate files and output artifacts
# during the build process. Note the go tool ignores directories
# with leading '.' or '_'.
WORKDIR  = ${ROOTDIR}/.build
# Location of RocksDB source under $WORKDIR.
ROCKSDIR = ${WORKDIR}/rocksdb-v${ROCKSDB_VERSION}

# Go binaries to package under the `gazette/broker` image.
ci-release-broker-targets = \
	${WORKDIR}/go-path/bin/gazctl \
	${WORKDIR}/go-path/bin/gazette

# Go binaries to package under the `gazette/examples` image.
ci-release-examples-targets = \
	${WORKDIR}/go-path/bin/chunker \
	${WORKDIR}/go-path/bin/counter \
	${WORKDIR}/go-path/bin/gazctl \
	${WORKDIR}/go-path/bin/summer \
	${WORKDIR}/go-path/bin/wordcountctl

# Targets of protobufs which must be compiled.
protobuf-targets = \
	./protocol/protocol.pb.go \
	./recoverylog/recorded_op.pb.go \
	./consumer/consumer.pb.go

# consumer.proto depends on protocol.proto & recorded_op.proto.
consumer/consumer.pb.go: protocol/protocol.proto recoverylog/recorded_op.proto

# Export appropriate CGO and run-time linker flags to build, link,
# and run against against our copy of Rocks.
export CGO_CFLAGS      = -I${ROCKSDIR}/include
export CGO_CPPFLAGS    = -I${ROCKSDIR}/include
export CGO_LDFLAGS     = -L${ROCKSDIR}
export LD_LIBRARY_PATH =   ${ROCKSDIR}
