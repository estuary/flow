# Version of Rocks to build against. This is tightly coupled with
# the github.com/tecbot/gorocksdb dependency.
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

# Export appropriate CGO and run-time linker flags to build, link,
# and run against against our copy of Rocks.
export CGO_CFLAGS      = -I${ROCKSDIR}/include
export CGO_CPPFLAGS    = -I${ROCKSDIR}/include
export CGO_LDFLAGS     = -L${ROCKSDIR}
export LD_LIBRARY_PATH =   ${ROCKSDIR}
