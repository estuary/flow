# common-config.mk defines build configuration which is common to both
# go.gazette.dev/core, and external consumer application projects
# which re-use the Gazette build infrastructure.

# Git version & date which are injected into built binaries.
VERSION = $(shell git describe --dirty --tags)
DATE    = $(shell date +%F-%T-%Z)
# Number of available processors for parallel builds.
NPROC := $(if ${NPROC},${NPROC},$(shell nproc))

# Version of Rocks to build against.
# - This is tightly coupled github.com/tecbot/gorocksdb (update them together).
# - Also update .github/workflows/ci-workflow.yaml
ROCKSDB_VERSION = 6.7.3

# Repository root (the directory of the invoked Makefile).
ROOTDIR  = $(abspath $(dir $(firstword $(MAKEFILE_LIST))))
# Location of the gazette/core repository (one level up from common-config.mk)
COREDIR  = $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
# Location to place intermediate files and output artifacts
# during the build process. Note the go tool ignores directories
# with leading '.' or '_'.
WORKDIR  = ${ROOTDIR}/.build
# Location of RocksDB source under $WORKDIR.
ROCKSDIR = ${WORKDIR}/rocksdb-v${ROCKSDB_VERSION}

# PROTOC_INC_MODULES is an append-able list of Go modules
# which should be included with `protoc` invocations.
PROTOC_INC_MODULES += "github.com/golang/protobuf"
PROTOC_INC_MODULES += "github.com/gogo/protobuf"

# module_path expands a $(module), like go.gazette.dev/core, to the local path
# of its respository as currently specified by go.mod.
module_path = $(shell go list -f '{{ .Dir }}' -m $(module))

# Export appropriate CGO and run-time linker flags to build, link,
# and run against against our copy of Rocks.
export CGO_CFLAGS      = -I${ROCKSDIR}/include
export CGO_CPPFLAGS    = -I${ROCKSDIR}/include
export CGO_LDFLAGS     = -L${ROCKSDIR} -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -ldl
export LD_LIBRARY_PATH =   ${ROCKSDIR}
