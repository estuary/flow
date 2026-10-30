# Git version & date which are injected into built binaries.
VERSION ?= "$(shell git describe --dirty --tags)"
DATE    = $(shell date +%F-%T-%Z)
# Repository root (the directory of this Makefile).
ROOTDIR  = $(abspath $(dir $(firstword $(MAKEFILE_LIST))))
# Location to place intermediate files and output artifacts.
# Note the go tool ignores directories with leading '.' or '_'.
WORKDIR  = ${ROOTDIR}/.build

###############################################################################
# Rules for generating protobuf sources:

${WORKDIR}/protoc-gen-gogo:
	go mod download github.com/golang/protobuf
	go build -o $@ github.com/gogo/protobuf/protoc-gen-gogo

${WORKDIR}/protoc-gen-grpc-gateway:
	go mod download github.com/grpc-ecosystem/grpc-gateway
	go build -o $@ github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway

# PROTOC_INC_MODULES is an append-able list of Go modules
# which should be included with `protoc` invocations.
PROTOC_INC_MODULES += "github.com/golang/protobuf"
PROTOC_INC_MODULES += "github.com/gogo/protobuf"

# module_path expands a $(module), like go.gazette.dev/core, to the local path
# of its repository as currently specified by go.mod.
module_path = $(shell go list -f '{{ .Dir }}' -m $(module))

# Run the protobuf compiler to generate message and gRPC service implementations.
# Invoke protoc with local and third-party include paths set. The `go list` tool
# is used to map submodules to corresponding go.mod versions and paths.
%.pb.go: %.proto ${WORKDIR}/protoc-gen-gogo
	PATH=${WORKDIR}:$${PATH} ;\
	protoc -I . $(foreach module, $(PROTOC_INC_MODULES), -I$(module_path)) \
	--gogo_out=paths=source_relative,plugins=grpc:. \
	$*.proto

%.pb.gw.go: %.proto ${WORKDIR}/protoc-gen-grpc-gateway
	PATH=${WORKDIR}:$${PATH} ;\
	protoc -I . $(foreach module, $(PROTOC_INC_MODULES), -I$(module_path)) \
	--grpc-gateway_out=logtostderr=true,paths=source_relative,generate_unbound_methods=false,grpc_api_configuration=$*_gateway.yaml:. \
	$*.proto

protobuf-targets = \
	./broker/protocol/protocol.pb.go \
	./broker/protocol/protocol.pb.gw.go \
	./consumer/protocol/protocol.pb.go \
	./consumer/protocol/protocol.pb.gw.go \
	./consumer/recoverylog/recorded_op.pb.go \
	./examples/word-count/word_count.pb.go

# consumer.proto depends on protocol.proto & recorded_op.proto.
consumer/protocol/consumer.pb.go: broker/protocol/protocol.proto consumer/recoverylog/recorded_op.proto

###############################################################################
# Rules for building and testing:

${WORKDIR}/amd64/integration.test:
	go test -v -c -tags integration ./test/integration -o $@

go-build: $(protobuf-targets) ${WORKDIR}/amd64/integration.test
	MBP=go.gazette.dev/core/mainboilerplate ;\
	go build -o ${WORKDIR}/amd64/ -v --tags libsqlite3 \
		-ldflags "-X $${MBP}.Version=${VERSION} -X $${MBP}.BuildDate=${DATE}" ./...

go-build-arm64: $(protobuf-targets)
	MBP=go.gazette.dev/core/mainboilerplate ;\
	GOOS=linux ;\
	GOARCH=arm64 \
	go build -o ${WORKDIR}/arm64/ -v --tags nozstd ./cmd/gazette ./cmd/gazctl

go-test-ci:   ${protobuf-targets}
	GORACE="halt_on_error=1" go test -race -count=10 --tags libsqlite3 --failfast ./...

go-test-fast: ${protobuf-targets}
	go test --tags libsqlite3 ./...

###############################################################################
# Rules for updating reference documentation:

cmd-reference-targets = \
	docs/_static/cmd-gazette-serve.txt \
	docs/_static/cmd-gazette-print-config.txt \
	docs/_static/cmd-gazctl.txt \
	docs/_static/cmd-gazctl-attach-uuids.txt \
	docs/_static/cmd-gazctl-journals-append.txt \
	docs/_static/cmd-gazctl-journals-apply.txt \
	docs/_static/cmd-gazctl-journals-edit.txt \
	docs/_static/cmd-gazctl-journals-fragments.txt \
	docs/_static/cmd-gazctl-journals-list.txt \
	docs/_static/cmd-gazctl-journals-prune.txt \
	docs/_static/cmd-gazctl-journals-read.txt \
	docs/_static/cmd-gazctl-journals-reset-head.txt \
	docs/_static/cmd-gazctl-journals-suspend.txt \
	docs/_static/cmd-gazctl-print-config.txt \
	docs/_static/cmd-gazctl-shards-apply.txt \
	docs/_static/cmd-gazctl-shards-edit.txt \
	docs/_static/cmd-gazctl-shards-list.txt \
	docs/_static/cmd-gazctl-shards-prune.txt

cmd-reference: ${cmd-reference-targets}


docs/_static/cmd-gazette-serve.txt: go-build
	${WORKDIR}/amd64/gazette serve --help > $@ || true
docs/_static/cmd-gazette-print-config.txt: go-build
	${WORKDIR}/amd64/gazette serve print config --help > $@ || true

docs/_static/cmd-gazctl.txt: go-build
	${WORKDIR}/amd64/gazctl --help > $@ || true
docs/_static/cmd-gazctl-attach-uuids.txt: go-build
	${WORKDIR}/amd64/gazctl attach-uuids --help > $@ || true
docs/_static/cmd-gazctl-journals-append.txt: go-build
	${WORKDIR}/amd64/gazctl journals append --help > $@ || true
docs/_static/cmd-gazctl-journals-apply.txt: go-build
	${WORKDIR}/amd64/gazctl journals apply --help > $@ || true
docs/_static/cmd-gazctl-journals-edit.txt: go-build
	${WORKDIR}/amd64/gazctl journals edit --help > $@ || true
docs/_static/cmd-gazctl-journals-fragments.txt: go-build
	${WORKDIR}/amd64/gazctl journals fragments --help > $@ || true
docs/_static/cmd-gazctl-journals-list.txt: go-build
	${WORKDIR}/amd64/gazctl journals list --help > $@ || true
docs/_static/cmd-gazctl-journals-prune.txt: go-build
	${WORKDIR}/amd64/gazctl journals prune --help > $@ || true
docs/_static/cmd-gazctl-journals-read.txt: go-build
	${WORKDIR}/amd64/gazctl journals read --help > $@ || true
docs/_static/cmd-gazctl-journals-reset-head.txt: go-build
	${WORKDIR}/amd64/gazctl journals reset-head --help > $@ || true
docs/_static/cmd-gazctl-journals-suspend.txt: go-build
	${WORKDIR}/amd64/gazctl journals suspend --help > $@ || true
docs/_static/cmd-gazctl-print-config.txt: go-build
	${WORKDIR}/amd64/gazctl print-config --help > $@ || true
docs/_static/cmd-gazctl-shards-apply.txt: go-build
	${WORKDIR}/amd64/gazctl shards apply --help > $@ || true
docs/_static/cmd-gazctl-shards-edit.txt: go-build
	${WORKDIR}/amd64/gazctl shards edit --help > $@ || true
docs/_static/cmd-gazctl-shards-list.txt: go-build
	${WORKDIR}/amd64/gazctl shards list --help > $@ || true
docs/_static/cmd-gazctl-shards-prune.txt: go-build
	${WORKDIR}/amd64/gazctl shards prune --help > $@ || true


.PHONY: \
	cmd-reference \
	go-build \
	go-build-arm64 \
	go-test-ci \
	go-test-fast