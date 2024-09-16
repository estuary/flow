include mk/common-config.mk

# Project-specific configuration:

# Go binaries to package under the `gazette/broker` image.
ci-release-gazette-broker-targets = \
	${WORKDIR}/go-path/bin/gazctl \
	${WORKDIR}/go-path/bin/gazette

# Go binaries to package under the `gazette/examples` image.
ci-release-gazette-examples-targets = \
	${WORKDIR}/go-path/bin/bike-share \
	${WORKDIR}/go-path/bin/chunker \
	${WORKDIR}/go-path/bin/counter \
	${WORKDIR}/go-path/bin/gazctl \
	${WORKDIR}/go-path/bin/integration.test \
	${WORKDIR}/go-path/bin/summer \
	${WORKDIR}/go-path/bin/wordcountctl

# Targets of protobufs which must be compiled.
protobuf-targets = \
	./broker/protocol/protocol.pb.go \
	./broker/protocol/protocol.pb.gw.go \
	./consumer/protocol/protocol.pb.go \
	./consumer/protocol/protocol.pb.gw.go \
	./consumer/recoverylog/recorded_op.pb.go \
	./examples/word-count/word_count.pb.go

# consumer.proto depends on protocol.proto & recorded_op.proto.
consumer/protocol/consumer.pb.go: broker/protocol/protocol.proto consumer/recoverylog/recorded_op.proto

# Rule for integration.test, depended on by ci-release-gazette-examples.
# It's an actual file, but we set it as PHONY so it's kept fresh
# with each build of that target.
${WORKDIR}/go-path/bin/integration.test:
	go test -v -c -tags integration ./test/integration -o $@

include mk/common-build.mk
include mk/microk8s.mk
include mk/cmd-reference.mk

# Push the broker & example image to a specified private registry.
# Override the registry to use by passing a "REGISTRY=" flag to make.
REGISTRY=localhost:32000
RELEASE_TAG=latest
push-to-registry:
	docker tag gazette/broker:latest $(REGISTRY)/broker:$(RELEASE_TAG)
	docker tag gazette/examples:latest $(REGISTRY)/examples:$(RELEASE_TAG)
	docker push $(REGISTRY)/broker:$(RELEASE_TAG)
	docker push $(REGISTRY)/examples:$(RELEASE_TAG)

${WORKDIR}/gazette-x86_64-linux-gnu.zip: go-install
	cd ${WORKDIR}/go-path/bin/
	zip gazette-x86_64-linux-gnu.zip gazette gazctl

# Builds a zip file containing both the gazette and gazctl release binaries. This target may only be run
# on a linux host, since we don't currently support cross-compilation (we may want to in the
# future).
release-linux-binaries: go-install
	@# sanity check, since our make builds don't support cross-compilation at the moment
	@test "$(shell uname -io)" = "x86_64 GNU/Linux" || (echo "only x86_64 linux binaries are produced" && exit 1)
	@rm -f ${WORKDIR}/gazette-x86_64-linux-gnu.zip
	zip -j ${WORKDIR}/gazette-x86_64-linux-gnu.zip ${WORKDIR}/go-path/bin/gazette ${WORKDIR}/go-path/bin/gazctl


.PHONY: release-linux-binaries ${WORKDIR}/go-path/bin/integration.test
