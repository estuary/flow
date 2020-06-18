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
	./consumer/protocol/protocol.pb.go \
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
# Override the registry to use by passing a "registry=" flag to make.
registry=localhost:32000
push-to-registry:
	docker tag gazette/broker:latest   $(registry)/broker:latest
	docker tag gazette/examples:latest $(registry)/examples:latest
	docker push $(registry)/broker:latest
	docker push $(registry)/examples:latest

# Push gazette/ci-builder to docker.io for distribution (used by CircleCI).
push-ci-builder-image:
	docker push gazette/ci-builder:latest

# Push images to docker.io for distribution. "release_tag" is a required make
# flag, and docker must already be authenticated to docker hub.
push-release-images:
	docker tag gazette/broker:latest gazette/broker:${release_tag}
	docker tag gazette/examples:latest gazette/examples:${release_tag}
	docker push gazette/broker:${release_tag}
	docker push gazette/examples:${release_tag}

.PHONY: push-ci-builder-image push-release-images ${WORKDIR}/go-path/bin/integration.test
