# The ci-builder-image target builds a Docker image suitable for building
# gazette. It is the primary image used by gazette continuous integration builds.
ci-builder-image:
	docker build -t gazette-ci-builder:latest - <  mk/ci-builder.Dockerfile

# The as-ci rule recursively calls `make` _within_ a instance of the ci-builder-image,
# and bind-mounting the gazette repository into the container. This rule allows for
# idempotent gazette builds which exactly match those produced by the CI builder.
# It uses a few tricks to keep builds fast:
#  * The gazette repository checkout is bind-mounted into the container.
#  * A ${WORKDIR}-ci directory is bind-mounted under the containers relative work
#    directory. This keeps intermediate files of local vs "as-ci" builds separate
#    so they don't clobber one another.
#  * The GOPATH and GOCACHE variables are set to fall within the mounted work
#    directory. Notably, this means repeat invocations can re-use the go modules
#    cache, and the go build cache.
#  * The Host's Docker socket is bind-mounted into the container, which has a docker
#    client. This allows the ci-builder container to itself build Docker images.
as-ci: ci-builder-image
	mkdir -p ${WORKDIR} ${WORKDIR}-ci
	# Strip root prefix from WORKDIR to build its equivalent within the container. 
	ROOT_CI=/gazette ;\
	WORK_CI=$${ROOT_CI}$(subst ${ROOTDIR},,${WORKDIR}) ;\
	docker run \
		--rm \
		--tty \
		--mount src=${WORKDIR}-ci,target=$${WORK_CI},type=bind \
		--mount src=${ROOTDIR},target=$${ROOT_CI},type=bind \
		--env  GOPATH=$${WORK_CI}/go-path \
		--env GOCACHE=$${WORK_CI}/go-build-cache \
		--mount src=/var/run/docker.sock,target=/var/run/docker.sock,type=bind \
		gazette-ci-builder make ${target}

# Go build & test targets.
go-install:   ${ROCKSDIR}/librocksdb.so
	go install -v -tags rocksdb ./...
go-test-fast: ${ROCKSDIR}/librocksdb.so
	go test       -tags rocksdb ./...
go-test-ci:   ${ROCKSDIR}/librocksdb.so
	go test -race -count=15 -tags rocksdb ./...

# The ci-release-% implicit rule builds a Docker image named by the rule
# stem, using binaries enumerated by a `-target` suffix. For example,
# an invocation with `ci-release-examples` has a stem `examples`, and will
# package binaries listed in `ci-release-examples-targets` into a docker
# image named `gazette/examples:latest`.
ci-release-%: go-install ${ROCKSDIR}/librocksdb.so
	rm -rf ${WORKDIR}/ci-release
	mkdir -p ${WORKDIR}/ci-release
	ln ${$@-targets} ${ROCKSDIR}/librocksdb.so.${ROCKSDB_VERSION} \
		${WORKDIR}/ci-release
	docker build \
		-f mk/ci-release.Dockerfile \
		-t gazette/$*:latest \
		${WORKDIR}/ci-release/

# The librocksdb.so fetches and builds the version of RocksDB identified by 
# the rule stem (eg, 5.17.2). We require a custom rule to build RocksDB as
# it's necessary to build with run-time type information (USE_RTTI=1), which
# is not enabled by default in third-party packages.
${WORKDIR}/rocksdb-v%/librocksdb.so:
	# Fetch RocksDB source.
	mkdir -p ${WORKDIR}/rocksdb-v$*
	curl -L -o ${WORKDIR}/tmp.tgz https://github.com/facebook/rocksdb/archive/v$*.tar.gz
	tar xzf ${WORKDIR}/tmp.tgz -C ${WORKDIR}/rocksdb-v$* --strip-components=1
	rm ${WORKDIR}/tmp.tgz

	USE_SSE=1 DEBUG_LEVEL=0 USE_RTTI=1 \
		$(MAKE) -C $(dir $@) shared_lib tools -j$(shell nproc)
	strip --strip-all $@

# Push images to docker.io for distribution. The "release_tag" argument is required,
# as is an authenticated account to docker hub.
push-to-docker-hub:
	docker tag gazette-ci-builder:latest gazette/ci-builder:latest
	docker push gazette/ci-builder:latest
	docker tag gazette/broker:latest gazette/broker:${release_tag}
	docker tag gazette/examples:latest gazette/examples:${release_tag}
	docker push gazette/broker:${release_tag}
	docker push gazette/examples:${release_tag}

.PHONY: ci-builder-image go-install go-test-fast go-test-ci push-to-docker-hub
