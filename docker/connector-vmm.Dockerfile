# libkrun is built from source, unpatched, rather than taken from Fedora's
# 1.19.0 package: 1.19.3 fixed virtiofs attribute caching, and the library that
# runs must be the source that was reviewed. libkrunfw, the guest kernel, stays
# Fedora's. These versions also label the image, so they are set once here.
ARG LIBKRUN_VERSION=1.19.4
ARG LIBKRUN_COMMIT=728df8125077d0db44265f6e997c72b81b65c015
ARG LIBKRUNFW_VERSION=5.5.0

FROM quay.io/fedora/fedora:43 AS libkrun
ARG LIBKRUN_VERSION
ARG LIBKRUN_COMMIT
ARG LIBKRUNFW_VERSION

# clang-devel is bindgen's libclang; glibc-static links libkrun's own static
# guest init. Fedora keeps only the newest build in its updates repository, so
# this install fails once libkrunfw moves past the pin, which is deliberate.
RUN dnf install -y --setopt=install_weak_deps=False \
        git make cargo rust clang-devel glibc-static \
        libkrunfw-devel-${LIBKRUNFW_VERSION} \
 && dnf clean all

# A tag can move, and the Makefile's cargo build is not --locked: hold both the
# commit and the lockfile to what was reviewed. BLK and NET are the Makefile's
# gates for the virtio-blk and virtio-net devices the VMM configures.
RUN git clone --depth 1 --branch v${LIBKRUN_VERSION} https://github.com/containers/libkrun /src/libkrun \
 && test "$(git -C /src/libkrun rev-parse HEAD)" = "${LIBKRUN_COMMIT}" \
 && cd /src/libkrun \
 && cargo fetch --locked \
 && make BLK=1 NET=1 -j"$(nproc)" \
 && make BLK=1 NET=1 PREFIX=/usr DESTDIR=/staging install

FROM quay.io/fedora/fedora:43
ARG LIBKRUN_VERSION
ARG LIBKRUNFW_VERSION

RUN dnf install -y --setopt=install_weak_deps=False \
        libkrunfw-${LIBKRUNFW_VERSION} \
        nftables \
        e2fsprogs \
        iproute \
 && dnf clean all

# Only the library itself, and only the soname link that dlopen("libkrun.so.1")
# resolves: COPY follows symlinks, so copying the install's links would land
# more copies of the library.
COPY --from=libkrun /staging/usr/lib64/libkrun.so.${LIBKRUN_VERSION} /usr/lib64/
RUN ln -s libkrun.so.${LIBKRUN_VERSION} /usr/lib64/libkrun.so.1 \
 && ldconfig

ARG TARGETARCH
COPY ${TARGETARCH}/flow-connector-vmm /usr/local/bin/
# launch::GUEST_INIT, which the VMM maps into the guest root.
COPY ${TARGETARCH}/flow-guest-init /flow-guest-init

LABEL dev.estuary.libkrun=${LIBKRUN_VERSION}
LABEL dev.estuary.libkrunfw=${LIBKRUNFW_VERSION}

ENTRYPOINT ["/usr/local/bin/flow-connector-vmm"]
