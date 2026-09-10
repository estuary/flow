# localhost/flow-sandbox-helper:spike
#
# libkrun is built from source at v1.19.4 rather than taken from Fedora's
# 1.19.0 package: 1.19.3 fixed virtiofs attribute caching, which sits on
# experiment 5's measurement path, and WP11 audits v1.19.4's source, so the
# binary that runs must be the one that was read. libkrunfw stays Fedora's.

FROM quay.io/fedora/fedora:43 AS libkrun
# clang-devel is bindgen's libclang; glibc-static links libkrun's own static
# guest-init blob. BLK and NET are the Makefile's gates for the virtio-blk and
# virtio-net devices the shim configures - Fedora's package builds both on.
RUN dnf install -y --setopt=install_weak_deps=False \
        git make cargo rust libkrunfw-devel clang-devel glibc-static \
 && dnf clean all
RUN git clone --depth 1 --branch v1.19.4 https://github.com/containers/libkrun /src/libkrun

# WP04 deviation, awaiting the master thread: without this, overlayfs cannot
# copy anything up from a virtiofs lower, so flow-init has no writable root.
#
# Every FUSE inode carries S_NOATIME, which makes overlayfs copy the lower's
# fileattr flags on copy-up (FS_IOC_GETFLAGS). libkrun answers an unrecognized
# ioctl with EOPNOTSUPP; overlayfs tolerates only ENOTTY and EINVAL there
# ("failed to retrieve lower fileattr (/etc, err=-95)") and fails the copy-up.
# ENOTTY is also what a filesystem without inode flags - tmpfs - returns, so
# this looks upstreamable. The grep makes a moved line fail the build rather
# than silently leaving the helper broken.
RUN cd /src/libkrun \
 && grep -q "_ => Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP))," \
        src/devices/src/virtio/fs/linux/passthrough.rs \
 && sed -i "s/_ => Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP)),/_ => Err(io::Error::from_raw_os_error(libc::ENOTTY)),/" \
        src/devices/src/virtio/fs/linux/passthrough.rs

RUN make -C /src/libkrun BLK=1 NET=1 -j"$(nproc)" \
 && make -C /src/libkrun BLK=1 NET=1 PREFIX=/usr DESTDIR=/staging install

FROM quay.io/fedora/fedora:43 AS build
# The toolchain comes from rustup rather than dnf because Fedora packages no
# musl std, and flow-init is injected into a guest root where no libc, loader
# or shell can be assumed. Pinned: the report names the commit it measured.
RUN dnf install -y --setopt=install_weak_deps=False \
        rustup gcc glibc-static binutils libkrunfw-devel \
 && dnf clean all
RUN rustup-init -y --no-modify-path --profile minimal \
        --default-toolchain 1.98.1 --target x86_64-unknown-linux-musl
ENV PATH=/root/.cargo/bin:$PATH

# The shim links against the libkrun built above, not Fedora's.
COPY --from=libkrun /staging/ /

WORKDIR /src
COPY shim/Cargo.toml shim/Cargo.lock ./shim/
COPY shim/src ./shim/src
RUN cargo build --release --locked --manifest-path shim/Cargo.toml

# flow-init arrives as an additional build context, `spike/flow-init`: the
# helper image's own context is `spike/helper`. See spike/tasks/helper-build.sh.
COPY --from=flow-init Cargo.toml Cargo.lock ./flow-init/
COPY --from=flow-init src ./flow-init/src
RUN cargo build --release --locked --manifest-path flow-init/Cargo.toml \
        --target x86_64-unknown-linux-musl \
 && cp flow-init/target/x86_64-unknown-linux-musl/release/flow-init /flow-init \
 && test -z "$(readelf -d /flow-init | grep NEEDED)"

# Static: it is injected into the guest root, where no libc can be assumed.
COPY guest/vsock-echo.c ./guest/
RUN gcc -static -O2 -Wall -Wextra -Werror -o /vsock-echo guest/vsock-echo.c

FROM quay.io/fedora/fedora:43
# tcpdump and conntrack-tools are for diagnosing the later egress experiments
# from inside the helper's namespace; socat exercises the vsock unix socket.
# No `libkrun` package: the library comes from the source build.
RUN dnf install -y --setopt=install_weak_deps=False \
        libkrunfw nftables e2fsprogs iproute socat tcpdump conntrack-tools \
 && dnf clean all
# Only the real library is copied, and its two symlinks are remade here: COPY
# follows symlinks, so a glob would land three copies of a 10 MiB file.
COPY --from=libkrun /staging/usr/lib64/libkrun.so.1.* /usr/lib64/
RUN cd /usr/lib64 \
 && ln -sf libkrun.so.1.* libkrun.so.1 \
 && ln -sf libkrun.so.1 libkrun.so \
 && ldconfig

COPY --from=build /src/shim/target/release/flow-sandbox-helper /usr/local/bin/flow-sandbox-helper
COPY --from=build /flow-init /flow-init
COPY --from=build /vsock-echo /vsock-echo

# Placeholders until WP02 lands.
COPY stubs/flow-sandbox-egress /usr/local/bin/flow-sandbox-egress
COPY stubs/flow-sandbox-resolver /usr/local/bin/flow-sandbox-resolver

ENTRYPOINT ["/usr/local/bin/flow-sandbox-helper"]
