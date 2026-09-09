# localhost/flow-sandbox-helper:spike
#
# Fedora 43 packages libkrun 1.19.0 and libkrunfw 5.5.0, so neither is built
# from source. 1.19.0's header differs from the v1.19.4 the design was written
# against only by the addition of `krun_add_virtiofs4`, which the shim does not
# call; see spike/STATUS.md.

FROM quay.io/fedora/fedora:43 AS build
RUN dnf install -y --setopt=install_weak_deps=False \
        cargo rust libkrun-devel libkrunfw-devel gcc glibc-static \
 && dnf clean all

WORKDIR /src
COPY shim/Cargo.toml shim/Cargo.lock ./shim/
COPY shim/src ./shim/src
RUN cargo build --release --locked --manifest-path shim/Cargo.toml

# Static: it is injected into the guest root, where no libc can be assumed.
COPY guest/vsock-echo.c ./guest/
RUN gcc -static -O2 -Wall -Wextra -Werror -o /vsock-echo guest/vsock-echo.c

FROM quay.io/fedora/fedora:43
# tcpdump and conntrack-tools are for diagnosing the later egress experiments
# from inside the helper's namespace; socat exercises the vsock unix socket.
RUN dnf install -y --setopt=install_weak_deps=False \
        libkrun libkrunfw nftables e2fsprogs iproute socat tcpdump conntrack-tools \
 && dnf clean all

COPY --from=build /src/shim/target/release/flow-sandbox-helper /usr/local/bin/flow-sandbox-helper
COPY --from=build /vsock-echo /vsock-echo

# Placeholders until WP02 and WP04 land.
COPY stubs/flow-init /flow-init
COPY stubs/flow-sandbox-egress /usr/local/bin/flow-sandbox-egress
COPY stubs/flow-sandbox-resolver /usr/local/bin/flow-sandbox-resolver

ENTRYPOINT ["/usr/local/bin/flow-sandbox-helper"]
