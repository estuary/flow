# This Dockerfile is a facsimile of the "ubuntu-2004" GitHub action runner,
# trimmed down to those software packages which we actually use.
# https://github.com/actions/virtual-environments/blob/main/images/linux/Ubuntu2004-Readme.md
#
# Don't install anything in this Dockerfile which isn't also present in that environment!
# Instead, further packages must be installed through explicit build steps.
# This practice keeps builds within devcontainer environments (i.e. codespaces) in lock-step
# with what works in GitHub Actions CI.
FROM ubuntu:20.04

## Set a configured locale.
ARG LOCALE=en_US.UTF-8

# See the package list in the GitHub reference link above, at the very bottom,
# which lists installed apt packages.
RUN apt update -y \
    && apt upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt install --no-install-recommends -y \
    bash-completion \
    build-essential \
    ca-certificates \
    clang-12 \
    cmake \
    curl \
    docker-compose \
    docker.io \
    git \
    gnupg2 \
    iproute2 \
    jq \
    less \
    libclang-12-dev \
    libsqlite3-dev \
    libssl-dev \
    lld-12 \
    locales \
    net-tools \
    netcat \
    openssh-client  \
    pkg-config \
    postgresql-client \
    psmisc \
    sqlite3 \
    strace \
    sudo \
    tcpdump \
    unzip \
    vim-tiny \
    wget \
    zip

RUN locale-gen ${LOCALE}

# Install git-lfs.
# RUN wget https://packagecloud.io/github/git-lfs/packages/debian/bullseye/git-lfs_3.2.0_amd64.deb/download \
#  && dpkg --install download \
#  && rm download
RUN curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash \
    && apt install -y git-lfs \
    && git lfs install

# Install package sources for google-cloud-sdk repository.
# Run `gcloud auth application-default login` to enable key management with the `sops` tool.
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# Install package source for more recent Nodejs packages.
RUN echo "Add NodeSource keyring for more recent nodejs packages" \
    && export NODE_KEYRING=/usr/share/keyrings/nodesource.gpg \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | gpg --dearmor | tee "$NODE_KEYRING" >/dev/null \
    && gpg --no-default-keyring --keyring "$NODE_KEYRING" --list-keys \
    && echo "deb [signed-by=$NODE_KEYRING] https://deb.nodesource.com/node_14.x bullseye main" | tee /etc/apt/sources.list.d/nodesource.list

# Install google-cloud-sdk and nodejs.
RUN apt update -y \
    && apt install google-cloud-sdk nodejs --no-install-recommends -y \
    && apt auto-remove -y

## Install Rust. This is pasted from:
## https://github.com/rust-lang/docker-rust/blob/master/1.64.0/bullseye/Dockerfile
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.64.0

RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "${dpkgArch##*-}" in \
    amd64) rustArch='x86_64-unknown-linux-gnu'; rustupSha256='5cc9ffd1026e82e7fb2eec2121ad71f4b0f044e88bca39207b3f6b769aaa799c' ;; \
    armhf) rustArch='armv7-unknown-linux-gnueabihf'; rustupSha256='48c5ecfd1409da93164af20cf4ac2c6f00688b15eb6ba65047f654060c844d85' ;; \
    arm64) rustArch='aarch64-unknown-linux-gnu'; rustupSha256='e189948e396d47254103a49c987e7fb0e5dd8e34b200aa4481ecc4b8e41fb929' ;; \
    i386) rustArch='i686-unknown-linux-gnu'; rustupSha256='0e0be29c560ad958ba52fcf06b3ea04435cb3cd674fbe11ce7d954093b9504fd' ;; \
    *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac; \
    url="https://static.rust-lang.org/rustup/archive/1.25.1/${rustArch}/rustup-init"; \
    wget "$url"; \
    echo "${rustupSha256} *rustup-init" | sha256sum -c -; \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --profile minimal --default-toolchain $RUST_VERSION --default-host ${rustArch}; \
    rm rustup-init; \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
    rustup --version; \
    cargo --version; \
    rustc --version;

# The above copy-paste installed the "minimal" profile, but GitHub runners
# feature additional tools.
# See: https://blog.rust-lang.org/2019/10/15/Rustup-1.20.0.html#profiles
RUN rustup set profile default \
    && rustup component add clippy rustfmt rust-docs

## Install Go.
## From https://github.com/docker-library/golang/blob/master/1.19/bullseye/Dockerfile
ARG GOLANG_VERSION=1.19.1
ENV PATH=/usr/local/go/bin:$PATH

RUN set -eux; \
    arch="$(dpkg --print-architecture)"; arch="${arch##*-}"; \
    url=; \
    case "$arch" in \
    'amd64') \
    url='https://dl.google.com/go/go1.19.1.linux-amd64.tar.gz'; \
    sha256='acc512fbab4f716a8f97a8b3fbaa9ddd39606a28be6c2515ef7c6c6311acffde'; \
    ;; \
    'armel') \
    export GOARCH='arm' GOARM='5' GOOS='linux'; \
    ;; \
    'armhf') \
    url='https://dl.google.com/go/go1.19.1.linux-armv6l.tar.gz'; \
    sha256='efe93f5671621ee84ce5e262e1e21acbc72acefbaba360f21778abd083d4ad16'; \
    ;; \
    'arm64') \
    url='https://dl.google.com/go/go1.19.1.linux-arm64.tar.gz'; \
    sha256='49960821948b9c6b14041430890eccee58c76b52e2dbaafce971c3c38d43df9f'; \
    ;; \
    'i386') \
    url='https://dl.google.com/go/go1.19.1.linux-386.tar.gz'; \
    sha256='9acc57342400c5b0c2da07b5b01b50da239dd4a7fad41a1fb56af8363ef4133f'; \
    ;; \
    'mips64el') \
    export GOARCH='mips64le' GOOS='linux'; \
    ;; \
    'ppc64el') \
    url='https://dl.google.com/go/go1.19.1.linux-ppc64le.tar.gz'; \
    sha256='4137984aa353de9c5ec1bd8fb3cd00a0624b75eafa3d4ec13d2f3f48261dba2e'; \
    ;; \
    's390x') \
    url='https://dl.google.com/go/go1.19.1.linux-s390x.tar.gz'; \
    sha256='ca1005cc80a3dd726536b4c6ea5fef0318939351ff273eff420bd66a377c74eb'; \
    ;; \
    *) echo >&2 "error: unsupported architecture '$arch' (likely packaging update needed)"; exit 1 ;; \
    esac; \
    build=; \
    if [ -z "$url" ]; then \
    # https://github.com/golang/go/issues/38536#issuecomment-616897960
    build=1; \
    url='https://dl.google.com/go/go1.19.1.src.tar.gz'; \
    sha256='27871baa490f3401414ad793fba49086f6c855b1c584385ed7771e1204c7e179'; \
    echo >&2; \
    echo >&2 "warning: current architecture ($arch) does not have a compatible Go binary release; will be building from source"; \
    echo >&2; \
    fi; \
    \
    wget -O go.tgz.asc "$url.asc"; \
    wget -O go.tgz "$url" --progress=dot:giga; \
    echo "$sha256 *go.tgz" | sha256sum -c -; \
    \
    # https://github.com/golang/go/issues/14739#issuecomment-324767697
    GNUPGHOME="$(mktemp -d)"; export GNUPGHOME; \
    # https://www.google.com/linuxrepositories/
    gpg --batch --keyserver keyserver.ubuntu.com --recv-keys 'EB4C 1BFD 4F04 2F6D DDCC  EC91 7721 F63B D38B 4796'; \
    # let's also fetch the specific subkey of that key explicitly that we expect "go.tgz.asc" to be signed by, just to make sure we definitely have it
    gpg --batch --keyserver keyserver.ubuntu.com --recv-keys '2F52 8D36 D67B 69ED F998  D857 78BD 6547 3CB3 BD13'; \
    gpg --batch --verify go.tgz.asc go.tgz; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME" go.tgz.asc; \
    \
    tar -C /usr/local -xzf go.tgz; \
    rm go.tgz; \
    \
    if [ -n "$build" ]; then \
    savedAptMark="$(apt-mark showmanual)"; \
    apt-get update; \
    apt-get install -y --no-install-recommends golang-go; \
    \
    export GOCACHE='/tmp/gocache'; \
    \
    ( \
    cd /usr/local/go/src; \
    # set GOROOT_BOOTSTRAP + GOHOST* such that we can build Go successfully
    export GOROOT_BOOTSTRAP="$(go env GOROOT)" GOHOSTOS="$GOOS" GOHOSTARCH="$GOARCH"; \
    ./make.bash; \
    ); \
    \
    apt-mark auto '.*' > /dev/null; \
    apt-mark manual $savedAptMark > /dev/null; \
    apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false; \
    rm -rf /var/lib/apt/lists/*; \
    \
    # remove a few intermediate / bootstrapping files the official binary release tarballs do not contain
    rm -rf \
    /usr/local/go/pkg/*/cmd \
    /usr/local/go/pkg/bootstrap \
    /usr/local/go/pkg/obj \
    /usr/local/go/pkg/tool/*/api \
    /usr/local/go/pkg/tool/*/go_bootstrap \
    /usr/local/go/src/cmd/dist/dist \
    "$GOCACHE" \
    ; \
    fi; \
    \
    go version

# Add `flow` user with sudo access.
RUN useradd flow --create-home --shell /bin/bash \
    && echo flow ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/flow \
    && chmod 0440 /etc/sudoers.d/flow

# Adapted from: https://github.com/microsoft/vscode-dev-containers/tree/main/containers/docker-from-docker#adding-the-user-to-a-docker-group
COPY docker-debian.sh /tmp
RUN bash /tmp/docker-debian.sh true /var/run/docker-host.sock /var/run/docker.sock flow false

# VS Code overrides ENTRYPOINT and CMD when executing `docker run` by default.
# Setting the ENTRYPOINT to docker-init.sh will configure non-root access to
# the Docker socket if "overrideCommand": false is set in devcontainer.json.
# The script will also execute CMD if you need to alter startup behaviors.
ENTRYPOINT [ "/usr/local/share/docker-init.sh" ]
CMD [ "sleep", "infinity" ]
