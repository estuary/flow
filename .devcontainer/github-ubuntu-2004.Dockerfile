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
      musl-tools \
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
RUN wget https://packagecloud.io/github/git-lfs/packages/debian/bullseye/git-lfs_3.2.0_amd64.deb/download \
 && dpkg --install download \
 && rm download

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
## https://github.com/rust-lang/docker-rust/blob/master/1.57.0/bullseye/Dockerfile
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.61.0

RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "${dpkgArch##*-}" in \
        amd64) rustArch='x86_64-unknown-linux-gnu'; rustupSha256='3dc5ef50861ee18657f9db2eeb7392f9c2a6c95c90ab41e45ab4ca71476b4338' ;; \
        armhf) rustArch='armv7-unknown-linux-gnueabihf'; rustupSha256='67777ac3bc17277102f2ed73fd5f14c51f4ca5963adadf7f174adf4ebc38747b' ;; \
        arm64) rustArch='aarch64-unknown-linux-gnu'; rustupSha256='32a1532f7cef072a667bac53f1a5542c99666c4071af0c9549795bbdb2069ec1' ;; \
        i386) rustArch='i686-unknown-linux-gnu'; rustupSha256='e50d1deb99048bc5782a0200aa33e4eea70747d49dffdc9d06812fd22a372515' ;; \
        *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac; \
    url="https://static.rust-lang.org/rustup/archive/1.24.3/${rustArch}/rustup-init"; \
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
## See releases and SHAs at: https://go.dev/dl/
ARG GOLANG_VERSION=1.18.3
ARG GOLANG_SHA256=956f8507b302ab0bb747613695cdae10af99bbd39a90cae522b7c0302cc27245
ENV PATH=/usr/local/go/bin:$PATH

RUN curl -L -o /tmp/golang.tgz \
      https://golang.org/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz \
 && echo "${GOLANG_SHA256} /tmp/golang.tgz" | sha256sum -c - \
 && tar --extract \
      --file /tmp/golang.tgz \
      --directory /usr/local \
 && rm /tmp/golang.tgz \
 && go version

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