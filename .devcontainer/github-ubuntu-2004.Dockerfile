# This Dockerfile is a facsimile of the "ubuntu-2004" GitHub action runner,
# trimmed down to those software packages which we actually use.
# https://github.com/actions/virtual-environments/blob/main/images/linux/Ubuntu2004-README.md
#
# Don't install anything in this Dockerfile which isn't also present in that environment!
# Instead, further packages must be installed through explicit build steps.
# This practice keeps builds within docker environments (i.e. codespaces) in lock-step
# with what GitHub Actions CI produces.
FROM ubuntu:20.04

## Set a configured locale.
ARG LOCALE=en_US.UTF-8

RUN apt-get update -y \
 && apt-get upgrade -y \
 && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
      # See the package list in the GitHub reference link above, at the very bottom,
      # which lists installed apt packages.
      bash-completion \
      build-essential \
      ca-certificates \
      clang-11 \
      curl \
      git \
      gnupg2 \
      iproute2 \
      jq \
      libclang-11-dev \
      libncurses5-dev \
      libreadline-dev \
      libssl-dev \
      lld-11 \
      locales \
      net-tools \
      netcat \
      nodejs \
      npm \
      openssh-client  \
      pkg-config \
      postgresql-client \
      psmisc \
      sqlite3 \
      strace \
      sudo \
      tcpdump \
      unzip \
      wget \
      zip

## Install Rust. This is pasted from:
## https://github.com/rust-lang/docker-rust/blob/master/1.51.0/bullseye/Dockerfile

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.54.0

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
ARG GOLANG_VERSION=1.16.7
ARG GOLANG_SHA256=7fe7a73f55ba3e2285da36f8b085e5c0159e9564ef5f63ee0ed6b818ade8ef04
ENV PATH=/usr/local/go/bin:$PATH

RUN curl -L -o /tmp/golang.tgz \
      https://golang.org/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz \
 && echo "${GOLANG_SHA256} /tmp/golang.tgz" | sha256sum -c - \
 && tar --extract \
      --file /tmp/golang.tgz \
      --directory /usr/local \
 && rm /tmp/golang.tgz \
 && go version

## Install Docker.
ARG DOCKER_VERSION=19.03.13
ARG DOCKER_SHA256=ddb13aff1fcdcceb710bf71a210169b9c1abfd7420eeaf42cf7975f8fae2fcc8

RUN curl -L -o /tmp/docker.tgz \
      https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz \
 && echo "${DOCKER_SHA256} /tmp/docker.tgz" | sha256sum -c - \
 && tar --extract \
      --file /tmp/docker.tgz \
      --strip-components 1 \
      --directory /usr/local/bin/ \
 && rm /tmp/docker.tgz \
 && docker --version

RUN locale-gen ${LOCALE}

RUN useradd flow --create-home --shell /usr/sbin/nologin \
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

USER flow
