# This Dockerfile is a facsimile of the "ubuntu-1804" GitHub action runner,
# trimmed down to those software packages which we actually use.
# https://github.com/actions/virtual-environments/blob/main/images/linux/Ubuntu1804-README.md
#
# Don't install anything in this Dockerfile which isn't also present in that environment!
# Instead, further packages must be installed through explicit build steps.
# This practice keeps builds within docker environments (i.e. codespaces) in lock-step
# with what GitHub Actions CI produces.
FROM ubuntu:18.04

RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get install --no-install-recommends -y \
      # See the package list in the GitHub reference link above, at the very bottom,
      # which lists installed apt packages.
      bash-completion \
      build-essential \
      ca-certificates \
      clang-9 \
      curl \
      git \
      gnupg2 \
      libclang-10-dev \
      libncurses5-dev \
      libreadline-dev \
      libsqlite3-dev \
      libsqlite3-dev \
      libssl-dev \
      locales \
      openssh-client  \
      pkg-config \
      postgresql-client \
      psmisc \
      sqlite3 \
      sudo \
      unzip \
      wget \
      zip

## Install Rust. This is pasted from:
# https://github.com/rust-lang/docker-rust/blob/3dea3da5cb709648d53b2a50ba89aef9c89bdd96/1.46.0/buster/Dockerfile
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.46.0

RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "${dpkgArch##*-}" in \
        amd64) rustArch='x86_64-unknown-linux-gnu'; rustupSha256='49c96f3f74be82f4752b8bffcf81961dea5e6e94ce1ccba94435f12e871c3bdb' ;; \
        armhf) rustArch='armv7-unknown-linux-gnueabihf'; rustupSha256='5a2be2919319e8778698fa9998002d1ec720efe7cb4f6ee4affb006b5e73f1be' ;; \
        arm64) rustArch='aarch64-unknown-linux-gnu'; rustupSha256='d93ef6f91dab8299f46eef26a56c2d97c66271cea60bf004f2f088a86a697078' ;; \
        i386) rustArch='i686-unknown-linux-gnu'; rustupSha256='e3d0ae3cfce5c6941f74fed61ca83e53d4cd2deb431b906cbd0687f246efede4' ;; \
        *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac; \
    url="https://static.rust-lang.org/rustup/archive/1.22.1/${rustArch}/rustup-init"; \
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

## Install NodeJS. This is pasted from:
# https://github.com/nodejs/docker-node/blob/de78cda8d04c7c3a5c45302dcce22d5ea258f7ef/12/buster/Dockerfile
RUN groupadd --gid 1000 node \
  && useradd --uid 1000 --gid node --shell /bin/bash --create-home node

ENV NODE_VERSION 12.18.4

RUN ARCH= && dpkgArch="$(dpkg --print-architecture)" \
  && case "${dpkgArch##*-}" in \
    amd64) ARCH='x64';; \
    ppc64el) ARCH='ppc64le';; \
    s390x) ARCH='s390x';; \
    arm64) ARCH='arm64';; \
    armhf) ARCH='armv7l';; \
    i386) ARCH='x86';; \
    *) echo "unsupported architecture"; exit 1 ;; \
  esac \
  # gpg keys listed at https://github.com/nodejs/node#release-keys
  && set -ex \
  && for key in \
    4ED778F539E3634C779C87C6D7062848A1AB005C \
    94AE36675C464D64BAFA68DD7434390BDBE9B9C5 \
    71DCFD284A79C3B38668286BC97EC7A07EDE3FC1 \
    8FCCA13FEF1D0C2E91008E09770F7A9A5AE15600 \
    C4F0DFFF4E8C1A8236409D08E73BC641CC11F4C8 \
    C82FA3AE1CBEDC6BE46B9360C43CEC45C17AB93C \
    DD8F2338BAE7501E3DD5AC78C273792F7D83545D \
    A48C2BEE680E841632CD4E44F07496B3EB3C1762 \
    108F52B48DB57BB0CC439B2997B01419BD92F80A \
    B9E2F5981AA6E0CD28160D9FF13993A75599653C \
  ; do \
    gpg --batch --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys "$key" || \
    gpg --batch --keyserver hkp://ipv4.pool.sks-keyservers.net --recv-keys "$key" || \
    gpg --batch --keyserver hkp://pgp.mit.edu:80 --recv-keys "$key" ; \
  done \
  && curl -fsSLO --compressed "https://nodejs.org/dist/v$NODE_VERSION/node-v$NODE_VERSION-linux-$ARCH.tar.xz" \
  && curl -fsSLO --compressed "https://nodejs.org/dist/v$NODE_VERSION/SHASUMS256.txt.asc" \
  && gpg --batch --decrypt --output SHASUMS256.txt SHASUMS256.txt.asc \
  && grep " node-v$NODE_VERSION-linux-$ARCH.tar.xz\$" SHASUMS256.txt | sha256sum -c - \
  && tar -xJf "node-v$NODE_VERSION-linux-$ARCH.tar.xz" -C /usr/local --strip-components=1 --no-same-owner \
  && rm "node-v$NODE_VERSION-linux-$ARCH.tar.xz" SHASUMS256.txt.asc SHASUMS256.txt \
  && ln -s /usr/local/bin/node /usr/local/bin/nodejs \
  # smoke tests
  && node --version \
  && npm --version

ENV YARN_VERSION 1.22.4

RUN set -ex \
  && for key in \
    6A010C5166006599AA17F08146C2130DFD2497F5 \
  ; do \
    gpg --batch --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys "$key" || \
    gpg --batch --keyserver hkp://ipv4.pool.sks-keyservers.net --recv-keys "$key" || \
    gpg --batch --keyserver hkp://pgp.mit.edu:80 --recv-keys "$key" ; \
  done \
  && curl -fsSLO --compressed "https://yarnpkg.com/downloads/$YARN_VERSION/yarn-v$YARN_VERSION.tar.gz" \
  && curl -fsSLO --compressed "https://yarnpkg.com/downloads/$YARN_VERSION/yarn-v$YARN_VERSION.tar.gz.asc" \
  && gpg --batch --verify yarn-v$YARN_VERSION.tar.gz.asc yarn-v$YARN_VERSION.tar.gz \
  && mkdir -p /opt \
  && tar -xzf yarn-v$YARN_VERSION.tar.gz -C /opt/ \
  && ln -s /opt/yarn-v$YARN_VERSION/bin/yarn /usr/local/bin/yarn \
  && ln -s /opt/yarn-v$YARN_VERSION/bin/yarnpkg /usr/local/bin/yarnpkg \
  && rm yarn-v$YARN_VERSION.tar.gz.asc yarn-v$YARN_VERSION.tar.gz \
  # smoke test
  && yarn --version

## Install Go.
ARG GOLANG_VERSION=1.15.2
ARG GOLANG_SHA256=b49fda1ca29a1946d6bb2a5a6982cf07ccd2aba849289508ee0f9918f6bb4552
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

## Set a configured locale.
ARG LOCALE=en_US.UTF-8
RUN locale-gen ${LOCALE}