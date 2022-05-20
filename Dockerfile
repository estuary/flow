FROM ubuntu:20.04 as builder
ARG LOCALE=en_US.UTF-8
RUN apt-get -y update && \
    apt-get -y upgrade && \
    DEBIAN_FRONTEND=noninteractive apt install --no-install-recommends -y \
        autoconf \
        automake \
        bash-completion \
        build-essential \
        ca-certificates \
        ca-certificates \
        clang-12 \
        clang-tools-12 \
        cmake \
        curl \
        g++ \
        git \
        gnupg2 \
        iproute2 \
        jq \
        less \
        libclang-12-dev \
        libncurses5-dev \
        libprotobuf-dev \
        libreadline-dev \
        libsqlite3-dev \
        libssl-dev \
        libxcrypt-source \
        lld-12 \
        llvm-12 \
        locales \
        lsb-release \
        musl-tools \
        net-tools \
        netcat \
        openssh-client  \
        pkg-config \
        postgresql-client \
        protobuf-compiler \
        psmisc \
        pv \
        python \
        python3-pip \
        sqlite3 \
        strace \
        tcpdump \
        unzip \
        wget \
        zip 

RUN ln -s /usr/bin/ld.lld-12 /usr/bin/ld.lld

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.60.0

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
ARG GOLANG_VERSION=1.17.5
ARG GOLANG_SHA256=bd78114b0d441b029c8fe0341f4910370925a4d270a6a590668840675b0c653e
ENV PATH=/usr/local/go/bin:$PATH

RUN curl -L -o /tmp/golang.tgz \
      https://golang.org/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz \
 && echo "${GOLANG_SHA256} /tmp/golang.tgz" | sha256sum -c - \
 && tar --extract \
      --file /tmp/golang.tgz \
      --directory /usr/local \
 && rm /tmp/golang.tgz \
 && go version

RUN locale-gen ${LOCALE}

COPY . /animated-carnival
WORKDIR /animated-carnival
RUN cd fetch-open-graph && \
    go build -o /usr/local/bin/
RUN cargo build --release
FROM ubuntu:20.04
COPY --from=builder /animated-carnival/target/release/agent /usr/local/bin
COPY --from=builder /usr/local/bin/fetch-open-graph /usr/local/bin/fetch-open-graph
COPY --from=ghcr.io/estuary/flow:dev /usr/local/bin/ /usr/local/bin/
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install --no-install-recommends \
    ca-certificates \
    curl \
    gnupg2 \
    jq \
    less \
    locales \
    lsb-release \
    net-tools \
    netcat \
    pkg-config \
    postgresql-client \
    psmisc \
    pv \
    python \
    sqlite3 \
    strace \
    tcpdump \
    unzip \
    wget \
    zip 

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get -y update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install --no-install-recommends \
    docker-ce-cli \
    google-cloud-sdk
RUN groupadd -g 1000 agent && \
    useradd -g agent -m -s /usr/sbin/nologin agent
USER agent
WORKDIR /home/agent
COPY --chown=agent:agent scripts/healthcheck.sh /home/agent/healthcheck.sh
CMD '/usr/local/bin/agent'