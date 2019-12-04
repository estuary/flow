# 18.04 (bionic) is latest Ubuntu LTS release. We require some of its updated
# packages (notably libzstd) over Debian stretch.
FROM ubuntu:18.04

RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get install --no-install-recommends -y \
      build-essential \
      ca-certificates \
      curl \
      git \
      libbz2-dev \
      libjemalloc-dev \
      liblz4-dev \
      libprotobuf-dev \
      libsnappy-dev \
      libsqlite3-dev \
      libzstd-dev \
      protobuf-compiler \
      zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

ARG GOLANG_VERSION=1.13.4
ARG GOLANG_SHA256=692d17071736f74be04a72a06dab9cac1cd759377bd85316e52b2227604c004c

ARG DOCKER_VERSION=19.03.5
ARG DOCKER_SHA256=50cdf38749642ec43d6ac50f4a3f1f7f6ac688e8d8b4e1c5b7be06e1a82f06e9

ENV PATH=/usr/local/go/bin:$PATH

RUN curl -L -o /tmp/golang.tgz \
      https://golang.org/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz \
 && echo "${GOLANG_SHA256} /tmp/golang.tgz" | sha256sum -c - \
 && tar --extract \
      --file /tmp/golang.tgz \
      --directory /usr/local \
 && rm /tmp/golang.tgz \
 && go version

RUN curl -L -o /tmp/docker.tgz \
      https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz \
 && sha256sum /tmp/docker.tgz \
 && echo "${DOCKER_SHA256} /tmp/docker.tgz" | sha256sum -c - \
 && tar --extract \
      --file /tmp/docker.tgz \
      --strip-components 1 \
      --directory /usr/local/bin/ \
 && rm /tmp/docker.tgz \
 && docker --version

WORKDIR /gazette
