# 18.04 (bionic) is latest Ubuntu LTS release. We require some of its updated
# packages (notably libzstd) over Debian stretch.
#
# Note that this image will be cached in Github Actions, and the cache key is computed by hashing
# this file. This works only so long as there are no _other_ files that go into the final image.
# So if you add any ADD or COPY directives, be sure to update the cache key in the github actions 
# workflow yaml
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
      zip \
 && rm -rf /var/lib/apt/lists/*

ARG GOLANG_VERSION=1.14.2
ARG GOLANG_SHA256=6272d6e940ecb71ea5636ddb5fab3933e087c1356173c61f4a803895e947ebb3

ARG DOCKER_VERSION=19.03.8
ARG DOCKER_SHA256=7f4115dc6a3c19c917f8b9664d7b51c904def1c984e082c4600097433323cf6f

ARG ETCD_VERSION=v3.4.7
ARG ETCD_SHA256=4ad86e663b63feb4855e1f3a647e719d6d79cf6306410c52b7f280fa56f8eb6b

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
 && echo "${DOCKER_SHA256} /tmp/docker.tgz" | sha256sum -c - \
 && tar --extract \
      --file /tmp/docker.tgz \
      --strip-components 1 \
      --directory /usr/local/bin/ \
 && rm /tmp/docker.tgz \
 && docker --version

RUN curl -L -o /tmp/etcd.tgz \
     https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz \
 && echo "${ETCD_SHA256} /tmp/etcd.tgz" | sha256sum -c - \
 && tar --extract \
      --file /tmp/etcd.tgz \
      --directory /tmp/ \
 && mv /tmp/etcd-${ETCD_VERSION}-linux-amd64/etcd /tmp/etcd-${ETCD_VERSION}-linux-amd64/etcdctl /usr/local/bin \
 && chown 1000:1000 /usr/local/bin/etcd /usr/local/bin/etcdctl \
 && rm -r /tmp/etcd-${ETCD_VERSION}-linux-amd64/ \
 && rm /tmp/etcd.tgz \
 && etcd --version

WORKDIR /gazette

