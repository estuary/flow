FROM golang:1.9.2 AS builder

ENV ROCKSDB_VERSION=5.14.2

# Install dependencies for building & running RocksDB.
RUN apt-get update -y \
 && apt-get install --no-install-recommends -y \
      build-essential \
      curl \
      libbz2-dev \
      libgflags-dev \
      libjemalloc-dev \
      liblz4-dev \
      libsnappy-dev \
      unzip \
      zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

# Build zstd. The packaged version (libzstd-dev) is currently 1.1.2 on Debian stretch.
# We need at least 1.3
RUN curl -L -o /opt/zstd.tgz \
      https://github.com/facebook/zstd/archive/v1.3.5.tar.gz \
 && mkdir -p /opt/zstd \
 && tar xzf /opt/zstd.tgz -C /opt/zstd --strip-components=1 \
 && rm /opt/zstd.tgz \
 && cd /opt/zstd \
 && make install

# Install RocksDB and the `ldb` & `sst_dump` tools.
RUN curl -L -o /opt/rocksdb.tgz \
      https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz \
 && mkdir -p /opt/rocksdb \
 && tar xzf /opt/rocksdb.tgz -C /opt/rocksdb --strip-components=1 \
 && rm /opt/rocksdb.tgz \
 && cd /opt/rocksdb \
 && USE_SSE=1 DEBUG_LEVEL=0 USE_RTTI=1 make shared_lib tools \
 && make install \
 && cp ldb sst_dump /usr/local/bin \
 && make clean \
 && ldconfig
