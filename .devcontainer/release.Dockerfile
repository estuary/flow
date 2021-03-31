FROM debian:bullseye-slim

# Pick run-time library packages which match the development packages
# used by the ci-builder image. "curl" is included, to allow node-zone.sh
# mappings to directly query AWS/Azure/GCP metadata APIs.
# Unzip is required by the snowsql installer.
RUN apt-get update -y \
 && apt-get install --no-install-recommends -y \
      ca-certificates \
      curl \
      libjemalloc2 \
      liblz4-1 \
      libsnappy1v5 \
      libzstd1 \
      nodejs \
      npm \
      unzip \
 && rm -rf /var/lib/apt/lists/*

# Install snowsql, which is required by the snowflake driver.
# LC_ALL and LANG are required at runtime by the snowsql cli
# The DEST and LOGIN_SHELL vars are needed by the installer in order to run in non-interactive mode.
# The VERSION vars are only here to make version updates easier.
ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    SNOWSQL_DEST=/usr/local/bin \
    SNOWSQL_LOGIN_SHELL=/root/snowsql-sux \
    SNOWSQL_MINOR_VERSION=1.2 \
    SNOWSQL_FULL_VERSION=1.2.14
RUN curl -o snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash \
  https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/${SNOWSQL_MINOR_VERSION}/linux_x86_64/snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash \
  && touch ${SNOWSQL_LOGIN_SHELL} \
  && bash snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash \
  && rm -f snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash

# Copy binaries & libraries to the image.
COPY bin/* /usr/local/bin/
COPY lib/* /usr/local/lib/

RUN ldconfig

# Run as non-privileged "flow" user.
RUN useradd flow --create-home --shell /usr/sbin/nologin
USER flow
WORKDIR /home/flow/project
