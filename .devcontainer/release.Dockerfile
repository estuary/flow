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

RUN curl -o docker-cli.deb 'https://download.docker.com/linux/debian/dists/bullseye/pool/stable/amd64/docker-ce-cli_20.10.7~3-0~debian-bullseye_amd64.deb' && \
    dpkg -i docker-cli.deb && \
    rm docker-cli.deb

# Create a non-privileged "flow" user.
RUN useradd flow --create-home --shell /usr/sbin/nologin

# Install snowsql, which is required by the snowflake driver.
# This must be done as the flow user, since snowsql always puts its actual binaries in ~/.snowsql.
# LC_ALL and LANG are required at runtime by the snowsql cli
# The DEST and LOGIN_SHELL vars are needed by the installer in order to run in non-interactive mode.
# The VERSION vars are only here to make version updates easier.
# The PATH must be modified to include the install location, since .profile will not be loaded.
USER flow
ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    SNOWSQL_DEST=/home/flow/bin \
    SNOWSQL_LOGIN_SHELL=/home/flow/.profile \
    SNOWSQL_MINOR_VERSION=1.2 \
    SNOWSQL_FULL_VERSION=1.2.14 \
    SNOWSQL_SHA256=1afb83a22b9ccb2f8e84c2abe861da503336cb3b882fcc2e8399f86ac76bc2a9 \
    PATH="/home/flow/bin:${PATH}"
RUN curl -o /tmp/snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash \
  https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/${SNOWSQL_MINOR_VERSION}/linux_x86_64/snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash \
  && echo "${SNOWSQL_SHA256} /tmp/snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash" | sha256sum -c - \
  && touch ${SNOWSQL_LOGIN_SHELL} \
  && bash /tmp/snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash \
  && rm -f /tmp/snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash \
  # Defying all reason and expectations, _this_ is what actually installs snowsql.
  # It will print a help message as if there was a problem, but it works as long as it exits 0.
  && snowsql -v ${SNOWSQL_FULL_VERSION}

# Copy binaries & libraries to the image, owned by root.
USER root
COPY bin/* /usr/local/bin/
COPY lib/* /usr/local/lib/
RUN ldconfig

USER flow
WORKDIR /home/flow/project
