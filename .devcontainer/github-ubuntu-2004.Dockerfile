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

# Install package sources for google-cloud-sdk repository.
# Run `gcloud auth application-default login` to enable key management with the `sops` tool.
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
# Install google-cloud-sdk and nodejs.
RUN apt update -y \
    && apt install google-cloud-sdk --no-install-recommends -y \
    && apt auto-remove -y

CMD [ "sleep", "infinity" ]