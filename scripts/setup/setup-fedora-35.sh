#!/bin/bash

# This script sets up all of the dependencies required to build Flow, as well as some useful dev
# tools. It can be run on a fresh Fedora 35 installation as a single step setup. It does not accept
# any arguments. This was adapted from a script in 
# [Phil's dotfiles](https://github.com/psFried/dotfiles/blob/69259305869dca4aa41643d5ff0e656ec3bd29cf/setup/setup-fedora-35.sh)
# so some quirks remain from that.

set -ex

# libX11-devel is a dependency of nu shell
# perl-core required in order to build openssl
# snappy-devel lz4-devel bzip2-devel are all required for building Gazette
sudo dnf install -y \
    alacritty \
    autoconf \
    automake \
    bzip2-devel \
    clang \
    clang-tools-extra \
    curl \
    cmake \
    dnf-plugins-core \
    gcc \
    g++ \
    git \
    git-lfs \
    jq \
    libX11-devel \
    libxcb-devel \
    libxkbcommon-devel \
    libxcrypt-compat \
    lld \
    llvm \
    lz4-devel \
    musl-gcc \
    neovim \
    openssl-devel \
    perl-core \
    protobuf-compiler \
    protobuf-devel \
    pv \
    snappy-devel \
    sqlite-devel

sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
sudo dnf install -y docker-ce docker-ce-cli containerd.io

# Do we need to setup the docker group?
if ! groups | grep docker >/dev/null; then
    echo "adding docker group"
    getent group docker >/dev/null || sudo groupadd docker
    sudo usermod -a -G docker ${USER}
    newgrp
fi

sudo dnf groupinstall -y "Development Tools"

# node installs as a module
sudo dnf module install nodejs:16/development

if [[ -z "$(command -v go)" ]]; then
	echo "Installing Golang"
    # Any reasonably recent Go version should be fine.
	GOLANG_VERSION=1.17.5
	GOLANG_SHA256=bd78114b0d441b029c8fe0341f4910370925a4d270a6a590668840675b0c653e
	echo 'export PATH=/usr/local/go/bin:$PATH' >> ~/.bashrc

	curl -L -o /tmp/golang.tgz https://golang.org/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz
	echo "${GOLANG_SHA256} /tmp/golang.tgz" | sha256sum -c - \
	 && sudo tar --extract \
	      --file /tmp/golang.tgz \
	      --directory /usr/local \
	 && rm /tmp/golang.tgz \
	 && /usr/local/go/bin/go version

fi

if [[ -z "$(command -v rustup)" ]]; then
	echo "Installing Rustup"

	# Install rust. This just gives the latest stable version, which should always be what you want.
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- \
		-c rust-docs \
		-c rustfmt \
		-c rust-analyzer \
		-c rust-std \
		-c clippy \
		-c rust-stc \
		-c rust-analysis \
		-c llvm-tools-preview \
		-t x86_64-unknown-linux-gnu \
		-t x86_64-unknown-linux-musl \
		--default-toolchain stable
fi

set +x

# source bashrc to get the new PATH, which should now include cargo
source ~/.bashrc || echo "sourcing bashrc failed"

set -x

# Sanity check to ensure that rust and cargo installed correctly
rustc --version
cargo --version

# Just some nice tools that I use.
cargo install --locked ripgrep skim bat jless git-delta lsd fd-find starship

if [[ -z "$(command -v gcloud)" ]]; then
	echo "Installing gcloud"

sudo tee -a /etc/yum.repos.d/google-cloud-sdk.repo << EOM
[google-cloud-sdk]
name=Google Cloud SDK
baseurl=https://packages.cloud.google.com/yum/repos/cloud-sdk-el8-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=0
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg
       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOM

	sudo dnf install -y google-cloud-sdk
fi

if [[ -z "$(command -v kubectl)" ]]; then
	echo "Installing kubectl"
sudo tee -a /etc/yum.repos.d/kubernetes.repo << EOF
[kubernetes]
name=Kubernetes
baseurl=https://packages.cloud.google.com/yum/repos/kubernetes-el7-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOF

sudo dnf install -y kubectl
fi

echo -e "\nSetup complete"
