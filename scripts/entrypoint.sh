#!/usr/bin/env bash
set -e
cd /home/agent
wget https://github.com/estuary/flow/releases/download/dev/flow-x86-linux.tar.gz
mkdir -p /home/agent/.bin
tar -xvf flow-x86-linux.tar.gz -C /home/agent/.bin
cp /usr/local/bin/fetch-open-graph /home/agent/.bin/fetch-open-graph

if [[ $1 ]]; then
  eval "$@"
fi
