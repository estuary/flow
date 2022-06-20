#!/usr/bin/env bash
cd /home/agent
wget https://github.com/estuary/flow/releases/download/dev/flow-x86-linux.tar.gz
mkdir -p /home/agent/.bin
tar -xvf flow-x86-linux.tar.gz -C /home/agent/.bin 

if [[ $1 ]]; then
  eval "$@"
fi