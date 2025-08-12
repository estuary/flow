#!/bin/bash
set -e
RUSTFLAGS="-C force-frame-pointers=yes" make data-plane-test-setup

export BENCHMARK_PREFIX="scratch/$(date +'%Y%m%d-%H%M-')"
./benchmark.sh | tee "${BENCHMARK_PREFIX}benchmark-logs.txt"
