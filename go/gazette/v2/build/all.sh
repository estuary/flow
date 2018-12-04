#!/usr/bin/env bash
set -Eeu -o pipefail

# Root directory of repository.
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

docker pull liveramp/gazette-base:2.1.0

# Build and test Gazette. This image includes all Gazette source, vendored
# dependencies, compiled packages and binaries, and only completes after
# all tests pass.
docker build ${ROOT} \
    --file ${ROOT}/v2/build/Dockerfile \
    --target build \
    --tag liveramp/gazette-build:latest \
    --cache-from liveramp/gazette-base:2.1.0

# Create the `gazette` image, which plucks the `gazette`, `gazctl` and
# `run-consumer` onto a base runtime image.
docker build ${ROOT} \
    --file ${ROOT}/v2/build/Dockerfile \
    --target gazette \
    --tag liveramp/gazette:latest \
    --cache-from liveramp/gazette-build:latest

# Create the `gazette-examples` image, which further plucks `stream-sum` and
# `word-count` example artifacts onto the `gazette` image.
docker build ${ROOT} \
    --file ${ROOT}/v2/build/Dockerfile \
    --target examples \
    --tag liveramp/gazette-examples:latest \
    --cache-from liveramp/gazette-build:latest \
    --cache-from liveramp/gazette:latest
