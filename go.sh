#!/bin/bash

set -e
PROFILE=release

export CGO_LDFLAGS="-L $(pwd)/target/${RUST_TARGET_TRIPLE}/${PROFILE} -L $(pwd)/target/${RUST_TARGET_TRIPLE}/${PROFILE}/librocksdb-exp -lbindings -lrocksdb -lsnappy -lstdc++ -ldl -lm"
export CGO_CFLAGS="-I $(pwd)/target/${RUST_TARGET_TRIPLE}/${PROFILE}/librocksdb-exp/include"
export CGO_CPPFLAGS="-I $(pwd)/target/${RUST_TARGET_TRIPLE}/${PROFILE}/librocksdb-exp/include"

# Uncomment me if you'd like to grab the resolved, final variables.
# Try placing them in your User VSCode settings, under `go.testEnvVars`.
# env | grep CGO

exec go $EXTLINKER "$@"
