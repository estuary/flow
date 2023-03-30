#!/bin/bash

PROFILE="${PROFILE:-release}"

export CGO_LDFLAGS="-L $(pwd)/target/${CARGO_BUILD_TARGET}/${PROFILE} -L $(pwd)/target/${CARGO_BUILD_TARGET}/${PROFILE}/librocksdb-exp -lbindings -lrocksdb -lsnappy -lstdc++ -ldl -lm"
if [ "$(uname)" == "Darwin" ]; then
  export CGO_CFLAGS="-I $(pwd)/target/${CARGO_BUILD_TARGET}/${PROFILE}/librocksdb-exp/include -I $(brew --prefix)/include -I $(brew --prefix)/opt/sqlite3/include"
else
  export CGO_CFLAGS="-I $(pwd)/target/${CARGO_BUILD_TARGET}/${PROFILE}/librocksdb-exp/include"
fi;
export CGO_CPPFLAGS="-I $(pwd)/target/${CARGO_BUILD_TARGET}/${PROFILE}/librocksdb-exp/include"

# Uncomment me if you'd like to grab the resolved, final variables.
# Try placing them in your User VSCode settings, under `go.testEnvVars`.
# env | grep CGO

exec go "$@"

