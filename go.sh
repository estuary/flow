#!/bin/bash

PROFILE="${PROFILE:-release}"

export CGO_LDFLAGS="-L $(pwd)/target/${CARGO_BUILD_TARGET}/${PROFILE} -L $(pwd)/target/${CARGO_BUILD_TARGET}/${PROFILE}/librocksdb-exp -lbindings -lrocksdb -lsnappy -lstdc++ -lssl -lcrypto -ldl -lm"
if [ "$(uname)" == "Darwin" ]; then
  export CGO_CFLAGS="-I $(pwd)/target/${CARGO_BUILD_TARGET}/${PROFILE}/librocksdb-exp/include -I $(brew --prefix)/include -I $(brew --prefix)/opt/sqlite3/include"
  export CC="$(brew --prefix)/opt/llvm/bin/clang"
  export CXX="$(brew --prefix)/opt/llvm/bin/clang"
  export CGO_LDFLAGS="${CGO_LDFLAGS} -framework SystemConfiguration"
else
  export CGO_CFLAGS="-I $(pwd)/target/${CARGO_BUILD_TARGET}/${PROFILE}/librocksdb-exp/include"
fi;
export CGO_CPPFLAGS="-I $(pwd)/target/${CARGO_BUILD_TARGET}/${PROFILE}/librocksdb-exp/include"

# RocksDB requires std::string_view, which is only available starting in C++17.
# We currently build on Ubuntu 20.04, where C++17 isn't the default.
export CGO_CXXFLAGS="-std=c++17"

# Uncomment me if you'd like to grab the resolved, final variables.
# Try placing them in your User VSCode settings, under `go.testEnvVars`.
# env | grep CGO

exec go "$@"

