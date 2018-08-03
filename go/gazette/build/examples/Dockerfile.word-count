FROM gazette-build:latest AS build

COPY examples/word-count /go/src/github.com/LiveRamp/gazette/examples/word-count

# Build each of the word-count consumers as plugins.
RUN go build --buildmode=plugin -o /go/bin/shuffler.so \
      github.com/LiveRamp/gazette/examples/word-count/shuffler
RUN go build --buildmode=plugin -o /go/bin/counter.so \
      github.com/LiveRamp/gazette/examples/word-count/counter

FROM liveramp/gazette-base:1.1.0
COPY --from=build \
        /go/bin/counter.so \
        /go/bin/run-consumer \
        /go/bin/shuffler.so \
    /go/bin/
