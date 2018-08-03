FROM gazette-build:latest AS build

COPY examples/stream-sum /go/src/github.com/LiveRamp/gazette/examples/stream-sum

# Build the `summer` consumer as a plugin, and the `chunker` binary.
RUN go build --buildmode=plugin -o /go/bin/summer.so \
      github.com/LiveRamp/gazette/examples/stream-sum/summer
RUN go install github.com/LiveRamp/gazette/examples/stream-sum/chunker

RUN go test github.com/LiveRamp/gazette/examples/stream-sum/...

FROM liveramp/gazette-base:1.1.0
COPY --from=build \
        /go/bin/chunker \
        /go/bin/run-consumer \
        /go/bin/summer.so \
    /go/bin/
