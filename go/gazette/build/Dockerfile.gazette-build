FROM liveramp/gazette-base:1.1.0 AS builder

ENV DEP_VERSION=v0.3.2

RUN curl -fsSL -o /usr/local/bin/dep \
    https://github.com/golang/dep/releases/download/${DEP_VERSION}/dep-linux-amd64 \
 && chmod +x /usr/local/bin/dep

COPY Gopkg.toml Gopkg.lock /go/src/github.com/LiveRamp/gazette/
RUN cd /go/src/github.com/LiveRamp/gazette/ && dep ensure -vendor-only

# Copy, install, and test library and binary packages.
COPY pkg /go/src/github.com/LiveRamp/gazette/pkg
COPY cmd /go/src/github.com/LiveRamp/gazette/cmd
RUN go install github.com/LiveRamp/gazette/pkg/...
RUN go install github.com/LiveRamp/gazette/cmd/...
RUN go test github.com/LiveRamp/gazette/pkg/...
RUN go test github.com/LiveRamp/gazette/cmd/...
