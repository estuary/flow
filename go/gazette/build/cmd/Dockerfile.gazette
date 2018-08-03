FROM gazette-build:latest AS build

FROM liveramp/gazette-base:1.1.0
COPY --from=build /go/bin/gazette /usr/local/bin
