FROM golang:1.18 as build
WORKDIR /build

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./
RUN go build -o /authn

FROM gcr.io/distroless/base-debian11
WORKDIR /

COPY --from=build /authn /authn
COPY templates/* /templates/

# The AUTHN_CONFIG is read for configuration of the service.
# It should be overridden with an injected secret in production contexts.
ENV AUTHN_CONFIG="{}"

EXPOSE 3000

USER nonroot:nonroot
ENTRYPOINT ["/authn", "serve", "--config=AUTHN_CONFIG"]
