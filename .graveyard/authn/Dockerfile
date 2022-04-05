FROM golang:1 as build
WORKDIR /build

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./
RUN go build -o authn

FROM gcr.io/distroless/base-debian11
WORKDIR /

COPY --from=build /build/authn /bin/authn
COPY templates/* /templates/

EXPOSE 3000

USER nonroot:nonroot
ENTRYPOINT ["/bin/authn"]