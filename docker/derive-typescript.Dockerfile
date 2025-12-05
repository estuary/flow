FROM denoland/deno:distroless-2.5.4

ARG TARGETARCH
COPY ${TARGETARCH}/derive-typescript /

USER nobody
ENV DENO_DIR=/tmp/deno-dir
ENV DENO_NO_UPDATE_CHECK=1
ENV NO_COLOR=1

ENTRYPOINT ["/derive-typescript"]
LABEL FLOW_RUNTIME_CODEC=json
LABEL FLOW_RUNTIME_PROTOCOL=derive