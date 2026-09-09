# The reference connector for first-class secrets. See ../README.md.
#
# Built and installed by hand -- there's no mise task, because this image is
# read by a person working through the README rather than by CI:
#
#   docker build -t registry.example/acme/source-secrets:local tests/secrets/connector
#   local/install-connector.sh registry.example/acme/source-secrets
#
# `registry.example` is a fictitious registry on purpose: nothing should ever
# try to pull this, and the `:local` tag is what tells the agent not to.
FROM python:3.12-slim

# Pinned: a connector image is a reproducible artifact, and this one has
# exactly one dependency.
RUN pip install --no-cache-dir pydantic==2.9.2

WORKDIR /connector
COPY source_secrets ./source_secrets

LABEL FLOW_RUNTIME_PROTOCOL=capture

# This connector speaks newline-delimited JSON. Without this label
# `flow-connector-init` frames requests as protobuf, and the connector waits
# forever for a line that never arrives.
LABEL FLOW_RUNTIME_CODEC=json

# The image rule: this image may be handed the secrets it names here, whatever
# task runs it, provided each is named `<prefix>/<this repository>/<leaf>`.
# The reactor checks every declared name against that rule before the container
# runs -- used or not -- so a typo fails at start rather than at first use.
LABEL dev.estuary.secrets=test/connectors/registry.example/acme/source-secrets/oauth-client

ENTRYPOINT ["python", "-m", "source_secrets"]
