# The reference connector for first-class secrets. See ../README.md.
#
# Built and installed by hand -- there's no mise task, because this image is
# read by a person working through the README rather than by CI:
#
#   docker build --platform linux/amd64 -t registry.example/acme/source-secrets:local tests/secrets/connector
#   mise exec -- local/install-connector.sh registry.example/acme/source-secrets
#
# `--platform` matters on an arm64 host: the runtime runs every connector as
# `linux/amd64`, and would try to pull an image it can't find for that platform.
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
# task runs it, provided each is named
# `<prefix>/connectors/<this repository>/<leaf>`. Each maps to the one
# configuration location it merges at: a task using it must name that same
# location, so a task author cannot redirect the vendor's secret elsewhere.
# The reactor checks every declared name against that rule before the container
# runs -- used or not -- so a typo fails at start rather than at first use.
LABEL dev.estuary.secrets='{"test/connectors/registry.example/acme/source-secrets/oauth-client":"/credentials"}'

ENTRYPOINT ["python", "-m", "source_secrets"]
