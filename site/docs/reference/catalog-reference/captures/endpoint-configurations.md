---
description: How to configure endpoints for captures
---

# Endpoint configurations

An [endpoint](../../../concepts/catalog-entities/materialization.md#endpoints) is an external system from which a Flow collection may be captured, or to which a Flow collection may be materialized. This page deals with capture endpoints specifically; to learn about materialization endpoints, go [here](../materialization/endpoints.md).

Endpoints are objects within the materialization definition. In most cases, they are powered by connectors. They use the following entity structure:

```yaml
captures:
  my/capture/name:
    # Required, type: object
    endpoint:
      # The value of an endpoint must be an object that defines only one of the following
      # top-level properties. Each of these top-level properties corresponds to a specific type of
      # external system that you want to connect to, and holds system-dependent connection
      # information. Each object here also permits any additional properties, which
      # will simply be ignored.
      specific_endpoint_type:
        specific_endpoint_configuration: value
        specific_endpoint_configuration2: value2

  my/other/name:
    endpoint:
      # As a concrete example, airbyteSource only requires `image`
      # and `config` keys as its configuration.
      airbyteSource:
        image: ghcr.io/estuary/source-kinesis:1d76c51
        config: {}
```

Flow currently supports the following configurations. Required values for each type are provided below, as well as external documentation for each system.

### Airbyte Source configuration

Any Airbyte Specification compatible source-connector is supported as a capture endpoint, regardless of the creator.&#x20;

* Estuary builds and maintains real-time connectors. The docker images can be found on GitHub [here](https://github.com/estuary/connectors), and [a current list can be found on the Connectors page](../../../concepts/connectors.md#capture-connectors).&#x20;
* A list of third-party connectors can be found on the [Airbyte docker hub](https://hub.docker.com/u/airbyte?page=1). You can use any item whose name begins with `source-`.

&#x20;Learn more about [Airbyte Source](https://docs.airbyte.io/understanding-airbyte/airbyte-specification#source).

```yaml
# An Airbyte Source endpoint
# To be nested under <endpoint> in a catalog spec.
airbyteSource:

  # The image is a path to a Docker image.
  # Required, type: string
  image: ghcr.io/estuary/source-kinesis:1f26eb0

  # Each Airbyte Source has required configuration that varies by the source itself.
  # Required, type: object
  config:

    # The Estuary Kinesis connector requires an access key to authenticate the
    # connection and a region to send its requests.
    awsAccessKeyId: your-aws-access-key-id
    awsSecretAccessKey: your-aws-secret-key
    region: us-east-2
```

### Configuration with `flowctl discover`

The preferred method for configuring captures is using the `flowctl discover` command with the link to the appropriate connector. This stubs out the required configuration in a catalog spec for you.&#x20;

