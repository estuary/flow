# Capture connectors

Estuary's available capture connectors are listed in this section. Each connector has a unique configuration you must follow in your [catalog specification](concepts/README.md#specifications); these will be linked below the connector name.

:::info Beta
More configurations coming to the docs soon. [Contact the team](mailto:info@estuary.dev) for more information on missing connectors.
:::

Also listed are links to the most recent Docker image, which you'll need for certain configuration methods.

Estuary is actively developing new connectors, so check back regularly for the latest additions. We’re prioritizing the development of high-scale technological systems, as well as client needs.

## Available capture connectors

* Amazon Kinesis
  * [Configuration](./amazon-kinesis.md)
  * Package — ghcr.io/estuary/source-kinesis:dev
* Amazon S3
  * Configuration
  * Package — ghcr.io/estuary/source-s3:dev
* Apache Kafka
  * [Configuration](./apache-kafka.md)
  * Package — ghcr.io/estuary/source-kafka:dev
* Google Cloud Storage
  * Configuration
  * Package — ghcr.io/estuary/source-gcs:dev
* MySQL
  * [Configuration](./MySQL.md)
  * Package - ghcr.io/estuary/source-mysql:dev
* PostgreSQL
  * [Configuration](./PostgreSQL.md)
  * Package — ghcr.io/estuary/source-postgres:dev
