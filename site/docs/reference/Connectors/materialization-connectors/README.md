# Materialization connectors
Estuary's available materialization connectors are listed in this section. Each connector has a unique configuration you must follow in your [catalog specification](concepts/README.md#specifications); these will be linked below the connector name.

:::info Beta
More configurations coming to the docs soon. [Contact the team](mailto:info@estuary.dev) for more information on missing connectors.
:::

Also listed are links to the most recent Docker image, which you'll need for certain configuration methods.

Estuary is actively developing new connectors, so check back regularly for the latest additions. We’re prioritizing the development of high-scale technological systems, as well as client needs.

## Available materialization connectors
* Apache Parquet
  * Configuration
  * Package — ghcr.io/estuary/materialize-s3-parquet:dev
* Elasticsearch
  * Configuration
  * Package — ghcr.io/estuary/materialize-elasticsearch:dev
* Google BigQuery
  * Configuration
  * Package — ghcr.io/estuary/materialize-bigquery:dev
* PostgreSQL
  * Configuration
  * Package — ghcr.io/estuary/materialize-postgres:dev
* Rockset
  * [Configuration](./Rockset.md)
  * Package — ghcr.io/estuary/materialize-rockset:dev
* Snowflake
  * Configuration
  * Package — ghcr.io/estuary/materialize-snowflake:dev
* Webhook
  * Configuration
  * Package — ghcr.io/estuary/materialize-webhook:dev
