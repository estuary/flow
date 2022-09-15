# Materialization connectors

Estuary's available materialization connectors are listed in this section. Each connector has a unique configuration you must follow as you create your Flow catalog.

Also listed are links to the most recent Docker image, which you'll need for certain configuration methods.

Estuary is actively developing new connectors, so check back regularly for the latest additions. We’re prioritizing the development of high-scale technological systems, as well as client needs.

## Available materialization connectors
* Apache Parquet in S3
  * [Configuration](./Parquet.md)
  * Package — ghcr.io/estuary/materialize-s3-parquet:dev
* Elasticsearch
  * [Configuration](./Elasticsearch.md)
  * Package — ghcr.io/estuary/materialize-elasticsearch:dev
* Firebolt
  * [Configuration](./Firebolt.md)
  * Package - ghcr.io/estuary/materialize-firebolt:dev
* Google BigQuery
  * [Configuration](./BigQuery.md)
  * Package — ghcr.io/estuary/materialize-bigquery:dev
* Google Cloud Pub/Sub
  * [Configuration](./google-pubsub.md)
  * Package - ghcr.io/estuary/materialize-google-pubsub:dev
* PostgreSQL
  * [Configuration](./PostgreSQL.md)
  * Package — ghcr.io/estuary/materialize-postgres:dev
* Rockset
  * [Configuration](./Rockset.md)
  * Package — ghcr.io/estuary/materialize-rockset:dev
* Snowflake
  * [Configuration](./Snowflake.md)
  * Package — ghcr.io/estuary/materialize-snowflake:dev
