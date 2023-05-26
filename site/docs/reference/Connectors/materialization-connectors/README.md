# Materialization connectors

Estuary's available materialization connectors are listed in this section. Each connector has a unique set of requirements for configuration; these are linked below the connector name.

Also listed are links to the most recent Docker images for each connector. You'll need these to write Flow specifications manually (if you're [developing locally](../../../concepts/flowctl.md)). If you're using the Flow web app, they aren't necessary.

Estuary is actively developing new connectors, so check back regularly for the latest additions. We’re prioritizing the development of high-scale technological systems, as well as client needs.

At this time, all the available materialization connectors are created by Estuary.
In the future, other open-source materialization connectors from third parties could be supported.

## Available materialization connectors

* AlloyDB
  * [Configuration](./alloydb.md)
  * Package - ghcr.io/estuary/materialize-alloydb:dev
* Amazon Redshift
  * [Configuration](./amazon-redshift.md)
  * Package - ghrc.io/estuary/materialize-redshift.dev
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
* MongoDB
  * [Configuration](./mongodb.md)
  * Package - ghcr.io/estuary/materialize-mongodb:dev
* Pinecone
  * [Configuration](./pinecone.md)
  * Package — ghcr.io/estuary/materialize-pinecone:dev
* PostgreSQL
  * [Configuration](./PostgreSQL.md)
  * Package — ghcr.io/estuary/materialize-postgres:dev
* Rockset
  * [Configuration](./Rockset.md)
  * Package — ghcr.io/estuary/materialize-rockset:dev
* Snowflake
  * [Configuration](./Snowflake.md)
  * Package — ghcr.io/estuary/materialize-snowflake:dev
* SQLite
  * [Configuration](./SQLite.md)
  * Package — ghcr.io/estuary/materialize-sqlite:dev
* TimescaleDB
  * [Configuration](./timescaledb.md)
  * Package - ghcr.io/estuary/materialize-timescaledb:dev
