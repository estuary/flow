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
* Amazon DynamoDB
  * [Configuration](./amazon-dynamodb.md)
  * Package - ghcr.io/estuary/materialize-dynamodb:dev
* Amazon Redshift
  * [Configuration](./amazon-redshift.md)
  * Package - ghcr.io/estuary/materialize-redshift:dev
* Amazon SQL Server
  * [Configuration](./SQLServer/amazon-rds-sqlserver.md)
  * Package - ghcr.io/estuary/materialize-amazon-rds-sqlserver:dev
* Apache Iceberg Tables in S3
  * [Configuration](./amazon-s3-iceberg.md)
  * Package — ghcr.io/estuary/materialize-s3-iceberg:dev
* Apache Kafka
  * [Configuration](./apache-kafka.md)
  * Package — ghcr.io/estuary/apache-kafka:dev
* Apache Parquet Files in GCS
  * [Configuration](./google-gcs-parquet.md)
  * Package — ghcr.io/estuary/materialize-gcs-parquet:dev
* Apache Parquet Files in S3
  * [Configuration](./amazon-s3-parquet.md)
  * Package — ghcr.io/estuary/materialize-s3-parquet:dev
* Azure SQL Server
  * [Configuration](./SQLServer/)
* CSV Files in GCS
  * [Configuration](./google-gcs-csv.md)
  * Package — ghcr.io/estuary/materialize-gcs-csv:dev
* CSV Files in S3
  * [Configuration](./amazon-s3-csv.md)
  * Package — ghcr.io/estuary/materialize-s3-csv:dev
* Databricks
  * [Configuration](./databricks.md)
  * Package — ghcr.io/estuary/materialize-databricks:dev
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
* Google Cloud SQL Server
  * [Configuration](./SQLServer/google-cloud-sql-sqlserver.md)
  * Package = ghcr.io/estuary/materialize-google-cloud-sql-sqlserver:dev
* MongoDB
  * [Configuration](./mongodb.md)
  * Package - ghcr.io/estuary/materialize-mongodb:dev
* MotherDuck
  * [Configuration](./motherduck.md)
  * Package - ghcr.io/estuary/materialize-motherduck:dev
* MySQL
  * [Configuration](./MySQL/)
  * Package - ghcr.io/estuary/materialize-mysql:dev
* Pinecone
  * [Configuration](./pinecone.md)
  * Package — ghcr.io/estuary/materialize-pinecone:dev
* PostgreSQL
  * [Configuration](./PostgreSQL/)
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
* SQL Server
  * [Configuration](./SQLServer/)
  * Package - ghcr.io/estuary/materialize-sqlserver:dev
* TimescaleDB
  * [Configuration](./timescaledb.md)
  * Package - ghcr.io/estuary/materialize-timescaledb:dev
