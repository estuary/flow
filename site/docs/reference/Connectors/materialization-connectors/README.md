# Materialization Connectors

Estuary's available materialization connectors are listed in this section. Each connector has a unique set of requirements for configuration; these are linked below the connector name.

Also listed are links to the most recent Docker images for each connector. You'll need these to write Data Flow specifications manually (if you're [developing locally](../../../concepts/flowctl.md)). If you're using the Estuary web app, they aren't necessary.

Estuary is actively developing new connectors, so check back regularly for the latest additions. We’re prioritizing the development of high-scale technological systems, as well as client needs.

At this time, all the available materialization connectors are created by Estuary.
In the future, other open-source materialization connectors from third parties could be supported.

## Available materialization connectors

* AlloyDB
  * [Configuration](./alloydb.md)
  * Package - ghcr.io/estuary/materialize-alloydb:v5
* Amazon DynamoDB
  * [Configuration](./amazon-dynamodb.md)
  * Package - ghcr.io/estuary/materialize-dynamodb:v1
* Amazon MySQL
  * [Configuration](./MySQL/amazon-rds-mysql.md)
  * Package - ghcr.io/estuary/materialize-amazon-rds-mysql:v2
* Amazon PostgreSQL
  * [Configuration](./PostgreSQL/amazon-rds-postgres.md)
  * Package - ghcr.io/estuary/materialize-amazon-rds-postgres:v5
* Amazon Redshift
  * [Configuration](./amazon-redshift.md)
  * Package - ghcr.io/estuary/materialize-redshift:v2
* Amazon SQL Server
  * [Configuration](./SQLServer/amazon-rds-sqlserver.md)
  * Package - ghcr.io/estuary/materialize-amazon-rds-sqlserver:v2
* Apache Iceberg Tables
  * [Configuration](./Apache-Iceberg/Apache-Iceberg.md)
  * Package — ghcr.io/estuary/materialize-iceberg:v1
* Apache Iceberg Tables in S3 (delta updates)
  * [Configuration](./amazon-s3-iceberg.md)
  * Package — ghcr.io/estuary/materialize-s3-iceberg:v2
* Apache Kafka
  * [Configuration](./apache-kafka.md)
  * Package — ghcr.io/estuary/materialize-kafka:v1
* Apache Parquet Files in Azure Blob Storage
  * [Configuration](./azure-blob-parquet.md)
  * Package — ghcr.io/estuary/materialize-azure-blob-parquet:v1
* Apache Parquet Files in GCS
  * [Configuration](./google-gcs-parquet.md)
  * Package — ghcr.io/estuary/materialize-gcs-parquet:v1
* Apache Parquet Files in S3
  * [Configuration](./amazon-s3-parquet.md)
  * Package — ghcr.io/estuary/materialize-s3-parquet:v3
* Azure Fabric Warehouse
  * [Configuration](./azure-fabric-warehouse.md)
  * Package - ghcr.io/estuary/materialize-azure-fabric-warehouse:v1
* Azure SQL Server
  * [Configuration](./SQLServer/)
  * Package - ghcr.io/estuary/materialize-sqlserver:v2
* Bytewax
  * [Configuration](./Dekaf/bytewax.md)
* ClickHouse
  * [Configuration](./Dekaf/clickhouse.md)
* CSV Files in GCS
  * [Configuration](./google-gcs-csv.md)
  * Package — ghcr.io/estuary/materialize-gcs-csv:v1
* CSV Files in S3
  * [Configuration](./amazon-s3-csv.md)
  * Package — ghcr.io/estuary/materialize-s3-csv:v1
* Databricks
  * [Configuration](./databricks.md)
  * Package — ghcr.io/estuary/materialize-databricks:v3
* Dekaf
  * [Configuration](./Dekaf/dekaf.md)
* Elasticsearch
  * [Configuration](./Elasticsearch.md)
  * Package — ghcr.io/estuary/materialize-elasticsearch:v3
* Firebolt
  * [Configuration](./Firebolt.md)
  * Package - ghcr.io/estuary/materialize-firebolt:v1
* Google BigQuery
  * [Configuration](./BigQuery.md)
  * Package — ghcr.io/estuary/materialize-bigquery:v3
* Google Cloud MySQL
  * [Configuration](./MySQL/google-cloud-sql-mysql.md)
  * Package - ghcr.io/estuary/materialize-google-cloud-sql-mysql:v2
* Google Cloud PostgreSQL
  * [Configuration](./PostgreSQL/google-cloud-sql-postgres.md)
  * Package - ghcr.io/estuary/materialize-google-cloud-sql-postgres:v5
* Google Cloud Pub/Sub
  * [Configuration](./google-pubsub.md)
  * Package - ghcr.io/estuary/materialize-google-pubsub:v1
* Google Cloud SQL Server
  * [Configuration](./SQLServer/google-cloud-sql-sqlserver.md)
  * Package - ghcr.io/estuary/materialize-google-cloud-sql-sqlserver:v2
* Google Sheets
  * [Configuration](./Google-sheets.md)
  * Package - ghcr.io/estuary/materialize-google-sheets:v2
* HTTP Webhook
  * [Configuration](./http-webhook.md)
  * Package - ghcr.io/estuary/materialize-webhook:v1
* Imply Polaris
  * [Configuration](./Dekaf/imply-polaris.md)
* Materialize
  * [Configuration](./Dekaf/materialize.md)
* MongoDB
  * [Configuration](./mongodb.md)
  * Package - ghcr.io/estuary/materialize-mongodb:v1
* MotherDuck
  * [Configuration](./motherduck.md)
  * Package - ghcr.io/estuary/materialize-motherduck:v4
* MySQL
  * [Configuration](./MySQL/)
  * Package - ghcr.io/estuary/materialize-mysql:v2
* MySQL Heatwave
  * [Configuration](./mysql-heatwave.md)
  * Package - ghcr.io/estuary/materialize-mysql-heatwave:v2
* Pinecone
  * [Configuration](./pinecone.md)
  * Package — ghcr.io/estuary/materialize-pinecone:v1
* PostgreSQL
  * [Configuration](./PostgreSQL/)
  * Package — ghcr.io/estuary/materialize-postgres:v5
* Rockset (Deprecated)
  * [Configuration](./Rockset.md)
  * Package — ghcr.io/estuary/materialize-rockset:v2
* SingleStore
  * [Configuration](./MySQL/singlestore-mysql.md)
  * Package - ghcr.io/estuary/materialize-singlestore:v2
* SingleStore (Dekaf)
  * [Configuration](./Dekaf/singlestore.md)
* Slack
  * [Configuration](./slack.md)
  * Package - ghcr.io/estuary/materialize-slack:v1
* Snowflake
  * [Configuration](./Snowflake.md)
  * Package — ghcr.io/estuary/materialize-snowflake:v4
* SQLite
  * [Configuration](./SQLite.md)
  * Package — ghcr.io/estuary/materialize-sqlite:v1
* SQL Server
  * [Configuration](./SQLServer/)
  * Package - ghcr.io/estuary/materialize-sqlserver:v2
* Starburst
  * [Configuration](./starburst.md)
  * Package - ghcr.io/estuary/materialize-starburst:v1
* Startree
  * [Configuration](./Dekaf/startree.md)
* Supabase
  * [Configuration](./PostgreSQL/supabase.md)
  * Package - ghcr.io/estuary/materialize-supabase-postgres:v5
* TimescaleDB
  * [Configuration](./timescaledb.md)
  * Package - ghcr.io/estuary/materialize-timescaledb:v5
* Tinybird
  * [Configuration](./Dekaf/tinybird.md)
