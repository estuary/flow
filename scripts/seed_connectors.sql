
-- This script is used to drop and re-create recommended connectors,
-- along with their descriptions and tags. Run as:
--
--  psql ${DATABASE_URL} --file scripts/seed_connectors.sql

begin;

delete from connector_tags;
delete from connectors;

do $$
declare
  connector_id flowid;
begin

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/source-http-file',
    json_build_object('en-US','HTTP File'),
    json_build_object('en-US',''),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/07/Group-22372-5-300x300.png'),
    false,
    'https://go.estuary.dev/source-http-file'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/source-s3',
    json_build_object('en-US','Amazon S3'),
    json_build_object('en-US',''),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2021/09/Amazon-S3.png'),
    false,
    'https://aws.amazon.com/s3/'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/source-postgres',
    json_build_object('en-US','PostgreSQL'),
    json_build_object('en-US','The world''s most advanced open source database.'),
    json_build_object('en-US','https://www.postgresql.org/media/img/about/press/elephant.png'),
    true,
    'https://postgresql.org'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/source-mysql',
    json_build_object('en-US','MySQL'),
    json_build_object('en-US',''),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/05/mysql-300x295.png'),
    true,
    'https://www.mysql.com/'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/source-kafka',
    json_build_object('en-US','Apache Kafka'),
    json_build_object('en-US','Apache Kafka: A Distributed Streaming Platform.'),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/05/kafka-300x300.png'),
    false,
    'https://kafka.apache.org/'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/materialize-bigquery',
    json_build_object('en-US','Bigquery'),
    json_build_object('en-US','BigQuery is a serverless, cost-effective and multicloud data warehouse designed to help you turn big data into valuable business insights. Start free.'),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/05/image-12513891-2-300x300.png'),
    false,
    'https://cloud.google.com/bigquery'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/materialize-s3-parquet',
    json_build_object('en-US','Amazon S3 Parquet'),
    json_build_object('en-US',''),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2021/09/Parquet.png'),
    false,
    'https://aws.amazon.com/s3/'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/source-gcs',
    json_build_object('en-US','Google Cloud Storage'),
    json_build_object('en-US','Object storage for companies of all sizes. Secure, durable, and with low latency. Store any amount of data. Retrieve it as often as you’d like.'),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/05/image-12513891-300x300.png'),
    false,
    'https://cloud.google.com/storage'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/materialize-postgres',
    json_build_object('en-US','PostgreSQL'),
    json_build_object('en-US','The world''s most advanced open source database.'),
    json_build_object('en-US','https://www.postgresql.org/media/img/about/press/elephant.png'),
    false,
    'https://postgresql.org'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/materialize-firebolt',
    json_build_object('en-US','Firebolt'),
    json_build_object('en-US','Firebolt is a complete redesign of the cloud data warehouse for the era of cloud and data lakes. Data warehousing with extreme speed & elasticity at scale.'),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/05/Bitmap-300x300.png'),
    false,
    'https://www.firebolt.io/'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/materialize-snowflake',
    json_build_object('en-US','Snowflake Data Cloud'),
    json_build_object('en-US',''),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2021/10/Snowflake.png'),
    false,
    'https://external'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/source-hello-world',
    json_build_object('en-US','Hello World'),
    json_build_object('en-US','Connectors for capturing data from external data sources - connectors/source-hello-world at main · estuary/connectors'),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/05/Group-4-300x300.png'),
    false,
    'https://github.com/estuary/connectors/tree/main/source-hello-world'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/materialize-rockset',
    json_build_object('en-US','Rockset'),
    json_build_object('en-US','Rockset is a real-time analytics database for serving fast analytics at scale, enabling developers to build modern data apps in record time.'),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/05/rockset-150x150.png'),
    false,
    'https://rockset.com/'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/source-kinesis',
    json_build_object('en-US','Amazon Kinesis'),
    json_build_object('en-US',''),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/05/Group-22372-2-300x300.png'),
    false,
    'https://aws.amazon.com/kinesis/'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

insert into connectors (image_name, title, short_description, logo_url, recommended external_url) values (
    'ghcr.io/estuary/materialize-elasticsearch',
    json_build_object('en-US','Elastic'),
    json_build_object('en-US','Elasticsearch is the leading distributed, RESTful, free and open search and analytics engine designed for speed, horizontal scalability, reliability, and easy management. Get started for free.'),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/05/Elastic-300x300.png'),
    false,
    'https://www.elastic.co/elasticsearch/'
)
returning id strict into connector_id;
insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

end;
$$ language plpgsql;

commit;
