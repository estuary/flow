
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

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/source-hello-world',
    json_build_object('en-US','A flood of greetings'),
    'https://github.com/estuary/connectors/tree/main/source-hello-world'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/source-postgres',
    json_build_object('en-US','Capture PostgreSQL tables into collections'),
    'https://postgresql.org'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/materialize-postgres',
    json_build_object('en-US','Materialize collections into PostgreSQL'),
    'https://postgresql.org'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/materialize-rockset',
    json_build_object('en-US','Materialize collections into Rockset'),
    'https://rockset.com/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/materialize-firebolt',
    json_build_object('en-US','Materialize collections into Firebolt'),
    'https://www.firebolt.io/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/source-mysql',
    json_build_object('en-US','Capture MySQL tables into collections'),
    'https://www.mysql.com/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/source-s3',
    json_build_object('en-US','Capture S3 files into collections'),
    'https://aws.amazon.com/s3/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/source-gcs',
    json_build_object('en-US','Capture Google Cloud Storage files into collections'),
    'https://cloud.google.com/storage'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/source-kinesis',
    json_build_object('en-US','Capture Kinesis topics into collections'),
    'https://aws.amazon.com/kinesis/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/source-kafka',
    json_build_object('en-US','Capture Kafka topics into collections'),
    'https://kafka.apache.org/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/materialize-bigquery',
    json_build_object('en-US','Materialize collections into BigQuery'),
    'https://cloud.google.com/bigquery'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/materialize-snowflake',
    json_build_object('en-US','Materialize collections into Snowflake'),
    'https://www.snowflake.com/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/materialize-s3-parquet',
    json_build_object('en-US','Materialize collections into S3 using Parquet'),
    'https://aws.amazon.com/s3/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/materialize-elasticsearch',
    json_build_object('en-US','Materialize collections into Elastic'),
    'https://www.elastic.co/elasticsearch/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, short_description, external_url) values (
    'airbyte/source-exchange-rates',
    json_build_object('en-US','Capture exchange rates into collections'),
    'https://exchangeratesapi.io/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.2.5');

  insert into connectors (image_name, short_description, external_url) values (
    'airbyte/source-hubspot',
    json_build_object('en-US','Capture from Hubspot into collections'),
    'https://www.hubspot.com/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.1.10');

  insert into connectors (image_name, short_description, external_url) values (
    'airbyte/source-facebook-marketing',
    json_build_object('en-US','Capture from Facebook Marketing into collections'),
    'https://www.facebook.com/business/marketing/facebook'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.2.14');

  insert into connectors (image_name, short_description, external_url) values (
    'airbyte/source-google-sheets',
    json_build_object('en-US','Capture from Google Sheets into collections'),
    'https://www.google.com/sheets/about/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.2.4');

  insert into connectors (image_name, short_description, external_url) values (
    'airbyte/source-google-ads',
    json_build_object('en-US','Capture from Google Ads into collections'),
    'https://ads.google.com/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.1.3');

  insert into connectors (image_name, short_description, external_url) values (
    'airbyte/source-github',
    json_build_object('en-US','Capture Github Events into collections'),
    'https://github.com/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.1.6');

  insert into connectors (image_name, short_description, external_url) values (
    'airbyte/source-google-analytics-v4',
    json_build_object('en-US','Capture from Google Analytics into collections'),
    'https://marketingplatform.google.com/about/analytics/'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.1.0');

  insert into connectors (image_name, short_description, external_url) values (
    'ghcr.io/estuary/source-http-file',
    json_build_object('en-US','Capture from any single file'),
    'https://go.estuary.dev/source-http-file'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

end;
$$ language plpgsql;

commit;
