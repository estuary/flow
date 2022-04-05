
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

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/source-hello-world',
    'A flood of greetings'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/source-postgres',
    'Capture PostgreSQL tables into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/materialize-postgres',
    'Materialize collections into PostgreSQL'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/materialize-rockset',
    'Materialize collections into Rockset'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/materialize-firebolt',
    'Materialize collections into Firebolt'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/source-mysql',
    'Capture MySQL tables into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/source-s3',
    'Capture S3 files into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/source-gcs',
    'Capture Google Cloud Storage files into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/source-kinesis',
    'Capture Kinesis topics into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/source-kafka',
    'Capture Kafka topics into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/materialize-bigquery',
    'Materialize collections into BigQuery'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/materialize-snowflake',
    'Materialize collections into Snowflake'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/materialize-s3-parquet',
    'Materialize collections into S3 using Parquet'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/materialize-elasticsearch',
    'Materialize collections into Elastic'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail) values (
    'airbyte/source-exchange-rates',
    'Capture exchange rates into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.2.5');

  insert into connectors (image_name, detail) values (
    'airbyte/source-hubspot',
    'Capture from Hubspot into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.1.10');

  insert into connectors (image_name, detail) values (
    'airbyte/source-facebook-marketing',
    'Capture from Facebook Marketing into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.2.14');

  insert into connectors (image_name, detail) values (
    'airbyte/source-google-sheets',
    'Capture from Google Sheets into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.2.4');

  insert into connectors (image_name, detail) values (
    'airbyte/source-google-ads',
    'Capture from Google Ads into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.1.3');

  insert into connectors (image_name, detail) values (
    'airbyte/source-github',
    'Capture Github Events into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.1.6');

  insert into connectors (image_name, detail) values (
    'airbyte/source-google-analytics-v4',
    'Capture from Google Analytics into collections'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':0.1.0');

end;
$$ language plpgsql;

commit;
