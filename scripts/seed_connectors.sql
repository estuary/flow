
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
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':01fb856');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/source-postgres',
    'CDC connector for PostgreSQL'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':f1bd86a');

  insert into connectors (image_name, detail) values (
    'ghcr.io/estuary/materialize-postgres',
    'Materialize views into PostgreSQL'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':898776b');

end;
$$ language plpgsql;

commit;
