
begin;

insert into auth.users (id, email) values
  ('11111111-1111-1111-1111-111111111111', 'alice@example.com'),
  ('22222222-2222-2222-2222-222222222222', 'bob@example.com'),
  ('33333333-3333-3333-3333-333333333333', 'carol@example.com')
;

-- Seed a small number of connectors. This is a small list, separate from our
-- production connectors, because each is pulled onto your dev machine.
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

end;
$$ language plpgsql;

commit;
