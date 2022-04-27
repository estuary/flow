
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

  insert into connectors (image_name, detail, external_url) values (
    'ghcr.io/estuary/source-hello-world',
    'A flood of greetings',
    'https://estuary.dev'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail, external_url) values (
    'ghcr.io/estuary/source-postgres',
    'Capture PostgreSQL tables into collections',
    'https://postgresql.org'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into connectors (image_name, detail, external_url) values (
    'ghcr.io/estuary/materialize-postgres',
    'Materialize collections into PostgreSQL',
    'https://postgresql.org'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':dev');

end;
$$ language plpgsql;

-- Create some storage mappings.
do $$
declare
  connector_id flowid;
begin

  -- This is an awkward list which lets us accomodate our
  -- current catalog integration tests.
  insert into storage_mappings (catalog_prefix, spec) values
    ('acmeBank/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
    ('example/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
    ('examples/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
    ('marketing/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
    ('ops/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
    ('patterns/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
    ('recovery/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
    ('soak/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
    ('stock/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
    ('temperature/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}');

end;
$$ language plpgsql;

commit;
