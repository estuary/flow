
begin;

insert into auth.users (id, email) values
  ('11111111-1111-1111-1111-111111111111', 'alice@example.com'),
  ('22222222-2222-2222-2222-222222222222', 'bob@example.com'),
  ('33333333-3333-3333-3333-333333333333', 'carol@example.com')
;

-- Tweak auth.users to conform with what a local Supabase install creates
-- if you perform the email "Sign Up" flow. In development mode it
-- doesn't actually send an email, and immediately creates a record like this:
update auth.users set
  "role" = 'authenticated',
  aud = 'authenticated',
  confirmation_token = '',
  created_at = now(),
  email_change = '',
  email_change_confirm_status = 0,
  email_change_token_new = '',
  email_confirmed_at = now(),
  encrypted_password = '$2a$10$vQCyRoGamfEBXOR05iNgseK.ukEUPV52W1B95Qt6Tb3kN4N32odji', -- "password"
  instance_id = '00000000-0000-0000-0000-000000000000',
  is_super_admin = false,
  last_sign_in_at = now(),
  raw_app_meta_data = '{"provider": "email", "providers": ["email"]}',
  raw_user_meta_data = '{}',
  recovery_token = '',
  updated_at = now()
;

insert into auth.identities (id, user_id, identity_data, provider, last_sign_in_at, created_at, updated_at)
select id, id, json_build_object('sub', id), 'email', now(), now(), now() from auth.users;

insert into user_grants (user_id, object_role, capability) values
  ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
  ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin'),
  ('33333333-3333-3333-3333-333333333333', 'carolCo/', 'read')
;

-- Also grant other namespaces commonly used while testing.
insert into role_grants (subject_role, object_role, capability) values
  ('aliceCo/', 'aliceCo/', 'write'),
  ('bobCo/', 'acmeCo/', 'admin'),
  ('bobCo/', 'examples/', 'admin'),
  ('bobCo/', 'ops/bobCo/', 'read'),
  ('bobCo/', 'testing/', 'admin'),
  ('examples/', 'examples/', 'write'),
  ('examples/', 'ops/examples/', 'read'),
  ('testing/', 'testing/', 'write')
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
insert into storage_mappings (catalog_prefix, spec) values
  ('aliceCo/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('acmeCo/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('examples/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('ops/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('recovery/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('testing/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}');


-- TODO(johnny): Awkward additional grants and storage mappings which
-- allow our current catalog tests to work. We should clean these up
-- by renaming specifications in our integration tests.

insert into storage_mappings (catalog_prefix, spec) values
  ('acmeBank/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('example/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('marketing/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('patterns/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('soak/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('stock/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('temperature/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}');

insert into role_grants (subject_role, object_role, capability) values
  ('acmeBank/', 'acmeBank/', 'write'),
  ('bobCo/', 'acmeBank/', 'admin'),
  ('bobCo/', 'example/', 'admin'),
  ('bobCo/', 'marketing/', 'admin'),
  ('bobCo/', 'patterns/', 'admin'),
  ('bobCo/', 'soak/', 'admin'),
  ('bobCo/', 'stock/', 'admin'),
  ('bobCo/', 'temperature/', 'admin'),
  ('example/', 'example/', 'write'),
  ('marketing/', 'marketing/', 'write'),
  ('patterns/', 'patterns/', 'write'),
  ('soak/', 'soak/', 'write'),
  ('stock/', 'stock/', 'write'),
  ('temperature/', 'temperature/', 'write');

commit;
