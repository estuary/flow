
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
  ('33333333-3333-3333-3333-333333333333', 'carolCo/', 'admin')
;

-- Also grant other namespaces commonly used while testing.
-- aliceCo, bobCo, and carolCo are distinct owned namespaces,
-- but all are also able to admin examples/
insert into role_grants (subject_role, object_role, capability) values
  ('aliceCo/', 'aliceCo/', 'write'),
  ('aliceCo/', 'examples/', 'admin'),
  ('aliceCo/', 'ops/aliceCo/', 'read'),
  ('bobCo/', 'bobCo/', 'write'),
  ('bobCo/', 'examples/', 'admin'),
  ('bobCo/', 'ops/bobCo/', 'read'),
  ('carolCo/', 'carolCo/', 'write'),
  ('carolCo/', 'examples/', 'admin'),
  ('carolCo/', 'ops/carolCo/', 'read'),
  ('examples/', 'examples/', 'write'),
  ('examples/', 'ops/examples/', 'read')
;

-- Create corresponding storage mappings.
insert into storage_mappings (catalog_prefix, spec) values
  ('aliceCo/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('bobCo/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('carolCo/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('examples/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('ops/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}'),
  ('recovery/', '{"stores":[{"provider":"S3","bucket":"a-bucket"}]}');

-- Seed a small number of connectors. This is a small list, separate from our
-- production connectors, because each is pulled onto your dev machine.
do $$
declare
  connector_id flowid;
begin

  insert into connectors (image_name, title, short_description, external_url) values (
    'ghcr.io/estuary/source-hello-world',
    json_build_object('en-US','Hello World'),
    json_build_object('en-US','A flood of greetings'),
    'https://estuary.dev'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':v1');

  insert into connectors (image_name, title, short_description, external_url) values (
    'ghcr.io/estuary/source-postgres',
    json_build_object('en-US','PostgreSQL'),
    json_build_object('en-US','Capture PostgreSQL tables into collections'),
    'https://postgresql.org'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':v1');

  insert into connectors (image_name, title, short_description, external_url) values (
    'ghcr.io/estuary/materialize-postgres',
    json_build_object('en-US','PostgreSQL'),
    json_build_object('en-US','Materialize collections into PostgreSQL'),
    'https://postgresql.org'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':v1');

end;
$$ language plpgsql;

commit;
