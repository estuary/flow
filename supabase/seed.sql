begin;

insert into auth.users (id, email) values
  -- Root account which provisions other accounts.
  -- It must exist for the agent to function.
  ('ffffffff-ffff-ffff-ffff-ffffffffffff', 'accounts@estuary.dev'),
  -- Accounts which are commonly used in tests.
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

-- Public directive which allows a new user to provision a new tenant.
insert into directives (catalog_prefix, spec, token) values
  ('ops/', '{"type":"clickToAccept"}', 'd4a37dd7-1bf5-40e3-b715-60c4edd0f6dc'),
  ('ops/', '{"type":"betaOnboard"}', '453e00cd-e12a-4ce5-b12d-3837aa385751');

-- Provision the ops/ tenant owned by the accounts@estuary.dev user.
with accounts_root_user as (
  select (select id from auth.users where email = 'accounts@estuary.dev' limit 1) as accounts_id
)
insert into applied_directives (directive_id, user_id, user_claims)
  select d.id, a.accounts_id, '{"requestedTenant":"ops"}'
    from directives d, accounts_root_user a
    where catalog_prefix = 'ops/' and spec = '{"type":"betaOnboard"}';

-- Seed a small number of connectors. This is a small list, separate from our
-- production connectors, because each is pulled onto your dev machine.
do $$
declare
  connector_id flowid;
begin

  insert into connectors (image_name, title, short_description, logo_url, external_url) values (
    'ghcr.io/estuary/source-hello-world',
    json_build_object('en-US','Hello World'),
    json_build_object('en-US','A flood of greetings'),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/05/Group-4-300x300.png'),
    'https://estuary.dev'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':v1');

  insert into connectors (image_name, title, short_description, logo_url, external_url) values (
    'ghcr.io/estuary/source-postgres',
    json_build_object('en-US','PostgreSQL'),
    json_build_object('en-US','Capture PostgreSQL tables into collections'),
    json_build_object('en-US','https://www.postgresql.org/media/img/about/press/elephant.png'),
    'https://postgresql.org'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':v1');

  insert into connectors (image_name, title, short_description, logo_url, external_url) values (
    'ghcr.io/estuary/materialize-postgres',
    json_build_object('en-US','PostgreSQL'),
    json_build_object('en-US','Materialize collections into PostgreSQL'),
    json_build_object('en-US','https://www.postgresql.org/media/img/about/press/elephant.png'),
    'https://postgresql.org'
  )
  returning id strict into connector_id;
  insert into connector_tags (connector_id, image_tag) values (connector_id, ':v1');

end;
$$ language plpgsql;

commit;
