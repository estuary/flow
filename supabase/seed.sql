begin;

insert into auth.users (id, email) values
  -- Root account which provisions other accounts.
  -- It must exist for the agent to function.
  ('ffffffff-ffff-ffff-ffff-ffffffffffff', 'support@estuary.dev'),
  -- Accounts which are commonly used in tests.
  ('11111111-1111-1111-1111-111111111111', 'alice@example.com'),
  ('22222222-2222-2222-2222-222222222222', 'bob@example.com'),
  ('33333333-3333-3333-3333-333333333333', 'carol@example.com'),
  ('44444444-4444-4444-4444-444444444444', 'dave@example.com')
;

-- Public directive which allows a new user to provision a new tenant.
insert into public.directives (catalog_prefix, spec, token) values
  ('ops/', '{"type":"clickToAccept"}', 'd4a37dd7-1bf5-40e3-b715-60c4edd0f6dc'),
  ('ops/', '{"type":"betaOnboard"}', '453e00cd-e12a-4ce5-b12d-3837aa385751'),
  ('ops/', '{"type":"acceptDemoTenant"}', '14c0beec-422f-4e95-94f1-567107b26840');

-- Provision the ops/ tenant owned by the support@estuary.dev user.
with accounts_root_user as (
  select (select id from auth.users where email = 'support@estuary.dev' limit 1) as accounts_id
)
insert into public.applied_directives (directive_id, user_id, user_claims)
  select d.id, a.accounts_id, '{"requestedTenant":"ops.us-central1.v1"}'
    from public.directives d, accounts_root_user a
    where catalog_prefix = 'ops/' and spec = '{"type":"betaOnboard"}';

insert into public.role_grants (subject_role, object_role, capability) values
  -- L1 roll-ups can read task logs & stats.
  ('ops/rollups/L1/', 'ops/tasks/', 'read'),
  -- L1 roll-ups tasks can write to themselves.
  ('ops/rollups/L1/', 'ops/rollups/L1/', 'write'),
  -- L2 roll-ups can read L1 roll-ups.
  ('ops.us-central1.v1/', 'ops/rollups/L1/', 'read'),
  -- L2 roll-ups can write to themselves.
  ('ops.us-central1.v1/', 'ops.us-central1.v1/', 'write')
  ;

-- Ops collections are directed to estuary-flow-poc and not estuary-trial for $reasons.
insert into public.storage_mappings (catalog_prefix, spec) values
  ('ops/', '{"stores": [{"provider": "GCS", "bucket": "estuary-flow-poc", "prefix": "collection-data/"}]}'),
  ('recovery/ops/', '{"stores": [{"provider": "GCS", "bucket": "estuary-flow-poc"}]}'),
  -- For access within local stack contexts:
  ('ops.us-central1.v1/', '{"stores": [{"provider": "GCS", "bucket": "estuary-trial", "prefix": "collection-data/"}]}'),
  ('recovery/ops.us-central1.v1/', '{"stores": [{"provider": "GCS", "bucket": "estuary-trial"}]}')
  ;

-- Give support@estuary.dev the admin role for `ops/` and `ops.us-central1.v1/` management.
insert into public.user_grants (user_id, object_role, capability) values
  -- TODO: estuary_support/ is currently required for control-plane automation.
  -- We should instead explicitly check for `system_user_id`.
  ('ffffffff-ffff-ffff-ffff-ffffffffffff', 'estuary_support/', 'admin'),
  -- support@estuary.dev manages `ops/`.
  ('ffffffff-ffff-ffff-ffff-ffffffffffff', 'ops/', 'admin'),
  -- support@estuary.dev manages legacy `ops.us-central1.v1/` L2 roll-ups and materialization.
  ('ffffffff-ffff-ffff-ffff-ffffffffffff', 'ops.us-central1.v1/', 'admin')
  ;

-- Seed a small number of connectors. This is a small list, separate from our
-- production connectors, because each is pulled onto your dev machine.
do $$
declare
  connector_id public.flowid;
begin

  insert into public.connectors (image_name, title, short_description, logo_url, external_url, recommended) values (
    'ghcr.io/estuary/source-hello-world',
    json_build_object('en-US','Hello World'),
    json_build_object('en-US','A flood of greetings'),
    json_build_object('en-US','https://www.estuary.dev/wp-content/uploads/2022/05/Group-4-300x300.png'),
    'https://estuary.dev',
    true
  )
  returning id strict into connector_id;
  insert into public.connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into public.connectors (image_name, title, short_description, logo_url, external_url, recommended) values (
    'ghcr.io/estuary/source-postgres',
    json_build_object('en-US','PostgreSQL'),
    json_build_object('en-US','Capture PostgreSQL tables into collections'),
    json_build_object('en-US','https://www.postgresql.org/media/img/about/press/elephant.png'),
    'https://postgresql.org',
    true
  )
  returning id strict into connector_id;
  insert into public.connector_tags (connector_id, image_tag) values (connector_id, ':dev');

  insert into public.connectors (image_name, title, short_description, logo_url, external_url, recommended) values (
    'ghcr.io/estuary/materialize-postgres',
    json_build_object('en-US','PostgreSQL'),
    json_build_object('en-US','Materialize collections into PostgreSQL'),
    json_build_object('en-US','https://www.postgresql.org/media/img/about/press/elephant.png'),
    'https://postgresql.org',
    true
  )
  returning id strict into connector_id;
  insert into public.connector_tags (connector_id, image_tag) values (connector_id, ':dev');

end;
$$ language plpgsql;

-- TODO(johnny): Support deprecated gateway_auth_token() RPC to be removed:
insert into internal.gateway_auth_keys (secret_key, detail) values (
  'supersecret', 'Used for development only. This value will be changed manually when deployed to production.'
);
insert into internal.gateway_endpoints (name, url, detail) values (
  'local', 'https://localhost:28318/', 'Used for development only. This value will be changed manually when deployed to production.'
);

commit;
