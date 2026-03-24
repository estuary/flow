-- SSO grant migration: automatic transfer when social user re-authenticates via SSO.

-- Helper to clean up state between tests.
create or replace function tests._clean_sso_migration()
returns void as $$
begin
  delete from public.user_grants;
  delete from auth.identities where user_id in (
    select id from auth.users where email like '%@sso-migration-test.example'
  );
  delete from auth.users where email like '%@sso-migration-test.example';
  -- Delete role_grants and tenants before sso_providers (FK + trigger constraints).
  delete from role_grants where subject_role = 'estuary_support/';
  delete from tenants;
  delete from auth.sso_providers where id in (
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'
  );
end
$$ language plpgsql;

-- Basic migration + mixed transferable/non-transferable grants.
create function tests.test_sso_grant_migration()
returns setof text as $$
declare
  provider_acme uuid = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  provider_bigcorp uuid = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';

  old_alice uuid = 'a1111111-1111-1111-1111-111111111111';
  new_alice uuid = 'a5555555-5555-5555-5555-555555555555';
begin
  perform tests._clean_sso_migration();

  insert into auth.sso_providers (id) values (provider_acme), (provider_bigcorp);

  insert into tenants (tenant, sso_provider_id) values
    ('acmeCo/', provider_acme),
    ('bigcorpCo/', provider_bigcorp),
    ('openCo/', null);

  -- Old Alice: social user with grants on all three tenants.
  insert into auth.users (id, email, is_sso_user) values
    (old_alice, 'alice@sso-migration-test.example', false);

  insert into user_grants (user_id, object_role, capability) values
    (old_alice, 'acmeCo/', 'admin'),
    (old_alice, 'bigcorpCo/', 'read'),
    (old_alice, 'openCo/', 'write');

  -- New Alice: SSO user with same email.
  insert into auth.users (id, email, is_sso_user) values
    (new_alice, 'alice@sso-migration-test.example', true);

  -- This INSERT fires the trigger.
  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    (new_alice, 'sso:' || provider_acme::text, provider_acme::text, '{}'::jsonb);

  -- acmeCo/ (matching SSO) and openCo/ (no SSO) should transfer.
  -- bigcorpCo/ (different SSO provider) should be skipped.
  return next results_eq(
    $i$ select object_role::text, capability::text
        from user_grants where user_id = 'a5555555-5555-5555-5555-555555555555'
        order by object_role $i$,
    $i$ values ('acmeCo/', 'admin'), ('openCo/', 'write') $i$,
    'matching SSO + non-SSO grants transferred to new user'
  );

  -- Old user's grants are preserved for the old account.
  return next results_eq(
    $i$ select count(*)::int from user_grants
        where user_id = 'a1111111-1111-1111-1111-111111111111' $i$,
    $i$ values (3) $i$,
    'old user grants preserved'
  );

  return;
end
$$ language plpgsql;

-- Capability upgrade: new user already has a lower grant.
create function tests.test_sso_grant_migration_capability_upgrade()
returns setof text as $$
declare
  provider_acme uuid = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  old_bob uuid = 'a2222222-2222-2222-2222-222222222222';
  new_bob uuid = 'a6666666-6666-6666-6666-666666666666';
begin
  perform tests._clean_sso_migration();

  insert into auth.sso_providers (id) values (provider_acme);

  insert into tenants (tenant) values ('openCo/');

  insert into auth.users (id, email, is_sso_user) values
    (old_bob, 'bob@sso-migration-test.example', false);

  insert into user_grants (user_id, object_role, capability) values
    (old_bob, 'openCo/', 'admin');

  -- New Bob already has a read grant on openCo/.
  insert into auth.users (id, email, is_sso_user) values
    (new_bob, 'bob@sso-migration-test.example', true);

  insert into user_grants (user_id, object_role, capability) values
    (new_bob, 'openCo/', 'read');

  -- Trigger fires: old Bob's admin should upgrade new Bob's read.
  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    (new_bob, 'sso:' || provider_acme::text, provider_acme::text, '{}'::jsonb);

  return next results_eq(
    $i$ select capability::text from user_grants
        where user_id = 'a6666666-6666-6666-6666-666666666666'
          and object_role = 'openCo/' $i$,
    $i$ values ('admin') $i$,
    'capability upgraded from read to admin'
  );

  return;
end
$$ language plpgsql;

-- No downgrade: new user already has a higher grant than old user.
create function tests.test_sso_grant_migration_capability_no_downgrade()
returns setof text as $$
declare
  provider_acme uuid = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  old_bob uuid = 'a2222222-2222-2222-2222-222222222222';
  new_bob uuid = 'a6666666-6666-6666-6666-666666666666';
begin
  perform tests._clean_sso_migration();

  insert into auth.sso_providers (id) values (provider_acme);

  insert into tenants (tenant) values ('openCo/');

  insert into auth.users (id, email, is_sso_user) values
    (old_bob, 'bob@sso-migration-test.example', false);

  insert into user_grants (user_id, object_role, capability) values
    (old_bob, 'openCo/', 'read');

  -- New Bob already has an admin grant on openCo/.
  insert into auth.users (id, email, is_sso_user) values
    (new_bob, 'bob@sso-migration-test.example', true);

  insert into user_grants (user_id, object_role, capability) values
    (new_bob, 'openCo/', 'admin');

  -- Trigger fires: old Bob's read should NOT downgrade new Bob's admin.
  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    (new_bob, 'sso:' || provider_acme::text, provider_acme::text, '{}'::jsonb);

  return next results_eq(
    $i$ select capability::text from user_grants
        where user_id = 'a6666666-6666-6666-6666-666666666666'
          and object_role = 'openCo/' $i$,
    $i$ values ('admin') $i$,
    'capability not downgraded from admin to read'
  );

  return;
end
$$ language plpgsql;

-- No migration when there is no prior social user.
create function tests.test_sso_grant_migration_no_old_user()
returns setof text as $$
declare
  provider_acme uuid = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  new_carol uuid = 'a7777777-7777-7777-7777-777777777777';
begin
  perform tests._clean_sso_migration();

  insert into auth.sso_providers (id) values (provider_acme);

  insert into auth.users (id, email, is_sso_user) values
    (new_carol, 'carol@sso-migration-test.example', true);

  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    (new_carol, 'sso:' || provider_acme::text, provider_acme::text, '{}'::jsonb);

  -- No grants should exist (none to migrate).
  return next is_empty(
    $i$ select 1 from user_grants
        where user_id = 'a7777777-7777-7777-7777-777777777777' $i$,
    'no grants created for brand-new SSO user with no prior social account'
  );

  return;
end
$$ language plpgsql;

-- Sub-prefix grants are matched to the correct tenant.
create function tests.test_sso_grant_migration_sub_prefix()
returns setof text as $$
declare
  provider_acme uuid = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  provider_bigcorp uuid = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';

  old_eve uuid = 'a4444444-4444-4444-4444-444444444444';
  new_eve uuid = 'a9999999-9999-9999-9999-999999999999';
begin
  perform tests._clean_sso_migration();

  insert into auth.sso_providers (id) values (provider_acme), (provider_bigcorp);

  insert into tenants (tenant, sso_provider_id) values
    ('acmeCo/', provider_acme),
    ('bigcorpCo/', provider_bigcorp);

  -- Old Eve has grants on sub-prefixes under both tenants.
  insert into auth.users (id, email, is_sso_user) values
    (old_eve, 'eve@sso-migration-test.example', false);

  insert into user_grants (user_id, object_role, capability) values
    (old_eve, 'acmeCo/team/data/', 'write'),
    (old_eve, 'bigcorpCo/eng/', 'read');

  -- New Eve signs in via acmeCo's SSO provider.
  insert into auth.users (id, email, is_sso_user) values
    (new_eve, 'eve@sso-migration-test.example', true);

  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    (new_eve, 'sso:' || provider_acme::text, provider_acme::text, '{}'::jsonb);

  -- acmeCo/team/data/ should transfer (sub-prefix of matching SSO tenant).
  return next results_eq(
    $i$ select object_role::text, capability::text
        from user_grants where user_id = 'a9999999-9999-9999-9999-999999999999'
        order by object_role $i$,
    $i$ values ('acmeCo/team/data/', 'write') $i$,
    'sub-prefix grant under matching SSO tenant transferred'
  );

  -- bigcorpCo/eng/ should NOT transfer (sub-prefix of different SSO tenant).
  return next is_empty(
    $i$ select 1 from user_grants
        where user_id = 'a9999999-9999-9999-9999-999999999999'
          and object_role = 'bigcorpCo/eng/' $i$,
    'sub-prefix grant under different SSO tenant not transferred'
  );

  return;
end
$$ language plpgsql;

-- Non-SSO identity insert does not fire the trigger.
create function tests.test_sso_grant_migration_non_sso_identity()
returns setof text as $$
declare
  old_dave uuid = 'a3333333-3333-3333-3333-333333333333';
  new_dave uuid = 'a8888888-8888-8888-8888-888888888888';
begin
  perform tests._clean_sso_migration();

  insert into tenants (tenant) values ('openCo/');

  insert into auth.users (id, email, is_sso_user) values
    (old_dave, 'dave@sso-migration-test.example', false);

  insert into user_grants (user_id, object_role, capability) values
    (old_dave, 'openCo/', 'admin');

  -- New Dave signs in with Google (not SSO). Use is_sso_user = true
  -- to avoid the partial unique constraint on email for non-SSO users.
  insert into auth.users (id, email, is_sso_user) values
    (new_dave, 'dave@sso-migration-test.example', true);

  -- Insert a Google identity — trigger should NOT fire.
  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    (new_dave, 'google', 'google-id-123', '{}'::jsonb);

  -- Old Dave's grants should still be intact.
  return next results_eq(
    $i$ select count(*)::int from user_grants
        where user_id = 'a3333333-3333-3333-3333-333333333333' $i$,
    $i$ values (1) $i$,
    'old user grants untouched for non-SSO identity insert'
  );

  -- New Dave should have no grants (trigger didn't fire).
  return next is_empty(
    $i$ select 1 from user_grants
        where user_id = 'a8888888-8888-8888-8888-888888888888' $i$,
    'no grants migrated for non-SSO identity'
  );

  return;
end
$$ language plpgsql;
