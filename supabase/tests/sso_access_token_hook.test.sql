-- Tests for the check_sso_requirement that adds sso_not_satisfied claim.
create function tests.test_sso_access_token_hook()
returns setof text as $$
declare
  provider_acme uuid = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  alice_id uuid = '11111111-1111-1111-1111-111111111111';
  result jsonb;
begin
  -- Setup: test user.
  insert into auth.users (id, email) values
    (alice_id, 'alice@example.com')
  on conflict do nothing;

  -- Setup: SSO provider for acmeCo.
  insert into auth.sso_providers (id) values (provider_acme)
    on conflict do nothing;

  -- Tenants: acmeCo has SSO configured, openCo does not.
  delete from tenants;
  insert into tenants (tenant, sso_provider_id) values
    ('acmeCo/', provider_acme),
    ('openCo/', null);

  -- Alice has grants on both tenants, no SSO identity yet.
  delete from user_grants;
  insert into user_grants (user_id, object_role, capability) values
    (alice_id, 'acmeCo/', 'admin'),
    (alice_id, 'openCo/', 'admin');

  delete from auth.identities where user_id = alice_id;

  -- No SSO identity — should get sso_not_satisfied with acmeCo's provider.
  select public.check_sso_requirement(jsonb_build_object(
    'user_id', alice_id,
    'claims', jsonb_build_object('sub', alice_id)
  )) into result;

  return next is(
    result->'claims'->'sso_not_satisfied',
    to_jsonb(provider_acme),
    'Non-SSO user on SSO tenant gets sso_not_satisfied'
  );

  -- Add SSO identity — sso_not_satisfied should disappear.
  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    (alice_id, 'sso:' || provider_acme::text, provider_acme::text, '{}'::jsonb);

  select public.check_sso_requirement(jsonb_build_object(
    'user_id', alice_id,
    'claims', jsonb_build_object('sub', alice_id)
  )) into result;

  return next ok(
    result->'claims'->'sso_not_satisfied' is null,
    'SSO user on own tenant has no sso_not_satisfied claim'
  );

  -- Only open-tenant grants: no sso_not_satisfied.
  delete from user_grants where user_id = alice_id;
  delete from auth.identities where user_id = alice_id;
  insert into user_grants (user_id, object_role, capability) values
    (alice_id, 'openCo/', 'admin');

  select public.check_sso_requirement(jsonb_build_object(
    'user_id', alice_id,
    'claims', jsonb_build_object('sub', alice_id)
  )) into result;

  return next ok(
    result->'claims'->'sso_not_satisfied' is null,
    'User with only open-tenant grants has no sso_not_satisfied claim'
  );

  -- Sub-prefix grant on acmeCo/reports/ should still trigger sso_not_satisfied.
  delete from user_grants where user_id = alice_id;
  insert into user_grants (user_id, object_role, capability) values
    (alice_id, 'acmeCo/reports/', 'read');

  select public.check_sso_requirement(jsonb_build_object(
    'user_id', alice_id,
    'claims', jsonb_build_object('sub', alice_id)
  )) into result;

  return next is(
    result->'claims'->'sso_not_satisfied',
    to_jsonb(provider_acme),
    'Sub-prefix grant on acmeCo/reports/ triggers sso_not_satisfied'
  );

  -- Malformed event (null user_id) should not throw — the exception handler
  -- returns the event unmodified so JWT issuance is never blocked.
  select public.check_sso_requirement(jsonb_build_object(
    'user_id', null,
    'claims', jsonb_build_object('sub', 'bogus')
  )) into result;

  return next ok(
    result->'claims'->>'sub' = 'bogus',
    'Malformed event returns claims unchanged (exception handler fires)'
  );

  return next ok(
    result->'claims'->'sso_not_satisfied' is null,
    'Malformed event does not inject sso_not_satisfied'
  );

  return;
end
$$ language plpgsql;
