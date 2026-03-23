-- Tests for the custom_access_token_hook that adds sso_required claim.
create function tests.test_sso_access_token_hook()
returns setof text as $$
declare
  provider_acme uuid = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  provider_bigcorp uuid = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';
  alice_id uuid = '11111111-1111-1111-1111-111111111111';
  bob_id uuid = '22222222-2222-2222-2222-222222222222';
  result jsonb;
begin
  -- Setup: test users.
  insert into auth.users (id, email) values
    (alice_id, 'alice@example.com'),
    (bob_id, 'bob@example.com')
  on conflict do nothing;

  -- Setup: two SSO providers.
  insert into auth.sso_providers (id) values (provider_acme), (provider_bigcorp)
    on conflict do nothing;

  -- Tenants: acmeCo and bigcorpCo enforce SSO, openCo does not.
  delete from tenants;
  insert into tenants (tenant, sso_provider_id, enforce_sso) values
    ('acmeCo/', provider_acme, true),
    ('bigcorpCo/', provider_bigcorp, true),
    ('openCo/', null, false);

  -- Alice has grants on all three, SSO identity for acmeCo only.
  delete from user_grants;
  insert into user_grants (user_id, object_role, capability) values
    (alice_id, 'acmeCo/', 'admin'),
    (alice_id, 'bigcorpCo/', 'read'),
    (alice_id, 'openCo/', 'admin');

  delete from auth.identities where user_id = alice_id;
  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    (alice_id, 'sso', provider_acme::text, '{}'::jsonb);

  -- Alice should get sso_required with bigcorpCo's provider (she lacks that identity).
  select public.custom_access_token_hook(jsonb_build_object(
    'user_id', alice_id,
    'claims', jsonb_build_object('sub', alice_id)
  )) into result;

  return next is(
    result->'claims'->'sso_required',
    jsonb_build_array(provider_bigcorp),
    'Alice gets sso_required with bigcorpCo provider ID'
  );

  -- Bob has grants on acmeCo and bigcorpCo, no SSO identities at all.
  insert into user_grants (user_id, object_role, capability) values
    (bob_id, 'acmeCo/', 'read'),
    (bob_id, 'bigcorpCo/', 'read'),
    (bob_id, 'openCo/', 'read');

  delete from auth.identities where user_id = bob_id;

  select public.custom_access_token_hook(jsonb_build_object(
    'user_id', bob_id,
    'claims', jsonb_build_object('sub', bob_id)
  )) into result;

  -- Bob should get both provider IDs (order not guaranteed, so check containment).
  return next ok(
    result->'claims'->'sso_required' @> jsonb_build_array(provider_acme)
      and result->'claims'->'sso_required' @> jsonb_build_array(provider_bigcorp),
    'Bob gets sso_required with both provider IDs'
  );
  return next is(
    jsonb_array_length(result->'claims'->'sso_required'),
    2,
    'Bob has exactly 2 provider IDs in sso_required'
  );

  -- Give Alice an SSO identity for bigcorpCo too — sso_required should disappear.
  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    (alice_id, 'sso', provider_bigcorp::text, '{}'::jsonb);

  select public.custom_access_token_hook(jsonb_build_object(
    'user_id', alice_id,
    'claims', jsonb_build_object('sub', alice_id)
  )) into result;

  return next ok(
    result->'claims'->'sso_required' is null,
    'Alice with both SSO identities has no sso_required claim'
  );

  -- Clean up the extra identity.
  delete from auth.identities
    where user_id = alice_id
      and provider_id = provider_bigcorp::text;

  -- User with only open-tenant grants: no sso_required.
  delete from user_grants where user_id = bob_id;
  insert into user_grants (user_id, object_role, capability) values
    (bob_id, 'openCo/', 'read');

  select public.custom_access_token_hook(jsonb_build_object(
    'user_id', bob_id,
    'claims', jsonb_build_object('sub', bob_id)
  )) into result;

  return next ok(
    result->'claims'->'sso_required' is null,
    'User with only open-tenant grants has no sso_required claim'
  );

  return;
end
$$ language plpgsql;
