-- Tests for check_sso_requirement hook that blocks social login
-- when the user's email domain matches an SSO-enforcing tenant.
create function tests.test_sso_access_token_hook()
returns setof text as $$
declare
  provider_acme uuid = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  provider_widgetly uuid = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';
  alice_id uuid = '11111111-1111-1111-1111-111111111111';
  bob_id uuid = '22222222-2222-2222-2222-222222222222';
  carol_id uuid = '33333333-3333-3333-3333-333333333333';
  result jsonb;
  base_event jsonb;
begin
  -- Setup: test users.
  insert into auth.users (id, email) values
    (alice_id, 'alice@acme.com'),
    (bob_id, 'bob@gmail.com'),
    (carol_id, 'carol@widgetly.io')
  on conflict (id) do update set email = excluded.email;

  -- Setup: SSO providers and domains.
  insert into auth.sso_providers (id) values (provider_acme), (provider_widgetly)
    on conflict do nothing;
  insert into auth.sso_domains (id, sso_provider_id, domain) values
    (gen_random_uuid(), provider_acme, 'acme.com'),
    (gen_random_uuid(), provider_widgetly, 'widgetly.io')
    on conflict do nothing;

  -- Tenants: acmeCo and widgetlyCo enforce SSO, openCo does not.
  insert into tenants (tenant, sso_provider_id, enforce_sso) values
    ('acmeCo/', provider_acme, true),
    ('widgetlyCo/', provider_widgetly, true),
    ('openCo/', null, false)
  on conflict (tenant) do update set
    sso_provider_id = excluded.sso_provider_id,
    enforce_sso = excluded.enforce_sso;

  -- Ensure all test users start as non-SSO.
  update auth.users set is_sso_user = false where id in (alice_id, bob_id, carol_id);

  -- =========================================================
  -- Case 1: Social user with matching domain → blocked
  -- =========================================================
  base_event = jsonb_build_object(
    'user_id', alice_id,
    'claims', jsonb_build_object('sub', alice_id)
  );

  select public.check_sso_requirement(base_event) into result;

  return next is(
    result->'error'->>'http_code', '403',
    'Social user with matching domain gets 403'
  );
  return next is(
    result->'error'->>'message', 'sso_required:acme.com',
    'Error message includes domain'
  );

  -- =========================================================
  -- Case 2: Social user on a different SSO domain → blocked with that domain
  -- =========================================================
  base_event = jsonb_build_object(
    'user_id', carol_id,
    'claims', jsonb_build_object('sub', carol_id)
  );

  select public.check_sso_requirement(base_event) into result;

  return next is(
    result->'error'->>'message', 'sso_required:widgetly.io',
    'Error message includes the users own domain, not a hardcoded one'
  );

  -- =========================================================
  -- Case 3: Social user with non-matching domain → allowed
  -- =========================================================
  base_event = jsonb_build_object(
    'user_id', bob_id,
    'claims', jsonb_build_object('sub', bob_id)
  );

  select public.check_sso_requirement(base_event) into result;

  return next ok(
    result->'error' is null,
    'Social user with non-matching domain is not blocked'
  );
  return next is(
    result->'claims'->>'sub', bob_id::text,
    'Non-matching domain user gets claims passed through'
  );

  -- =========================================================
  -- Case 4: SSO user with matching domain → allowed (token refresh)
  -- =========================================================
  update auth.users set is_sso_user = true where id = alice_id;

  base_event = jsonb_build_object(
    'user_id', alice_id,
    'claims', jsonb_build_object('sub', alice_id)
  );

  select public.check_sso_requirement(base_event) into result;

  return next ok(
    result->'error' is null,
    'SSO user with matching domain is not blocked'
  );

  -- =========================================================
  -- Case 5: enforce_sso = false → not blocked even with matching domain
  -- =========================================================
  update auth.users set is_sso_user = false where id = alice_id;
  update tenants set enforce_sso = false where tenant = 'acmeCo/';

  base_event = jsonb_build_object(
    'user_id', alice_id,
    'claims', jsonb_build_object('sub', alice_id)
  );

  select public.check_sso_requirement(base_event) into result;

  return next ok(
    result->'error' is null,
    'Social user with matching domain allowed when enforce_sso is false'
  );

  -- Restore for remaining tests.
  update tenants set enforce_sso = true where tenant = 'acmeCo/';

  -- =========================================================
  -- Case 6: Malformed user_id → structured 500 (fails closed)
  -- =========================================================
  select public.check_sso_requirement(jsonb_build_object(
    'user_id', 'not-a-uuid',
    'claims', jsonb_build_object('sub', 'bogus')
  )) into result;

  return next is(
    result->'error'->>'http_code', '500',
    'Malformed user_id returns 500'
  );
  return next ok(
    result->'error'->>'message' like 'check_sso_requirement failed:%',
    'Error message includes function name for observability'
  );

  return;
end
$$ language plpgsql;
