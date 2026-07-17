create function tests.test_create_refresh_token()
returns setof text as $$
declare
  response json;
begin
  delete from refresh_tokens;

  -- Drop priviledge to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  select create_refresh_token(false, '1 day', 'test detail') into response;

  return query select results_eq(
    $i$ select multi_use, valid_for, detail, uses, user_id from refresh_tokens $i$,
    $i$ values (false, interval '1 day', 'test detail', 0, '11111111-1111-1111-1111-111111111111'::uuid) $i$,
    'initial refresh_token created'
  );

  return query select ok(response::jsonb ? 'id', 'refresh_token response has id');
  return query select ok(response::jsonb ? 'secret', 'refresh_token response has secret');
end;
$$ language plpgsql;


create function tests.test_generate_access_token()
returns setof text as $$
declare
  rt refresh_tokens;
  new_rt refresh_tokens;
  rt_response jsonb;
  response json;
begin
  delete from refresh_tokens;

  -- Drop priviledge to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  select create_refresh_token(true, '1 day', 'test detail') into rt_response;

  set role postgres;
  select * into rt from refresh_tokens;

  -- Drop priviledge to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  -- Multi-use refresh_token used for this one, so expect only an access_token
  -- in response. Expect an update to `updated_at` and `uses` of the
  -- refresh_token
  select generate_access_token((rt_response->>'id')::flowid, rt_response->>'secret') into response;

  return query select ok(response::jsonb ? 'access_token', 'generate_access_token response has access_token');
  return query select ok(not (response::jsonb ? 'refresh_token'), 'generate_access_token response does not have refresh_token (multi-use)');

  set role postgres;
  select * into new_rt from refresh_tokens;
  return query select is(new_rt.uses, 1, 'refresh_tokens uses bumped');
  return query select is(new_rt.hash, rt.hash, 'refresh_token hash unchanged (multi-use)');
  return query select ok(rt.updated_at < (select updated_at from refresh_tokens), 'refresh_tokens updated_at bumped');

  -- Drop priviledge to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  -- Single-use refresh_token used for this one, so expect an access_token
  -- and a new refresh_token secret in response. Expect an update to `updated_at` and `uses` of the
  -- refresh_token
  update refresh_tokens set multi_use = false;

  select generate_access_token((rt_response->>'id')::flowid, rt_response->>'secret') into response;

  return query select ok(response::jsonb ? 'access_token', 'generate_access_token response has access_token');
  return query select ok(response::jsonb ? 'refresh_token', 'generate_access_token response has refresh_token');
  return query select ok(response::jsonb->'refresh_token' ? 'id', 'generate_access_token response has refresh_token.id');
  return query select ok(response::jsonb->'refresh_token' ? 'secret', 'generate_access_token response has refresh_token.id');

  set role postgres;
  select * into new_rt from refresh_tokens;

  return query select is(new_rt.uses, 2, 'refresh_tokens uses bumped');
  return query select isnt(new_rt.hash, rt.hash, 'refresh_token hash changed');
  return query select ok(rt.updated_at < (select updated_at from refresh_tokens), 'refresh_tokens updated_at bumped');
end;
$$ language plpgsql;

create function tests.test_generate_access_token_errors()
returns setof text as $$
declare
  rt refresh_tokens;
  rt_response jsonb;
begin
  delete from refresh_tokens;

  -- Drop priviledge to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  select create_refresh_token(true, '1 day', 'test detail') into rt_response;

  set role postgres;
  select * into rt from refresh_tokens;

  -- Drop priviledge to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  -- wrong id provided
  prepare wrong_id as select generate_access_token('00:00:00:00:00:00:00:00'::flowid, 'test');
  return query select throws_like('wrong_id', 'could not find refresh_token with the given `refresh_token_id`');

  -- wrong secret provided
  prepare wrong_secret as select generate_access_token($1, 'test');
  return query select throws_like('EXECUTE wrong_secret(''' || rt.id || ''')', 'invalid secret provided');

  -- wrong secret provided
  set role postgres;
  update refresh_tokens set updated_at = now() - (interval '1 day 1 second');

  -- Drop priviledge to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');
  prepare expired as select generate_access_token($1, $2);
  return query select throws_like('EXECUTE expired(''' || rt.id || ''', ''' || (rt_response->>'secret') || ''')', 'refresh_token has expired.');

end;
$$ language plpgsql;

-- The GraphQL createRefreshToken/revokeRefreshToken mutations already reject a
-- service-account caller (verify_not_service_account), but the same table is
-- reachable through the SECURITY DEFINER create_refresh_token RPC over
-- PostgREST. Without this guard a service account could mint itself a fresh
-- refresh token there, escaping the admin-only createApiKey flow and the
-- admin-chosen expiry. Guard fires before the function's insert, so the test
-- stays within pgTAP's rolled-back transaction.
create function tests.test_service_account_cannot_self_mint_refresh_token()
returns setof text as $$
declare
  sa_user_id uuid := 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
begin
  -- Seed a service-account identity: an auth.users row and its
  -- internal.service_accounts anchor, minted by Alice.
  set role postgres;
  insert into auth.users (id, email) values (sa_user_id, 'sa-bot@example.test');
  insert into internal.service_accounts (user_id, catalog_name, created_by)
    values (sa_user_id, 'aliceCo/ci-bot', '11111111-1111-1111-1111-111111111111');

  -- Authorize as the service account and attempt to self-mint.
  perform set_authenticated_context(sa_user_id);
  prepare sa_self_mint as select create_refresh_token(true, '1 day', 'self mint');
  return query select throws_like(
    'sa_self_mint',
    '%service accounts cannot mint their own refresh tokens%',
    'a service account cannot self-mint a refresh token via create_refresh_token'
  );
  deallocate sa_self_mint;
end;
$$ language plpgsql;
