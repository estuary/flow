/*
create function tests.test_create_refresh_token()
returns setof text as $$
declare
  response json;
begin
  delete from refresh_tokens;
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

  select create_refresh_token(true, '1 day', 'test detail') into rt_response;
  select * into rt from refresh_tokens;

  -- Multi-use refresh_token used for this one, so expect only an access_token
  -- in response. Expect an update to `updated_at` and `uses` of the
  -- refresh_token
  select generate_access_token((rt_response->>'id')::flowid, rt_response->>'secret') into response;

  return query select ok(response::jsonb ? 'access_token', 'generate_access_token response has access_token');
  return query select ok(not (response::jsonb ? 'refresh_token'), 'generate_access_token response does not have refresh_token (multi-use)');

  select * into new_rt from refresh_tokens;
  return query select is(new_rt.uses, 1, 'refresh_tokens uses bumped');
  return query select is(new_rt.hash, rt.hash, 'refresh_token hash unchanged (multi-use)');
  return query select ok(rt.updated_at < (select updated_at from refresh_tokens), 'refresh_tokens updated_at bumped');

  -- Single-use refresh_token used for this one, so expect an access_token
  -- and a new refresh_token secret in response. Expect an update to `updated_at` and `uses` of the
  -- refresh_token
  update refresh_tokens set multi_use = false;

  select generate_access_token((rt_response->>'id')::flowid, rt_response->>'secret') into response;

  return query select ok(response::jsonb ? 'access_token', 'generate_access_token response has access_token');
  return query select ok(response::jsonb ? 'refresh_token', 'generate_access_token response has refresh_token');
  return query select ok(response::jsonb->'refresh_token' ? 'id', 'generate_access_token response has refresh_token.id');
  return query select ok(response::jsonb->'refresh_token' ? 'secret', 'generate_access_token response has refresh_token.id');

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

  select create_refresh_token(true, '1 day', 'test detail') into rt_response;
  select * into rt from refresh_tokens;

  -- wrong id provided
  prepare wrong_id as select generate_access_token('00:00:00:00:00:00:00:00'::flowid, 'test');
  return query select throws_like('wrong_id', 'could not find refresh_token with the given `refresh_token_id`');

  -- wrong secret provided
  prepare wrong_secret as select generate_access_token($1, 'test');
  return query select throws_like('EXECUTE wrong_secret(''' || rt.id || ''')', 'invalid secret provided');

  -- wrong secret provided
  update refresh_tokens set updated_at = now() - (interval '1 day 1 second');
  prepare expired as select generate_access_token($1, $2);
  return query select throws_like('EXECUTE expired(''' || rt.id || ''', ''' || (rt_response->>'secret') || ''')', 'refresh_token has expired.');

end;
$$ language plpgsql;
*/