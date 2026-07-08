-- Tests for scoped CI access tokens (20260608120000_scoped_ci_tokens.sql).
-- Covers: the unchanged null-pg_role path, the scoped round trip ending in a
-- connector_tag re-queue, the admin/allowlist guards on create_scoped_refresh_token,
-- and that the scoped roles are assumable by `authenticator` (the PostgREST path).

-- Decode the (unverified) claims of a JWT's payload segment. The payload is
-- base64url, so map back to standard base64 and pad before decoding.
create function tests.jwt_claims(token text) returns jsonb as $$
  select convert_from(
    decode(
      rpad(
        translate(split_part(token, '.', 2), '-_', '+/'),
        ((length(split_part(token, '.', 2)) + 3) / 4) * 4,
        '='
      ),
      'base64'
    ),
    'utf8'
  )::jsonb;
$$ language sql;


create function tests.test_generate_access_token_null_role_unchanged()
returns setof text as $$
declare
  rt_response jsonb;
  response json;
  claims jsonb;
begin
  delete from refresh_tokens;

  -- A refresh token created the normal way has a null pg_role, so the emitted
  -- claims must be exactly as before this migration.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');
  select create_refresh_token(true, '1 day', 'null role') into rt_response;
  select generate_access_token((rt_response->>'id')::flowid, rt_response->>'secret') into response;

  -- jwt_claims lives in the `tests` schema, which `authenticated` cannot reach.
  set role postgres;
  claims := tests.jwt_claims(response->>'access_token');
  return query select is(claims->>'role', 'authenticated', 'null pg_role still yields role=authenticated');
  return query select is(claims->>'aud', 'authenticated', 'aud claim unchanged');
  return query select is(claims->>'sub', '11111111-1111-1111-1111-111111111111', 'sub is the token user');
end;
$$ language plpgsql;


create function tests.test_scoped_refresh_token_roundtrip()
returns setof text as $$
declare
  rt_response jsonb;
  response json;
  claims jsonb;
  requeued integer;
begin
  delete from refresh_tokens;

  -- support@estuary.dev is an ops/ admin in the seed fixtures.
  perform set_authenticated_context('ffffffff-ffff-ffff-ffff-ffffffffffff');
  select create_scoped_refresh_token('github_action_connector_refresh', '1 day', 'ci') into rt_response;

  return query select ok(rt_response ? 'id', 'scoped refresh token response has id');
  return query select ok(rt_response ? 'secret', 'scoped refresh token response has secret');

  -- The minted access token must carry the scoped role claim.
  select generate_access_token((rt_response->>'id')::flowid, rt_response->>'secret') into response;
  set role postgres;
  claims := tests.jwt_claims(response->>'access_token');
  return query select is(claims->>'role', 'github_action_connector_refresh', 'access token carries the scoped role');

  -- And the scoped role can re-queue a connector tag via the new RPC.
  set role postgres;
  insert into connectors (id, image_name, title, short_description, logo_url, external_url, recommended) values
    ('12:34:56:78:87:65:43:22', 'ghcr.io/estuary/scoped-test', '{"en-US":"t"}', '{"en-US":"d"}', '{"en-US":"l"}', 'http://foo.test', true);
  insert into connector_tags (connector_id, image_tag, job_status) values
    ('12:34:56:78:87:65:43:22', ':v1', '{"type": "success"}');

  -- The test runner connects as the non-superuser `postgres`, which is not a
  -- member of the scoped role; grant membership (rolled back with the test) so
  -- we can assume it, as `authenticator` does over PostgREST in production.
  -- Grant to the explicit role name, not `current_user`: `grant ... to current_user`
  -- followed by `set role` segfaults the backend (a Postgres role-cache bug).
  grant github_action_connector_refresh to postgres;
  set role github_action_connector_refresh;
  select requeue_connector_tag('ghcr.io/estuary/scoped-test', array[':v1', ':dev']) into requeued;
  set role postgres;

  return query select is(requeued, 1, 'requeue_connector_tag updated one matching tag');
  return query select results_eq(
    $i$ select job_status->>'type' from connector_tags
        where connector_id = '12:34:56:78:87:65:43:22' and image_tag = ':v1' $i$,
    $i$ values ('queued') $i$,
    'connector_tag job_status was set to queued'
  );
end;
$$ language plpgsql;


create function tests.test_create_scoped_refresh_token_guards()
returns setof text as $$
begin
  -- A non-admin (Alice) cannot mint a scoped token.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');
  prepare not_admin as select create_scoped_refresh_token('github_action_connector_refresh', '1 day', 'x');
  return query select throws_like('not_admin', '%admin of the ops/ tenant%', 'non-admin cannot create a scoped token');
  deallocate not_admin;

  -- Even an ops/ admin cannot target a role outside the allowlist.
  perform set_authenticated_context('ffffffff-ffff-ffff-ffff-ffffffffffff');
  prepare bad_role as select create_scoped_refresh_token('service_role', '1 day', 'x');
  return query select throws_like('bad_role', '%not an allowlisted scoped role%', 'allowlist blocks arbitrary target roles');
  deallocate bad_role;
end;
$$ language plpgsql;


create function tests.test_scoped_roles_authenticator_membership()
returns setof text as $$
begin
  -- PostgREST logs in as `authenticator` and SET ROLEs to the token's role claim,
  -- which only works if authenticator is a member of the scoped role.
  return query select ok(
    pg_has_role('authenticator', 'github_action_connector_refresh', 'member'),
    'authenticator can assume github_action_connector_refresh');
  return query select ok(
    pg_has_role('authenticator', 'data_plane_releases_ci', 'member'),
    'authenticator can assume data_plane_releases_ci');
end;
$$ language plpgsql;
