-- Tests for scope-confined refresh tokens (20260805120000_scoped_refresh_tokens.sql).
-- Covers that generate_access_token stamps a non-null scope_prefix into the
-- `scope_prefix` claim, and that a null scope_prefix leaves the claims as they were.
--
-- The Rust side resolves this claim into a `tables::AuthScope`; the narrowing it
-- performs is covered by unit tests in `crates/tables` and `control-plane-api`.
-- What can only be checked here is that the SQL actually emits the claim, since
-- `sign()` needs pgjwt and the vault-held JWT secret, neither of which exists in
-- the `sqlx::test` databases the Rust tests run against.

-- Decode the (unverified) claims of a JWT's payload segment. The payload is
-- base64url, so map back to standard base64 and pad before decoding.
create function tests.scope_jwt_claims(token text) returns jsonb as $$
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


create function tests.test_access_token_omits_scope_when_unscoped()
returns setof text as $$
declare
  rt_response jsonb;
  response json;
  claims jsonb;
begin
  delete from refresh_tokens;

  -- A refresh token created the normal way has a null scope_prefix, so the
  -- emitted claims must carry no `scope_prefix` at all: an absent claim is what
  -- the Rust side reads as "unscoped", and an explicit null would be a distinct
  -- (and unhandled) shape.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');
  select create_refresh_token(true, '1 day', 'unscoped') into rt_response;
  select generate_access_token((rt_response->>'id')::flowid, rt_response->>'secret') into response;

  -- scope_jwt_claims lives in the `tests` schema, which `authenticated` cannot reach.
  set role postgres;
  claims := tests.scope_jwt_claims(response->>'access_token');

  return query select ok(not (claims ? 'scope_prefix'), 'unscoped token carries no scope_prefix claim');
  return query select is(claims->>'role', 'authenticated', 'role claim unchanged');
  return query select is(claims->>'aud', 'authenticated', 'aud claim unchanged');
  return query select is(claims->>'sub', '11111111-1111-1111-1111-111111111111', 'sub is the token user');
end;
$$ language plpgsql;


create function tests.test_access_token_carries_scope_prefix()
returns setof text as $$
declare
  rt refresh_tokens;
  rt_response jsonb;
  response json;
  claims jsonb;
begin
  delete from refresh_tokens;

  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');
  select create_refresh_token(true, '1 day', 'scoped') into rt_response;

  -- The GraphQL mutation sets this column at insert time, after checking that the
  -- caller can read the prefix. Set it directly here: this test covers the claim
  -- stamping, not the mutation's authorization gate.
  set role postgres;
  update refresh_tokens set scope_prefix = 'aliceCo/' where id = (rt_response->>'id')::flowid;

  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');
  select generate_access_token((rt_response->>'id')::flowid, rt_response->>'secret') into response;

  set role postgres;
  claims := tests.scope_jwt_claims(response->>'access_token');

  return query select is(claims->>'scope_prefix', 'aliceCo/', 'access token carries the scope_prefix claim');
  return query select is(claims->>'sub', '11111111-1111-1111-1111-111111111111', 'sub is still the token user');
  return query select is(claims->>'role', 'authenticated', 'a scope does not change the Postgres role');

  -- The scope rides on the row, so every exchange of this credential is scoped:
  -- whoever holds the secret cannot mint an unscoped token from it.
  select * into rt from refresh_tokens where id = (rt_response->>'id')::flowid;
  return query select is(rt.scope_prefix::text, 'aliceCo/', 'scope_prefix persists across exchange');
end;
$$ language plpgsql;


create function tests.test_scope_prefix_rejects_malformed_prefix()
returns setof text as $$
begin
  delete from refresh_tokens;

  -- The column is a catalog_prefix, so the domain rejects a name that is not a
  -- prefix (no trailing slash) even if the application layer were to miss it.
  set role postgres;
  prepare bad_prefix as
    insert into refresh_tokens (user_id, multi_use, valid_for, hash, scope_prefix)
    values ('11111111-1111-1111-1111-111111111111', true, '1 day', 'x', 'aliceCo');
  return query select throws_ok('bad_prefix', '23514',
    null, 'catalog_prefix domain rejects a non-prefix scope');
  deallocate bad_prefix;
end;
$$ language plpgsql;
