-- Enables the SQL access-token mint (`public.generate_access_token`) inside
-- a `#[sqlx::test]` database, where the real signing dependencies don't
-- exist: `internal.access_token_jwt_secret()` reads `vault.decrypted_secrets`
-- and `sign()` comes from the pgjwt extension, and `00_polyfill.sql` stubs
-- neither (see the coverage note in `graphql/refresh_tokens.rs`).
--
-- The secret must match the harness key `test_server::build_app` hands the
-- App, so that SQL-minted tokens verify against real Envelope extraction.
-- `sign()` is a minimal HS256 implementation over pgcrypto's `hmac()`
-- (already installed into `public` by `00_polyfill.sql`); its correctness is
-- proven by the tokens it signs passing that verification.
create or replace function internal.access_token_jwt_secret() returns text
language sql stable as $$
  select 'test-jwt-secret-for-integration-tests';
$$;

-- `translate` both maps base64 to the url-safe alphabet and strips the
-- padding and line breaks `encode(..., 'base64')` inserts.
create function public.jwt_url_encode(data bytea) returns text
language sql immutable as $$
  select translate(encode(data, 'base64'), E'+/=\n', '-_');
$$;

create function public.sign(payload json, secret text) returns text
language sql immutable as $$
  with parts as (
    select public.jwt_url_encode(convert_to('{"alg":"HS256","typ":"JWT"}', 'utf8'))
      || '.' || public.jwt_url_encode(convert_to(payload::text, 'utf8')) as signable
  )
  select signable || '.' || public.jwt_url_encode(public.hmac(signable, secret, 'sha256'))
  from parts;
$$;
