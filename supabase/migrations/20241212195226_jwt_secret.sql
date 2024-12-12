
begin;

select vault.create_secret('super-secret-jwt-token-with-at-least-32-characters-long', 'app.jwt_secret', 'The jwt secret');

CREATE OR REPLACE FUNCTION internal.access_token_jwt_secret() RETURNS text
    LANGUAGE sql STABLE
    AS $$

    select decrypted_secret
       from vault.decrypted_secrets
       where name = 'app.jwt_secret';
$$;

commit;
