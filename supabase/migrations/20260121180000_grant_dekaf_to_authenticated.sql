begin;

-- These got dropped from the most recent migration rollup
--
-- Guarded to the primary `postgres` database: roles are cluster-global, so
-- these GRANT / ALTER ROLE statements mutate the shared pg_auth_members /
-- pg_authid catalogs. `#[sqlx::test]` isolates each test in its own database
-- but runs them concurrently, and every test re-applies the full migration
-- set; N concurrent runs updating the same `dekaf` tuple abort all but one
-- with "tuple concurrently updated". Test databases inherit the cluster-wide
-- roles from the `postgres` run and don't need these. See 00_polyfill.sql.
do $$
begin
    if current_database() = 'postgres' then
        grant dekaf to authenticator;
        alter role dekaf nologin bypassrls;
    end if;
end
$$;

commit;
