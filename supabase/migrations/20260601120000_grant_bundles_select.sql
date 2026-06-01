begin;

-- Migration 20260511120000 added the `bundles` column to user_grants and
-- role_grants with deliberately column-scoped grants that excluded `bundles`,
-- so PostgREST-facing roles could neither read nor write it.
--
-- That broke dashboard grant mutations: supabase-js issues insert/update/delete
-- with `Prefer: return=representation`, which PostgREST executes as
-- `... RETURNING *`. The `*` expands to every column, including `bundles`, and
-- `authenticated` lacks SELECT on it, so Postgres rejects the whole statement
-- with `42501 permission denied for table ...`.
--
-- Grant SELECT (only) on `bundles` so representation works. Writes remain
-- blocked: there is still no INSERT/UPDATE column grant on `bundles`, so
-- PostgREST-facing roles can read but cannot set it.

grant select (bundles) on user_grants to authenticated;
grant select (bundles) on role_grants to authenticated;

commit;
