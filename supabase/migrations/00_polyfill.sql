begin;

DO $$
BEGIN
    -- Are we applying migrations for supabase?
    -- In this case, we need to create expected roles that will be referenced
    -- in the compacted schema.
    IF current_database() = 'postgres' THEN

        -- Roles which are created by supabase: anon, authenticated, supabase_admin, service_role.
        create role data_plane_releases_ci;
        create role dekaf;
        create role gatsby_reader;
        create role github_action_connector_refresh;
        create role marketplace_integration;
        create role reporting_user;
        create role stats_loader with login password 'stats_loader_password' bypassrls;
        create role wgd_automation;

        -- Enable pg_cron.
        create extension pg_cron with schema pg_catalog;
        grant usage on schema cron to postgres;
        grant all privileges on all tables in schema cron to postgres;

    ELSE
        -- We're applying migrations for a sqlx::test.
        -- Roles are cluster-wide and already exist from migrations applied to the
        -- primary `postgres` database. We only need to stub Supabase schemas.

        -- Create auth schema with minimal users table stub.
        create schema auth;
        create table auth.users (
            id uuid primary key,
            email text,
            is_sso_user boolean,
            raw_user_meta_data jsonb
        );

        -- Stub for auth.uid() function.
        create function auth.uid() returns uuid as $uid$
            select null::uuid;
        $uid$ language sql stable;

    END IF;
END
$$;

-- Required for postgres to give ownership of catalog_stats to stats_loader.
grant stats_loader to postgres;

-- Required for stats materialization to create flow_checkpoints_v1 and flow_materializations_v2.
grant create on schema public to stats_loader;

-- TODO(johnny): Required for `authenticated` to own `drafts_ext` and `publication_specs_ext`.
-- We should make them owed by postgres and grant usage instead.
grant create on schema public to authenticated;

-- The production database has a Flow materialization of Stripe customer data.
-- This is a partial table which matches the portions we use today.
create schema stripe;

create table stripe.customers (
    id text primary key,
    address json,
    "address/city" text,
    "address/country" text,
    "address/line1" text,
    "address/line2" text,
    "address/postal_code" text,
    "address/state" text,
    balance bigint,
    created bigint,
    currency text,
    default_source text,
    delinquent boolean,
    description text,
    email text,
    invoice_prefix text,
    invoice_settings json,
    "invoice_settings/custom_fields" json,
    "invoice_settings/default_payment_method" text,
    metadata json,
    name text,
    phone text,
    flow_document json not null
);

grant usage on schema stripe to postgres;

commit;