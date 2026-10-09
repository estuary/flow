begin;

-- These records are accessible only to the control plane, never through RLS or
-- PostgREST. A request is a short-lived capability handed to the login/consent UI.
create table internal.oauth_requests (
    id uuid primary key default gen_random_uuid(),
    client_id text not null,
    client_name text not null,
    redirect_uri text not null,
    resource text not null,
    scope text not null check (scope = 'estuary'),
    state text,
    challenge text not null,
    expires_at timestamptz not null default (now() + interval '10 minutes'),
    user_id uuid references auth.users(id) on delete cascade,
    decision boolean,
    code_hash bytea unique check (octet_length(code_hash) = 32),
    code_expires_at timestamptz,
    check (decision is null or user_id is not null),
    check ((code_hash is null) = (code_expires_at is null)),
    check (code_hash is null or decision is true)
);
create index on internal.oauth_requests (expires_at);

create index on internal.oauth_requests (code_expires_at) where code_hash is not null;

create function internal.cleanup_oauth_requests() returns void
language sql set search_path = pg_catalog, internal as $$
    -- Preserve the consent decision so cleanup cannot permit another approval.
    update internal.oauth_requests set code_hash = null, code_expires_at = null
        where code_hash is not null and code_expires_at <= now();
    delete from internal.oauth_requests where expires_at <= now();
$$;

revoke all on internal.oauth_requests from public, anon, authenticated, service_role;
revoke all on function internal.cleanup_oauth_requests() from public, anon, authenticated, service_role;

commit;
