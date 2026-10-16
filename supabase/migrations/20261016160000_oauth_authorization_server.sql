begin;

-- OAuth secrets are high-entropy opaque values. Only their SHA-256 digests
-- are retained; none of these tables is exposed through PostgREST.
create table internal.oauth_requests (
    request_hash bytea primary key,
    client_id text not null,
    client_name text not null,
    redirect_uri text not null,
    client_state text,
    code_challenge text not null,
    resource text not null,
    scope text not null,
    expires_at timestamptz not null default now() + interval '10 minutes'
);
create index on internal.oauth_requests (expires_at);

create table internal.oauth_grants (
    id uuid primary key,
    user_id uuid not null references auth.users(id) on delete cascade,
    client_id text not null,
    client_name text not null,
    resource text not null,
    scope text not null,
    created_at timestamptz not null default now(),
    expires_at timestamptz not null default now() + interval '90 days',
    revoked_at timestamptz
);
create index on internal.oauth_grants (user_id);
create index on internal.oauth_grants (expires_at);

create table internal.oauth_codes (
    code_hash bytea primary key,
    grant_id uuid not null references internal.oauth_grants(id) on delete cascade,
    redirect_uri text not null,
    code_challenge text not null,
    expires_at timestamptz not null default now() + interval '60 seconds',
    used boolean not null default false
);
create index on internal.oauth_codes (expires_at);

-- Spent refresh hashes are retained until grant expiry to detect reuse and
-- revoke the whole grant, including the successor held by a competing caller.
create table internal.oauth_refresh_tokens (
    token_hash bytea primary key,
    grant_id uuid not null references internal.oauth_grants(id) on delete cascade,
    used boolean not null default false
);
create index on internal.oauth_refresh_tokens (grant_id);

alter table internal.oauth_requests enable row level security;
alter table internal.oauth_grants enable row level security;
alter table internal.oauth_codes enable row level security;
alter table internal.oauth_refresh_tokens enable row level security;

commit;
