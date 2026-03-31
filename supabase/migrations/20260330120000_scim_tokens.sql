-- Partial SCIM deprovisioning: token table for authenticating SCIM requests.
--
-- Each row maps a hashed bearer token to a tenant. The control-plane-api
-- hashes the incoming Authorization header and looks up the row to identify
-- which tenant the SCIM client is acting on behalf of.
--
-- Multiple tokens per tenant are supported for zero-downtime key rotation:
-- create new token → configure IdP → delete old token.

begin;

create table internal.scim_tokens (
  id          uuid primary key default gen_random_uuid(),
  tenant_id   flowid not null references tenants(id),
  token_hash  text not null,           -- SHA-256 hex digest of plaintext bearer token
  label       text,                    -- optional human-readable label (e.g. "Okta prod")
  created_at  timestamptz not null default now()
);

-- Fast lookup by hash on every SCIM request.
create unique index on internal.scim_tokens (token_hash);

comment on table internal.scim_tokens is
  'Bearer tokens for SCIM API authentication, hashed with SHA-256. Each token is scoped to a tenant.';

commit;
