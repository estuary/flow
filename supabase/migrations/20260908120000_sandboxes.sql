begin;

-- Sandboxes: per-user Linux VMs that run shell commands, hosted by a sandbox
-- provider (Fly.io Sprites today). The provider knows a sandbox only by its
-- handle. The control plane records here which user owns which handle, so a
-- handle carries no user identifier, and a user's sandbox can be replaced by a
-- fresh one under a new handle.

create table internal.sandboxes (
  id           public.flowid primary key,        -- defaults via the flowid domain
  user_id      uuid not null references auth.users (id),
  handle       text not null unique,
  catalog_name text not null,
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now(),
  deleted_at  timestamptz
);

comment on table internal.sandboxes is
  'Per-user sandboxes and the provider handle each one is addressed by. '
  'A row with deleted_at set is retired; its handle is never reused.';
comment on column internal.sandboxes.handle is
  'The provider''s name for the sandbox (the sprite name). Derived from id, '
  'never from the owner.';
comment on column internal.sandboxes.catalog_name is
  'Catalog name on which CreateSandbox was authorized at creation. '
  'Every operation addresses the sandbox by id; the provider never sees this name.';

-- Live-row lookups and the per-user limit count are per user. The control
-- plane enforces the limit itself when it inserts a record.
create index sandboxes_live_user_idx on internal.sandboxes (user_id)
  where deleted_at is null;

-- A catalog name tells one user's live sandboxes apart, so it is unique among them.
-- Two users may use the same catalog name, and a retired sandbox frees its catalog name.
create unique index sandboxes_live_user_catalog_name_idx on internal.sandboxes (user_id, catalog_name)
  where deleted_at is null;

commit;
