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
  updated_at   timestamptz not null default now()
);

comment on table internal.sandboxes is
  'Per-user sandboxes and the provider handle each one is addressed by. '
  'Rows are deleted after the provider confirms deletion.';
comment on column internal.sandboxes.handle is
  'The provider''s name for the sandbox (the sprite name). Derived from id, '
  'never from the owner.';
comment on column internal.sandboxes.catalog_name is
  'Catalog name on which CreateSandbox was authorized at creation. '
  'GraphQL operations address the sandbox by catalog name; the provider never sees this name.';

-- Sandbox listings are scoped to the owner.
create index sandboxes_user_idx on internal.sandboxes (user_id);

create unique index sandboxes_catalog_name_idx on internal.sandboxes (catalog_name);

commit;
