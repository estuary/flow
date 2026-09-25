begin;

-- Sandboxes: per-user Linux VMs that run shell commands, hosted by a sandbox
-- provider (Fly.io Sprites today).

create table internal.sandboxes (
  id           public.flowid primary key,        -- defaults via the flowid domain
  user_id      uuid not null references auth.users (id),
  handle       text not null unique,
  catalog_name text not null,
  baseline_checkpoint_id text,
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now()
);

comment on column internal.sandboxes.baseline_checkpoint_id is
  'Provider checkpoint restored on reset. NULL until provisioning completes; '
  'its presence indicates that the sandbox is ready.';

create index sandboxes_user_idx on internal.sandboxes (user_id);

create unique index sandboxes_catalog_name_idx on internal.sandboxes (catalog_name);

commit;
