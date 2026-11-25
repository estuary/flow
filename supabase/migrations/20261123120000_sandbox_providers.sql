begin;

-- A sandbox is one provider machine for the whole life of its record.
-- Commands run through `flowctl raw sandbox-exec` on that machine.

alter table internal.sandboxes
  add column provider text,
  add column provider_state jsonb,
  add column gone_at timestamptz;

-- The machines of existing sandboxes are Sprites without the baseline which
-- runs `flowctl raw sandbox-exec`, so every existing sandbox is gone.
-- Deleting one still destroys its sprite.
update internal.sandboxes set
  provider = 'sprites',
  provider_state = '{}',
  gone_at = now(),
  updated_at = now();

alter table internal.sandboxes
  alter column provider set not null,
  drop column baseline_checkpoint_id,
  add constraint sandboxes_gone_provisioned
    check (gone_at is null or provider_state is not null);

comment on column internal.sandboxes.provider is
  'Kind of the provider which hosts the sandbox machine, such as sprites or docker.';
comment on column internal.sandboxes.provider_state is
  'Provider-assigned identifiers of the machine. NULL while it is provisioned; '
  'its presence indicates that the machine was provisioned.';
comment on column internal.sandboxes.gone_at is
  'When the provider reported that the machine no longer exists. It is final: '
  'the record keeps its catalog name reserved until the sandbox is deleted.';

-- Credentials which a user approved for commands of a sandbox.
-- Tokens are minted from an approval when a command is run.
create table internal.sandbox_credentials (
  sandbox_id      public.flowid not null references internal.sandboxes (id) on delete cascade,
  workspace       text not null,
  name            text not null,
  capability_mask text[] not null,
  prefix_mask     text[] not null,
  approved_by     uuid not null references auth.users (id),
  created_at      timestamptz not null default now(),
  expires_at      timestamptz not null,
  primary key (sandbox_id, workspace, name)
);

comment on table internal.sandbox_credentials is
  'Approvals of credentials for the commands of a sandbox workspace.';
comment on column internal.sandbox_credentials.workspace is
  'Workspace of the sandbox whose commands receive the credential.';
comment on column internal.sandbox_credentials.name is
  'Name of the credential within its workspace.';
comment on column internal.sandbox_credentials.capability_mask is
  'Capabilities to which minted tokens are restricted.';
comment on column internal.sandbox_credentials.prefix_mask is
  'Catalog prefixes to which minted tokens are restricted.';
comment on column internal.sandbox_credentials.approved_by is
  'User who approved the credential.';
comment on column internal.sandbox_credentials.expires_at is
  'When the approval lapses. No token is minted from it afterwards.';

commit;
