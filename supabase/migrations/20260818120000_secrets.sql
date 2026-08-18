begin;

-- First-class secrets: values which a task's connector configuration
-- references by catalog name, rather than embedding as ciphertext of its own.
--
-- Rows hold the sops-wrapped document produced by config-encryption, which is
-- opaque to the control plane: only config-encryption holds the KMS grants to
-- decrypt it, and the control plane cannot verify its MAC. Documents are read
-- and written exclusively through control-plane-api, which is why this table
-- lives in `internal` (no PostgREST exposure) and carries no RLS policies.
create table internal.secrets (
  catalog_name  public.catalog_name primary key,
  -- `id` is the secret's lifecycle identity: each distinct wrapped document is
  -- a distinct entity, and flowids are time-ordered so `id` also answers which
  -- of two observations is newer. Code and APIs outside this table call it
  -- `secret_id`. There is deliberately no `updated_at` and no version counter.
  id            public.flowid unique not null default internal.id_generator(),
  -- `json` and not `jsonb`: sops verifies its MAC by traversing the document in
  -- order, and `jsonb` would normalize key order and break verification.
  document      json not null
);

comment on table internal.secrets is
  'Wrapped secret documents, referenced by catalog name from the `secrets` stanza of a task.';

comment on column internal.secrets.id is
  'Lifecycle identity of the current document. Any change to the document mints a new id.';

comment on column internal.secrets.document is
  'The sops-wrapped document, opaque to the control plane. Stored as `json` to preserve key order for MAC verification.';

create index secrets_catalog_name_spgist on internal.secrets
  using spgist ((catalog_name::text));

commit;
