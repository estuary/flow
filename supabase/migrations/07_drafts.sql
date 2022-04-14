
-- Draft changesets of Flow specifications.
create table drafts (
  like internal._model including all,

  user_id uuid references auth.users(id) not null default auth.uid()
);
alter table drafts enable row level security;

create policy "Users can access only their created drafts"
  on drafts as permissive
  using (user_id = auth.uid());

grant insert (detail) on drafts to authenticated;
grant select on drafts to authenticated;
grant delete on drafts to authenticated;

comment on table drafts is
  'Draft change-sets of Flow catalog specifications';
comment on column drafts.user_id is
  'User who owns this draft';

create index idx_drafts_user_id on drafts(user_id);


-- Errors encountered within user drafts
create table draft_errors (
  draft_id  flowid not null references drafts(id) on delete cascade,
  scope     text not null,
  detail    text not null
);
alter table draft_errors enable row level security;

create policy "Users can access and delete errors of their drafts"
  on draft_errors as permissive
  using (draft_id in (select id from drafts));
grant select, delete on draft_errors to authenticated;

comment on table draft_errors is
  'Errors found while validating, testing or publishing a user draft';
comment on column draft_errors.draft_id is
  'Draft which produed this error';
comment on column draft_errors.scope is
  'Location scope of the error within the draft';
comment on column draft_errors.detail is
  'Description of the error';

create index idx_draft_errors_draft_id on draft_errors(draft_id);


-- Draft specifications which the user is working on.
create table draft_specs (
  draft_id      flowid not null references drafts(id) on delete cascade,
  catalog_name  catalog_name not null,
  primary key (draft_id, catalog_name),

  spec_type     catalog_spec_type not null,
  -- spec_patch is a partial JSON patch of a models::${spec_type}Def specification,
  -- which may be patched into a live_specs.spec (which is always a fully-reduced spec).
  --
  -- Note this also covers deletion! According to the
  -- JSON merge patch RFC, deletion is expressed as a `null`
  -- value within a patch, so a patch consisting only of
  -- `null` is a semantic deletion of the entire specification.
  spec_patch    jsonb not null
);
alter table draft_specs enable row level security;

create policy "Users can access all specifications of their drafts"
  on draft_specs as permissive
  using (draft_id in (select id from drafts));
create policy "Users must be authorized to the specification catalog name"
  on draft_specs as restrictive
  using (true); -- TODO(johnny) auth catalog_name.
grant all on draft_specs to authenticated;

-- TODO - comments