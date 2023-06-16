
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
grant update (detail) on drafts to authenticated;
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
  like internal._model including all,

  draft_id      flowid not null references drafts(id) on delete cascade,
  catalog_name  catalog_name not null,
  unique (draft_id, catalog_name),

  expect_pub_id   flowid default null,
  spec            json,
  spec_type       catalog_spec_type,
  built_spec      json,
  validated       json,

  constraint "spec and spec_type must be consistent" check (
    json_typeof(spec) is distinct from 'null' and (spec is null) = (spec_type is null)
  )
);
alter table draft_specs enable row level security;

create policy "Users access their draft specs"
  on draft_specs as permissive
  using (draft_id in (select id from drafts));
grant all on draft_specs to authenticated;

comment on table draft_specs is
  'Proposed catalog specifications of a draft';
comment on column draft_specs.draft_id is
  'Draft which this specification belongs to';
comment on column draft_specs.catalog_name is
  'Catalog name of this specification';
comment on column draft_specs.expect_pub_id is '
Draft specifications may be drawn from a current live specification,
and in this case it''s recommended that expect_pub_id is also set to the
last_pub_id of that inititializing live specification.

Or if there isn''t expected to be a live specification then
expect_pub_id can be the set to an explicit value of ''00:00:00:00:00:00:00:00''
to represent that no live specification is expected to exist.

Then when this draft is published, the publication will fail if the now-current
live specification has a different last_pub_id. This prevents inadvertent errors
where two users attempt to modify or create a catalog specification at the same time,
as the second user publication will fail rather than silently overwriting changes
made by the first user.

When NULL, expect_pub_id has no effect.
';
comment on column draft_specs.spec is '
Spec is a serialized catalog specification. Its schema depends on its spec_type:
either CollectionDef, CaptureDef, MaterializationDef, DerivationDef,
or an array of TestStep from the Flow catalog schema.

It may also be NULL, in which case `spec_type` must also be NULL
and the specification will be deleted when this draft is published.
';
comment on column draft_specs.spec_type is
  'Type of this draft catalog specification';
comment on column draft_specs.built_spec is
  'Built specification for this catalog';
comment on column draft_specs.validated is
  'Serialized response from the connector Validate RPC as populated by a dry run of this draft specification';
