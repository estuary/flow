
-- publications are operations that publish a draft.
create table publications (
  like internal._model_async including all,

  user_id   uuid references auth.users(id) not null default auth.uid(),
  draft_id  flowid not null,
  dry_run   bool   not null default false
);
alter table publications enable row level security;

-- We don't impose a foreign key on drafts, because a publication
-- operation
-- audit log may stick around much longer than the draft does.
create policy "Users can access only their initiated publish operations"
  on publications as permissive for select
  using (user_id = auth.uid());
create policy "Users can insert publications from permitted drafts"
   on publications as permissive for insert
   with check (draft_id in (select id from drafts));

grant select on publications to authenticated;
grant insert (draft_id, dry_run) on publications to authenticated;


-- Published specifications which record the changes
-- made to specs over time, and power reverts.
create table publication_specs (
  pub_id flowid references publications(id) not null,
  catalog_name  catalog_name not null,
  primary key (catalog_name, pub_id),

  spec_type catalog_spec_type not null,
  -- spec_min_patch is a minimal delta of what actually changed,
  -- determined at time of publication by diffing the "before"
  -- and "after" document.
  spec_min_patch  jsonb not null,
  -- spec_rev_patch is like spec_fwd_patch but in reverse.
  -- A revert of a publication can be initialized by creating
  -- a draft having all of its publication_specs.spec_rev_patch
  spec_rev_patch  jsonb not null
);
alter table draft_specs enable row level security;

create policy "Users must be authorized to the specification catalog name"
  on publication_specs as permissive
  using (true); -- TODO(johnny) auth on catalog_name.
grant all on draft_specs to authenticated;


-- Live (current) specifications of the catalog.
create table live_specs (
  like internal._model including all,

  -- catalog_name is the conceptual primary key, but we use flowid as
  -- the literal primary key for consistency and join performance.
  catalog_name  catalog_name unique not null,

  -- `spec` is the models::${spec_type}Def specification which corresponds to `spec_type`.
  spec_type    catalog_spec_type not null,
  spec         jsonb,
  last_pub_id  flowid references publications(id) not null,

  -- reads_from and writes_to is the list of collections read
  -- or written by a task, or is null if not applicable to this
  -- specification type.
  -- We'll index these to efficiently retrieve connected components
  -- using recursive common table expression(s).
  reads_from text[],
  writes_to  text[],

  -- Image name and tag are extracted to make it easier
  -- to determine specs which are out of date w.r.t. the latest
  -- connector tag.
  connector_image_name  text,
  connector_image_tag   text
);
alter table live_specs enable row level security;

create policy "Users must be authorized to the specification catalog name"
  on live_specs as restrictive
  using (true); -- TODO(johnny) auth catalog_name.
grant all on live_specs to authenticated;


create view draft_specs_ext as
select
  draft_specs.*,
  live_specs.spec as live_spec,
  jsonb_merge_patch(
    coalesce(live_specs.spec, 'null'::jsonb),
    draft_specs.spec_patch
  ) as draft_spec,
  coalesce(
    jsonb_merge_diff(
      jsonb_merge_patch(
        coalesce(live_specs.spec, 'null'::jsonb),
        draft_specs.spec_patch
      ),
      live_specs.spec
    ),
    '{}'
  ) as spec_patch_min
from draft_specs
left outer join live_specs
  on draft_specs.catalog_name = live_specs.catalog_name
;

grant select on draft_specs_ext to authenticated;

comment on view draft_specs_ext is 'Extended draft specifications view';
comment on column draft_specs_ext.live_spec is
  'Live specification to be updated by this draft';
comment on column draft_specs_ext.draft_spec is
  'Fully patched draft specification which would be published by this draft';
comment on column draft_specs_ext.spec_patch_min is
  'Minimized effective patch of the live specification contained by this draft specification';