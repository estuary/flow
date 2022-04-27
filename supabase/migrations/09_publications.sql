
-- publications are operations that publish a draft.
create table publications (
  like internal._model_async including all,

  user_id   uuid references auth.users(id) not null default auth.uid(),
  draft_id  flowid not null,
  dry_run   bool   not null default false
);
alter table publications enable row level security;
alter publication supabase_realtime add table publications;

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
grant insert (draft_id, dry_run, detail) on publications to authenticated;

comment on table publications is
  'Publications are operations which test and publish drafts into live specifications';
comment on column publications.user_id is
  'User who created the publication';
comment on column publications.draft_id is
  'Draft which is published';
comment on column publications.dry_run is
  'A dry-run publication will test and verify a draft, but doesn''t publish into live specifications';

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
  spec_before json not null,
  -- spec_rev_patch is like spec_fwd_patch but in reverse.
  -- A revert of a publication can be initialized by creating
  -- a draft having all of its publication_specs.spec_rev_patch
  spec_after  json not null
);
alter table draft_specs enable row level security;

create policy "Users must be authorized to the specification catalog name"
  on publication_specs as permissive
  using (true); -- TODO(johnny) auth on catalog_name.
grant all on draft_specs to authenticated;


comment on table publication_specs is '
For each publication, publication_specs details the set of catalog specifications
that changed and their "before" and "after" versions.
';
comment on column publication_specs.pub_id is
  'Publication which published this specification';
comment on column publication_specs.catalog_name is
  'Catalog name of this specification';
comment on column publication_specs.spec_type is
  'Type of this published catalog specification';
comment on column publication_specs.spec_before is '
Former catalog specification which was replaced by this publication.
If the publication created this specification, this will be
the JSON `null` value.
';
comment on column publication_specs.spec_before is '
Catalog specification which was published by this publication.
If the publication deleted this specification, this will be
the JSON `null` value.
';


-- Live (current) specifications of the catalog.
create table live_specs (
  like internal._model including all,

  -- catalog_name is the conceptual primary key, but we use flowid as
  -- the literal primary key for consistency and join performance.
  catalog_name  catalog_name unique not null,

  -- `spec` is the models::${spec_type}Def specification which corresponds to `spec_type`.
  spec_type    catalog_spec_type not null,
  spec         json not null,
  last_pub_id  flowid references publications(id) not null,

  -- Image name and tag are extracted to make it easier
  -- to determine specs which are out of date w.r.t. the latest
  -- connector tag.
  connector_image_name  text,
  connector_image_tag   text
);
alter table live_specs enable row level security;

create policy "Users must be authorized to the specification catalog name"
  on live_specs as permissive
  using (true); -- TODO(johnny) auth catalog_name.
grant select on live_specs to authenticated;

comment on table live_specs is
  'Live (in other words, current) catalog specifications of the platform';
comment on column live_specs.catalog_name is
  'Catalog name of this specification';
comment on column live_specs.spec_type is
  'Type of this catalog specification';
comment on column live_specs.spec is
  'Serialized catalog specification';
comment on column live_specs.last_pub_id is
  'Last publication ID which updated this live specification';

comment on column live_specs.connector_image_name is
  'OCI (Docker) connector image name used by this specification';
comment on column live_specs.connector_image_tag is
  'OCI (Docker) connector image tag used by this specification';


-- Data-flows between live specifications.
create table live_spec_flows (
  source_id flowid not null references live_specs(id),
  target_id flowid not null references live_specs(id),
  flow_type catalog_spec_type not null
);
grant select on live_specs to authenticated;

create unique index idx_live_spec_flows_forward
  on live_spec_flows(source_id, target_id) include (flow_type);
create unique index idx_live_spec_flows_reverse
  on live_spec_flows(target_id, source_id) include (flow_type);

comment on table live_spec_flows is
  'Join table of directed data-flows between live specifications';
comment on column live_spec_flows.source_id is
  'Specification from which data originates';
comment on column live_spec_flows.target_id is
  'Specification to which data flows';


create view draft_specs_ext as
select
  draft_specs.catalog_name,
  draft_specs.draft_id,
  draft_specs.expect_pub_id,
  draft_specs.spec as draft_spec,
  draft_specs.spec_type as draft_spec_type,
  live_specs.last_pub_id,
  live_specs.spec as live_spec,
  live_specs.spec_type as live_spec_type
from draft_specs
left outer join live_specs
  on draft_specs.catalog_name = live_specs.catalog_name
;

grant select on draft_specs_ext to authenticated;
