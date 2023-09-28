
-- publications are operations that test (if dry_run) or test and then publish
-- a draft. We retain publication rows for a limited period of time,
-- but continue to use their unique IDs within the longer-lived audit log
-- of published specifications.
create table publications (
  like internal._model_async including all,

  user_id   uuid references auth.users(id) not null default auth.uid(),
  draft_id  flowid not null,
  dry_run   bool   not null default false,
  auto_evolve boolean not null default false
);
alter table publications enable row level security;

create trigger "Notify agent about changes to publication" after insert or update on publications
for each statement execute procedure internal.notify_agent();

-- We don't impose a foreign key on drafts, because a publication
-- operation audit log may stick around much longer than the draft does.
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
comment on column publications.auto_evolve is
  'Whether to automatically handle schema evolution if the publication fails due to incompatible collections.
  If true, then an evolutions job will be created automatically if needed, and the results will be published again.';


-- Live (current) specifications of the catalog.
create table live_specs (
  like internal._model including all,

  -- catalog_name is the conceptual primary key, but we use flowid as
  -- the literal primary key for consistency and join performance.
  catalog_name          catalog_name not null,
  connector_image_name  text,
  connector_image_tag   text,
  last_build_id         flowid not null,
  last_pub_id           flowid not null,
  reads_from            text[],
  spec                  json,
  spec_type             catalog_spec_type,
  writes_to             text[],
  built_spec            json,
  -- JSON specs are encoded into the database with leading spaces which must be trimmed to compute
  -- an accurate md5.
  md5                   text generated always as (md5(trim(spec::text))) stored,

  constraint "spec and spec_type must be consistent" check (
    json_typeof(spec) is distinct from 'null' and (spec is null) = (spec_type is null)
  ),
  unique (catalog_name)
);
alter table live_specs enable row level security;

-- Index that accelerates operator ^@ (starts-with) for live_specs_ext view.
create index idx_live_specs_catalog_name_spgist on live_specs using spgist ((catalog_name::text));

create index idx_live_specs_spec_type on live_specs (spec_type);
create index idx_live_specs_updated_at on live_specs (updated_at desc nulls last);

create policy "Users must be read-authorized to the specification catalog name"
  on live_specs as permissive for select
  using (exists(
    select 1 from auth_roles('read') r where catalog_name ^@ r.role_prefix
  ));
grant select on live_specs to authenticated;

comment on table live_specs is
  'Live (in other words, current) catalog specifications of the platform';
comment on column live_specs.catalog_name is
  'Catalog name of this specification';
comment on column live_specs.connector_image_name is
  'OCI (Docker) connector image name used by this specification';
comment on column live_specs.connector_image_tag is
  'OCI (Docker) connector image tag used by this specification';
comment on column live_specs.last_build_id is '
Last publication ID under which this specification was built and activated
into the data-plane, even if it was not necessarily updated.

A specification may be included in a publication which did not directly
change it simply because of its connection to other specifications which
were part of that publication: Flow identifies connected specifications
in order to holistically verify and test their combined behaviors.
';
comment on column live_specs.last_pub_id is
  'Last publication ID which updated this specification';
comment on column live_specs.reads_from is '
List of collections read by this catalog task specification,
or NULL if not applicable to this specification type.
These adjacencies are also indexed within `live_spec_flows`.
';
comment on column live_specs.spec is
  'Serialized catalog specification, or NULL if this specification is deleted';
comment on column live_specs.spec_type is
  'Type of this catalog specification, or NULL if this specification is deleted';
comment on column live_specs.writes_to is '
List of collections written by this catalog task specification,
or NULL if not applicable to this specification type.
These adjacencies are also indexed within `live_spec_flows`.
';
comment on column live_specs.built_spec is
  'Built specification for this catalog';


-- Data-flows between live specifications.
create table live_spec_flows (
  source_id flowid not null references live_specs(id),
  target_id flowid not null references live_specs(id),
  flow_type catalog_spec_type not null
);
alter table live_spec_flows enable row level security;

create policy "Users must be authorized to referenced specifications"
  on live_spec_flows as permissive for select
  using (
    source_id in (select id from live_specs) and
    target_id in (select id from live_specs)
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


-- Published specifications which record the changes made to specs over time.
create table publication_specs (
  live_spec_id  flowid references live_specs(id) not null,
  pub_id        flowid not null,
  primary key   (live_spec_id, pub_id),

  detail        text,
  published_at  timestamptz not null default now(),
  spec          json,
  spec_type     catalog_spec_type,
  user_id       uuid references auth.users(id) not null default auth.uid(),

  constraint "spec and spec_type must be consistent" check (
    json_typeof(spec) is distinct from 'null' and (spec is null) = (spec_type is null)
  )
);
alter table draft_specs enable row level security;

create policy "Users must be read-authorized to the specification catalog name"
  on publication_specs as permissive for select
  using (live_spec_id in (select id from live_specs));
grant select on publication_specs to authenticated;


comment on table publication_specs is '
publication_specs details the publication history of the `live_specs` catalog.
Each change to a live specification is recorded into `publication_specs`.
';
comment on column publication_specs.live_spec_id is
  'Live catalog specification which was published';
comment on column publication_specs.pub_id is
  'Publication ID which published to the catalog specification';
comment on column publication_specs.spec_type is
  'Type of the published catalog specification, or NULL if this was a deletion';
comment on column publication_specs.spec is '
Catalog specification which was published by this publication,
or NULL if this was a deletion.
';
comment on column publication_specs.user_id is
  'User who performed this publication.';
