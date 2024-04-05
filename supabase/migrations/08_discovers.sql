
-- User-initiated discover operations, which upsert specifications into a draft.
create table discovers (
  like internal._model_async including all,

  capture_name      catalog_name not null,
  connector_tag_id  flowid   not null references connector_tags(id),
  draft_id          flowid   not null references drafts(id) on delete cascade,
  endpoint_config   json_obj not null,
  update_only       boolean  not null default false,
  auto_publish      boolean  not null default false,
  auto_evolve       boolean  not null default false
);
alter table discovers enable row level security;

create trigger "Notify agent about changes to discover requests" after insert or update on discovers
for each statement execute procedure internal.notify_agent();

create policy "Users access their discovers"
  on discovers as permissive
  using (draft_id in (select id from drafts));

grant select on discovers to authenticated;
grant insert (capture_name, connector_tag_id, draft_id, endpoint_config, update_only)
  on discovers to authenticated;

comment on table discovers is
  'User-initiated connector discovery operations';
comment on column discovers.capture_name is
  'Intended name of the capture produced by this discover';
comment on column discovers.connector_tag_id is
  'Tagged connector which is used for discovery';
comment on column discovers.draft_id is
  'Draft to be populated by this discovery operation';
comment on column discovers.endpoint_config is
  'Endpoint configuration of the connector. May be protected by sops';
comment on column discovers.update_only is '
If true, this operation will draft updates to existing bindings and their
target collections but will not add new bindings or collections.';
comment on column discovers.auto_publish is
'whether to automatically publish the results of the discover, if successful';
comment on column discovers.auto_evolve is
'whether to automatically create an evolutions job if the automatic publication
fails due to incompatible collection schemas. This determines the value of `auto_evolve`
in the publications table when inserting a new row as a result of this discover.';
