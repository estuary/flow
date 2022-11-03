
-- User-initiated discover operations, which upsert specifications into a draft.
create table discovers (
  like internal._model_async including all,

  capture_name      catalog_name not null,
  connector_tag_id  flowid   not null references connector_tags(id),
  draft_id          flowid   not null references drafts(id) on delete cascade,
  endpoint_config   json_obj not null
);
alter table discovers enable row level security;
alter publication supabase_realtime add table discovers;

create trigger "Notify agent about changes to discover requests" after insert or update on discovers
for each statement execute procedure internal.notify_agent();

create policy "Users access their discovers"
  on discovers as permissive
  using (draft_id in (select id from drafts));

grant select on discovers to authenticated;
grant insert (capture_name, connector_tag_id, draft_id, endpoint_config)
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

