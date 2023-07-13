
-- User-initiated derivation preview
create table derivation_previews (
  like internal._model_async including all,

  draft_id        flowid        not null references drafts(id) on delete cascade,
  num_documents   integer       not null,
  collection_name catalog_name  not null
);
alter table derivation_previews enable row level security;
alter publication supabase_realtime add table derivation_previews;

create trigger "Notify agent about changes to derivation preview requests" after insert or update on derivation_previews
for each statement execute procedure internal.notify_agent();

create policy "Users access their derivation_previews"
  on derivation_previews as permissive
  using (draft_id in (select id from drafts));

grant select on derivation_previews to authenticated;
grant insert (draft_id, num_documents)
  on derivation_previews to authenticated;

comment on table derivation_previews is
  'User-initiated derivation preview operations';
comment on column derivation_previews.draft_id is
  'Draft to be used as a base by this derivation preview operation';
comment on column derivation_previews.num_documents is
  'Number of documents to feed through the derivation for the preview operation';
comment on column derivation_previews.collection_name is
  'Name of the collection to run the derivation preview on. Must be part of the draft.';
