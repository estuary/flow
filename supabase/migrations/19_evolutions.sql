
-- evolutions are operations that update a draft to re-create one or more existing
-- collections with a new name, and updates existing usages of the old collection
-- to the new ones.
create table evolutions (
  like internal._model_async including all,

  user_id   uuid references auth.users(id) not null default auth.uid(),
  draft_id  flowid not null,
  collections json not null  check (json_typeof(collections) = 'array'),
  auto_publish boolean not null default false
);

comment on table evolutions is
  'Evolutions are async jobs that rename a set of collections in a given draft, '
  'and update and to the draft any captures and materializations of the old collection';
comment on column evolutions.user_id is 
  'Id of the user who created the job';
comment on column evolutions.draft_id is
  'Id of the draft to operate on';
comment on column evolutions.collections is
  'JSON array containing objects in the form of '
  '{"old_name": "acmeCo/foo", "new_name": "acmeCo/foo_v2"}.'
  'Note that the old_name of each collection must identify a draft_spec of the '
  'given draft_id';
comment on column evolutions.auto_publish is
  'whether to automatically publish the results of the evolution, if successful';

alter table evolutions enable row level security;

create trigger "Notify agent about changes to evolution" after insert or update on evolutions
for each statement execute procedure internal.notify_agent();

-- We don't impose a foreign key on drafts, because an evolution
-- operation audit log may stick around much longer than the draft does.
create policy "Users can access only their initiated evolution operations"
  on evolutions as permissive for select
  using (user_id = auth.uid());
create policy "Users can insert evolutions from permitted drafts"
   on evolutions as permissive for insert
   with check (draft_id in (select id from drafts));

grant select on evolutions to authenticated;
grant insert (draft_id, collections, detail) on evolutions to authenticated;

comment on table evolutions is
  'evolutions are operations which test and publish drafts into live specifications';
comment on column evolutions.user_id is
  'User who created the evolution';
comment on column evolutions.draft_id is
  'Draft that is updated to affect the re-creation of the collections';
comment on column evolutions.collections is
  'The names of the collections to re-create';


