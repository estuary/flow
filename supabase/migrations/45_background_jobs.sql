-- Introduces the ability to distinguish between background and interactive jobs. Interactive jobs
-- are those that users may be actively awaiting. They are identified by having `background = false`,
-- which is the default. The agent will process all jobs where `background = false` before it processes
-- any that have `background = true`. Background jobs are expected to be things like auto-discovers,
-- and the ultimate goal is to prevent things like auto-discovers causing delays in jobs that users
-- are actively waiting on.
begin;

alter table internal._model_async add column background boolean not null default false;
comment on column internal._model_async.background is
    'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';

alter table discovers add column background boolean not null default false;
comment on column discovers.background is
    'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';

alter table publications add column background boolean not null default false;
comment on column publications.background is
    'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';

alter table connector_tags add column background boolean not null default false;
comment on column connector_tags.background is
    'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';

alter table applied_directives add column background boolean not null default false;
comment on column applied_directives.background is
    'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';

alter table evolutions add column background boolean not null default false;
comment on column evolutions.background is
    'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';

-- Previously defined in 20_auto_discovers.sql, and re-created here to set `background = true`.
create or replace function internal.create_auto_discovers()
returns integer as $$
declare
  support_user_id uuid = (select id from auth.users where email = 'support@estuary.dev');
  next_row internal.next_auto_discovers;
  total_created integer := 0;
  tmp_draft_id flowid;
  tmp_discover_id flowid;
begin

for next_row in select * from internal.next_auto_discovers
loop
  -- Create a draft, which we'll discover into
  insert into drafts (user_id) values (support_user_id) returning id into tmp_draft_id;

  insert into discovers (capture_name, draft_id, connector_tag_id, endpoint_config, update_only, auto_publish, auto_evolve, background)
  values (
    next_row.capture_name,
    tmp_draft_id,
    next_row.connector_tags_id,
    next_row.endpoint_json,
    not next_row.add_new_bindings,
    true,
    next_row.evolve_incompatible_collections,
    true
  ) returning id into tmp_discover_id;

  -- This is just useful when invoking the function manually.
  total_created := total_created + 1;
end loop;

return total_created;
end;
$$ language plpgsql security definer;

comment on function internal.create_auto_discovers is
'Creates discovers jobs for each capture that is due for an automatic discover. Each disocver will have auto_publish
set to true. The update_only and auto_evolve columns of the discover will be set based on the addNewBindings and
evolveIncompatibleCollections fields in the capture spec. This function is idempotent. Once a discover is created by
this function, the next_auto_discovers view will no longer include that capture until its interval has passed again.
So its safe to call this function at basically any frequency. The return value of the function is the count of newly
created discovers jobs.';

-- Re-define these triggers to execute for each row, but only for queued jobs.
-- This cuts down on extraneous queries resulting from the agents own `update` statements.
-- This is also an opportunity to use more consistent naming.
drop trigger "Notify agent about changes to publication" on publications;
drop trigger "Notify agent about changes to discover requests" on discovers;
drop trigger "Notify agent of applied directive" on applied_directives;
drop trigger "Notify agent about changes to connector_tags" on connector_tags;
drop trigger "Notify agent about changes to evolution" on evolutions;

create trigger publications_agent_notifications after insert or update on publications
for each row when (NEW.job_status->>'type' = 'queued') execute procedure internal.notify_agent();

create or replace trigger discovers_agent_notifications after insert or update on discovers
for each row when (NEW.job_status->>'type' = 'queued') execute procedure internal.notify_agent();

create or replace trigger applied_directives_agent_notifications after insert or update on applied_directives
for each row when (NEW.job_status->>'type' = 'queued') execute procedure internal.notify_agent();

create or replace trigger connector_tags_agent_notifications after insert or update on connector_tags
for each row when (NEW.job_status->>'type' = 'queued') execute procedure internal.notify_agent();

create or replace trigger evolutions_agent_notifications after insert or update on evolutions
for each row when (NEW.job_status->>'type' = 'queued') execute procedure internal.notify_agent();

commit;
