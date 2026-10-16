begin;

-- Preserve application history and its referenced directives, but retire the
-- token exchange API. Revoke postgrest access to the directives and applied_directives tables.
drop function public.exchange_directive_token(uuid);
drop type public.exchanged_directive;
revoke all on public.directives from authenticated;
revoke all on public.applied_directives from authenticated;

drop trigger create_directive_task on public.applied_directives;
drop trigger applied_directives_agent_notifications on public.applied_directives;
drop function internal.create_directive_task();

-- Task type 6 belonged to applied directives and no longer has an executor.
delete from internal.tasks where task_type = 6;

commit;
