-- This disables and removes the sql-based auto-discovers, and should be run
-- prior to deploying the new agent version with controller-based
-- auto-discovers.
begin;

select cron.unschedule('create-discovers');
drop function intenrnal.create_auto_discovers;
drop view internal.next_auto_discovers;

commit;
