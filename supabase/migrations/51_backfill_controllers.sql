-- Backfills a chunk of controller_jobs rows, kicking off controllers for that chunk of live specs.
-- This allows us to incrementally enable controllers for tasks that were last published by
-- prior versions of the agent. The idea is to run this migration repeatedly until it stops returning
-- any rows.
begin;

with insert_controller_jobs(live_spec_id) as (
	insert into controller_jobs (live_spec_id)
	select id from live_specs
	where id not in (select live_spec_id from controller_jobs)
	limit 1000
	-- on conflict can't hurt anything and I just can't be bothered to go through
	-- the read-committed docs right now to prove to myself that it isn't necessary.
	on conflict(live_spec_id) do nothing
	returning live_spec_id
)
update live_specs set controller_next_run = now()
where id in (select live_spec_id from insert_controller_jobs)
returning id, catalog_name;

commit;
