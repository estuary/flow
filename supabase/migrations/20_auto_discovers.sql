
-- TODO: add comments
create table auto_discovers (
	capture_id flowid not null references live_specs(id),
	last_discover_id flowid references discovers(id),
	discover_interval interval not null,
	primary key (capture_id)
);


create view internal.next_auto_discovers as
select
	live_specs.id as capture_id,
	live_specs.catalog_name as capture_name,
	live_specs.spec->'endpoint' as endpoint_json,
	(live_specs.spec->'autoDiscover'->>'addNewBindings')::boolean as add_new_bindings,
	(live_specs.spec->'autoDiscover'->>'evolveIncompatibleCollections')::boolean as evolve_incompatible_collections,
	connector_tags.id as connector_tags_id,
	now() - discovers.updated_at as overdue_interval
from live_specs
left join auto_discovers on auto_discovers.capture_id = live_specs.id
left join discovers on auto_discovers.last_discover_id = discovers.id
-- We can only perform discovers if we have the connectors and tags rows present.
-- I'd consider it an improvement if we could somehow refactor this to log a warning in cases where there's no connector_tag
inner join connectors
	on split_part(live_specs.spec->'endpoint'->'connector'->>'image', ':', 1) = connectors.image_name
inner join connector_tags
	on connectors.id = connector_tags.connector_id
	and ':' || split_part(live_specs.spec->'endpoint'->'connector'->>'image', ':', 2) = connector_tags.image_tag
where
	coalesce((live_specs.spec->'shards'->>'disabled')::boolean, false) != true
	and coalesce(live_specs.spec->'autoDiscover'->>'addNewBindings', live_specs.spec->'autoDiscover'->>'evolveIncompatibleCollections', 'false')::boolean
	and now() - discovers.updated_at > auto_discovers.discover_interval
order by overdue_interval desc;


-- This job definition depends on the presence of the support@estuary.dev user, which is
-- added by seed.sql and thus run _after_ this migration in fresh installs. This hasn't actually
-- presented as a problem in practice, but I'll leave this comment here just to "I told ya so" myself
-- if it ever becomes an issue ;).
-- 
-- 'ffffffff-ffff-ffff-ffff-ffffffffffff' is support@estuary.dev
-- 
-- TODO: this doesn't acutally work yet.
create function internal.create_auto_discovers()
returns integer as $$
with next_discovers as (
	select * from internal.next_auto_discovers
),
insert_drafts as (
	insert into drafts (user_id)
		select 'ffffffff-ffff-ffff-ffff-ffffffffffff' as user_id
		from next_discovers
		returning
			id as draft_id,
			next_discovers.capture_name as capture_name,
			next_discovers.endpoint_json as endpoint_json, 
			next_discovers.add_new_bindings,
			next_discovers.evolve_incompatible_collections,
			next_discovers.connector_tags_id
),
insert_discovers as (
	
	insert into discovers (capture_name, connector_tag_id, draft_id, endpoint_config, update_only, auto_publish, auto_evolve)
	select capture_name, connector_tags_id as connector_tag_id, draft_id, endpoint_json as endpoint_config,
		not add_new_bindings as update_only, true as auto_publish, evolve_incompatible_collections as auto_evolve
	from insert_drafts
	returning id, insert_drafts.capture_id
)
insert into auto_discovers (capture_id, last_discover_id) select capture_id, id as last_discover_id from insert_discovers

$$ language sql stable security definer;

select cron.schedule (
    'create-discovers', -- name of the cron job
    '*/10 * * * *', -- every 10 minutes. TODO: increase duration for production
    $$ select internal.create_auto_discovers() $$
);
