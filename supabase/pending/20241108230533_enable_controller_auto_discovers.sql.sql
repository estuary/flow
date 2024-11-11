begin;

-- Trigger controller runs of all enabled captures. The controller will start
-- auto-discovery. This is intended to be run multiple times, so we can
-- incrementally enable auto-discovers.
with enabled_captures as (
    select id from live_specs
    where spec_type = 'capture'
    and spec->'autoDiscover' is not null
    and coalesce(spec->'shards'->>'disable', 'false') != 'true'
    limit 50
)
update live_specs
set controller_next_run = now()
where id in (select id from enabled_captures) returning catalog_name;

commit;
