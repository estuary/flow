create or replace function internal.get_collections_eligible_for_deletion(live_spec_id flowid)
returns table(id flowid, catalog_name catalog_name, last_pub_id flowid) as $$
declare
  eligible_collection_ids flowid[];
begin

eligible_collection_ids := array(with target_collections as (
    select target_id from live_spec_flows
        where source_id = live_spec_id
)
select * from target_collections
    where target_id not in (select source_id from live_spec_flows));

return query select
  live_specs.id,
  live_specs.catalog_name,
  live_specs.last_pub_id
from live_specs
where spec_type = 'collection' and live_specs.id = ANY(eligible_collection_ids);

end;
$$ language plpgsql security definer;

create or replace function draft_collections_eligible_for_deletion(live_spec_id flowid, draft_id flowid)
returns void as $$
begin

with eligible_collections as (
  select * from internal.get_collections_eligible_for_deletion(live_spec_id)
)
insert into draft_specs (draft_id, catalog_name, expect_pub_id, spec, spec_type)
  select draft_id, catalog_name, last_pub_id, null, null from eligible_collections;

end;
$$ language plpgsql security definer;