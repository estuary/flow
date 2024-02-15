create or replace function internal.get_collections_eligible_for_deletion(live_spec_id flowid)
returns table(id flowid, catalog_name catalog_name, last_pub_id flowid) as $$
begin

return query with target_collections as (
  select target_id from live_spec_flows
    where source_id = live_spec_id
),
collections_read as (
  select target_collections.target_id from target_collections
    join live_spec_flows lsf on target_collections.target_id = lsf.source_id
),
collections_written as (
  select target_collections.target_id from target_collections
    join live_spec_flows lsf on target_collections.target_id = lsf.target_id and lsf.source_id <> live_spec_id
),
ineligible_collections as (
  select target_id from collections_read
    union select target_id from collections_written
),
eligible_collections as (
  select target_id from target_collections
    except select target_id from ineligible_collections
)
select
  ls.id,
  ls.catalog_name,
  ls.last_pub_id
from eligible_collections
  join live_specs ls on eligible_collections.target_id = ls.id;

end;
$$ language plpgsql security definer;

comment on function internal.get_collections_eligible_for_deletion is '
get_collections_eligible_for_deletion facilitates the deletion of a capture and its associated collections
by identifying the collections eligible for deletion. A collection is eligible for deletion
if it is not consumed by an active task.
';

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

comment on function draft_collections_eligible_for_deletion is '
draft_collections_eligible_for_deletion facilitates the deletion of a capture and its associated collections
in the same publication by populating the specified draft with the collections eligible for deletion.
The specified draft should contain the capture pending deletion.
';