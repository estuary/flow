
begin;

-- Add the storageMappings directive, using a static token so that we can hard code the token in the UI.
-- This token is not sensitive, and is safe to be shared publicly.
insert into directives (catalog_prefix, spec, token) values ('ops/', '{"type": "storageMappings"}', 'dd1319b2-e72b-421c-ad2b-082352569bb1');

create or replace function republish_prefix(prefix catalog_prefix)
returns flowid as $$
declare
    draft_id flowid;
    pub_id flowid;
begin
    insert into drafts default values returning id into draft_id;
    insert into draft_specs (draft_id, catalog_name, spec_type, spec, expect_pub_id)
        select draft_id, catalog_name, spec_type, spec, last_pub_id as expect_pub_id
        from live_specs
        where starts_with(catalog_name, prefix) and spec_type is not null;

    insert into publications (draft_id) values (draft_id) returning id into pub_id;
    return pub_id;
end;
$$ language plpgsql security invoker;

comment on function republish_prefix is
'Creates a publication of every task and collection under the given prefix. This will not modify any
of the specs, and will set expect_pub_id to ensure that the publication does not overwrite changes
from other publications. This is intended to be called after an update to the storage mappings of
the prefix to apply the updated mappings.';

commit;
