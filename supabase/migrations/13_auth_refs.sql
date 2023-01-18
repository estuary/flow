-- TODO(johnny): These views are particularly experimental and are likely to change.

/*
-- View which identifies live specifications that are missing
-- a required write authorization to a referant.
create view internal.missing_write_auth as
select
  src.id as sub_id,
  src.catalog_name as sub_name,
  src.spec_type as sub_type,
  tgt.id as obj_id,
  tgt.catalog_name as obj_name,
  tgt.spec_type as obj_type
from live_specs src
join live_spec_flows e on src.spec_type in ('capture', 'test') and src.id = e.source_id
join live_specs tgt on e.target_id = tgt.id
where not exists(
  select 1 from role_grants
  where starts_with(src.catalog_name, subject_role) and
        starts_with(tgt.catalog_name, object_role) and
        capability >= 'write'
);

-- View which identifies live specifications that are missing
-- a required read authorization to a referant.
create view internal.missing_read_auth as
select
  src.id as obj_id,
  src.catalog_name as obj_name,
  src.spec_type as obj_type,
  tgt.id as sub_id,
  tgt.catalog_name as sub_name,
  tgt.spec_type as sub_type
from live_specs src
join live_spec_flows e on src.spec_type = 'collection' and src.id = e.source_id
join live_specs tgt on e.target_id = tgt.id
where not exists(
  select 1 from role_grants
  where starts_with(src.catalog_name, object_role) and
        starts_with(tgt.catalog_name, subject_role) and
        capability >= 'read'
);

-- View which identifies live specifications that are missing
-- a required read or write authorization to a referant.
create view internal.missing_auth as
select sub_id, sub_name, sub_type, obj_id, obj_name, obj_type, true as write
from internal.missing_write_auth
union all
select sub_id, sub_name, sub_type, obj_id, obj_name, obj_type, false
from internal.missing_read_auth
;
*/