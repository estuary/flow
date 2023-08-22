with p1 as (
  insert into auth.users (id) values
  ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
),
p2 as (
  insert into drafts (id, user_id) values
  ('1110000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
),
p3 as (
    insert into live_specs (id, catalog_name, spec, spec_type, last_build_id, last_pub_id) values
    ('1000000000000000', 'usageB/CollectionA', '{"schema": {},"key": ["foo"]}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
),
p4 as (
  insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
  (
    '1111000000000000',
    '1110000000000000',
    'usageB/DerivationA',
    '{
        "schema": {},
        "key": ["foo"],
        "derive": {
            "using": {"sqlite": {}},
            "transforms":[
                {
                  "name": "my-name",
                  "source": "usageB/CollectionA",
                  "shuffle": "any"
                }
            ]
        }
    }'::json,
    'collection'
  )
),
p5 as (
  insert into publications (id, job_status, user_id, draft_id) values
  ('1111100000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1110000000000000')
),
p6 as (
  insert into role_grants (subject_role, object_role, capability) values
  ('usageB/', 'usageB/', 'admin'),
  -- This extra grant is here to exercise code paths that might otherwise be skipped
  ('usageB/', 'somethingElse/', 'admin')
),
p7 as (
  insert into user_grants (user_id, object_role, capability) values
  ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'usageB/', 'admin')
)
select 1;