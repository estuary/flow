with s1 as (
  -- start with a simple end-to-end catalog with a few collections, 
  -- plus another materialization that the user won't have access to.
  insert into live_specs (
    id, catalog_name, spec, spec_type, 
    last_build_id, last_pub_id
  ) 
  values 
    (
      'a100000000000000', 'evolution/CollectionA', 
      '{"schema": {
            "type": "object",
            "properties": { "id": {"type": "string"}}, "required": ["id"]
        }, "key": ["id"]}' :: json, 
      'collection', 'bbbbbbbbbbbbbbbb', 
      'bbbbbbbbbbbbbbbb'
    ), 
    (
      'a200000000000000', 'evolution/CollectionB', 
      '{"schema": {
            "type": "object",
            "properties": { "id": {"type": "string"}}, "required": ["id"]
        }, "key": ["id"]}' :: json, 
      'collection', 'bbbbbbbbbbbbbbbb', 
      'bbbbbbbbbbbbbbbb'
    ), 
    (
      'a300000000000000', 'evolution/CollectionC', 
      -- The x-infer-schema annotation used to be relevant, but no longer is.
      '{"schema": {
            "x-infer-schema": true,
            "type": "object",
            "properties": { "id": {"type": "string"}}, "required": ["id"]
        }, "key": ["id"]}' :: json, 
      'collection', 'bbbbbbbbbbbbbbbb', 
      'bbbbbbbbbbbbbbbb'
    ),
    (
      'a400000000000000', 'evolution/CollectionD', 
      '{"schema": {
            "x-infer-schema": true,
            "type": "object",
            "properties": { "id": {"type": "string"}}, "required": ["id"]
        }, "key": ["id"]}' :: json, 
      'collection', 'bbbbbbbbbbbbbbbb', 
      'bbbbbbbbbbbbbbbb'
    ), 
    (
      'a600000000000000', 'evolution/CaptureA', 
      '{
            "bindings": [
                {"target": "evolution/CollectionA", "resource": {"thingy": "foo"}},
                {"target": "evolution/CollectionB", "resource": {"thingy": "bar"}}
            ],
            "endpoint": {"connector": {"image": "captureImage:v1", "config": {}}}
        }' :: json, 
      'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'
    ), 
    (
      'a700000000000000', 'evolution/CaptureB', 
      '{
            "bindings": [
                {"target": "evolution/CollectionC", "resource": {"thingy": "baz"}},
                {"target": "evolution/CollectionD", "resource": {"thingy": "qux"}}
            ],
            "endpoint": {"connector": {"image": "captureImage:v1", "config": {}}}
        }' :: json, 
      'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'
    ), 
    (
      'a800000000000000', 'evolution/MaterializationA', 
      '{
            "bindings": [
                {"source": "evolution/CollectionA", "resource": {"targetThingy": "aThing"}},
                {"source": "evolution/CollectionB", "resource": {"targetThingy": "bThing"}}
            ],
            "endpoint": {"connector": {"image": "matImage:v1", "config": {}}}
        }' :: json, 
      'materialization', 'bbbbbbbbbbbbbbbb', 
      'bbbbbbbbbbbbbbbb'
    ), 
    (
      'a900000000000000', 'evolution/MaterializationB', 
      '{
            "bindings": [
                {"source": "evolution/CollectionC", "backfill": 9, "resource": {"targetThingy": "cThing"}},
                {"source": "evolution/CollectionD", "resource": {"targetThingy": "dThing"}}
            ],
            "endpoint": {"connector": {"image": "matImage:v1", "config": {}}}
        }' :: json, 
      'materialization', 'bbbbbbbbbbbbbbbb', 
      'bbbbbbbbbbbbbbbb'
    ), 
	-- These specs are here so that we can ensure we don't update tasks that the user isn't authorized to.
    (
      'b100000000000000', 'schmevolution/CaptureZ',
      '{
            "bindings": [
              {"target": "evolution/CollectionB", "resource": {"thing": "testSourceThingB"}},
              {"target": "evolution/CollectionD", "resource": {"thing": "testSourceThingD"}}
            ],
            "endpoint": {"connector": {"image": "captureImage:v1", "config": {}}}
        }' :: json, 
      'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'
    ),
    (
      'b200000000000000', 'schmevolution/MaterializationZ',
      '{
            "bindings": [
              {"source": "evolution/CollectionA", "resource": {"targetThingy": "testTargetThingA"}},
              {"source": "evolution/CollectionC", "resource": {"targetThingy": "testTargetThingC"}}
            ],
            "endpoint": {"connector": {"image": "matImage:v1", "config": {}}}
        }' :: json, 
      'materialization', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'
    )
), 
s2 as (
  insert into live_spec_flows (source_id, target_id, flow_type) 
  values 
    (
      'a600000000000000', 'a100000000000000',
      'capture'
    ), 
    (
      'a600000000000000', 'a200000000000000',
      'capture'
    ), 
    (
      'a700000000000000', 'a300000000000000',
      'capture'
    ), 
    (
      'a700000000000000', 'a400000000000000',
      'capture'
    ), 
    (
      'a100000000000000', 'a800000000000000',
      'materialization'
    ), 
    (
      'a200000000000000', 'a800000000000000',
      'materialization'
    ), 
    (
      'a300000000000000', 'a900000000000000',
      'materialization'
    ), 
    (
      'a400000000000000', 'a900000000000000',
      'materialization'
    ), 

    (
      'b100000000000000', 'a200000000000000',
      'capture'
    ),
    (
      'b100000000000000', 'a400000000000000',
      'capture'
    ),
    (
      'a100000000000000', 'b200000000000000',
      'materialization'
    ),
    (
      'a300000000000000', 'b200000000000000',
      'materialization'
    )
),
s3 as (
  insert into tenants (
    tenant, tasks_quota, collections_quota
  ) 
  values 
    ('evolution/', 10, 10), 
    ('schmevolution/', 10, 10)
), 
s4 as (
  insert into auth.users (id) 
  values 
    (
      '43a18a3e-5a59-11ed-9b6a-0242ac188888'
    )
),
-- Create a draft containing new versions of the two collections, plus an
-- additional materialization of one of them, which we expect to also get
-- updated.
s5 as (
  insert into drafts (id, user_id) 
  values 
    (
      '2230000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac188888'
    )
), 
s6 as (
  insert into draft_specs (
    id, draft_id, catalog_name, spec, spec_type
  ) 
  values 
    (
      '1111000000000000', '2230000000000000',
      'evolution/CollectionA', '{"schema": {
            "type": "object",
            "properties": { "id": {"type": "integer"}}, "required": ["id"]
        }, "key": ["id"]}' :: json, 
      'collection'
    ),
    (
      '1112000000000000', '2230000000000000',
      'evolution/CollectionB', '{"schema": {
            "type": "object",
            "properties": { "id": {"type": "integer"}}, "required": ["id"]
        }, "key": ["id"]}' :: json, 
      'collection'
    ),
    (
      '1113000000000000', '2230000000000000',
      'evolution/CollectionC', '{"schema": {
            "x-infer-schema": true,
            "type": "object",
            "properties": { "id": {"type": "integer"}}, "required": ["id"]
        }, "key": ["id"]}' :: json, 
      'collection'
    ),
    (
      '1114000000000000', '2230000000000000',
      'evolution/CollectionD', '{"schema": {
            "x-infer-schema": true,
            "type": "object",
            "properties": { "id": {"type": "integer"}}, "required": ["id"]
        }, "key": ["id"]}' :: json, 
      'collection'
    ),
    (
      '1115000000000000', '2230000000000000',
      'evolution/MaterializationC', '{
        "endpoint": {"connector": {"image": "matImage:v1", "config": {}}},
        "bindings": [{
			"source": "evolution/CollectionB",
			"resource": {
				"targetThingy": "CollectionB"
			}
		}]
      }' :: json, 
      'materialization'
    )
), 
-- The user has admin access to evolution/ but not schmevolution/
s7 as (
  insert into role_grants (
    subject_role, object_role, capability
  ) 
  values 
    (
      'evolution/', 'evolution/', 'admin'
    ), 
    (
      'schmevolution/', 'evolution/', 'write'
    )
), 
s8 as (
  insert into user_grants (user_id, object_role, capability) 
  values 
    (
      '43a18a3e-5a59-11ed-9b6a-0242ac188888', 
      'evolution/', 'admin'
    )
)
select 1;

