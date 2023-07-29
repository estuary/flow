
with p1 as (
  insert into auth.users (id, email, email_confirmed_at) values
  ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'someuser@org.test', '2023-08-01T00:01:02Z'),
  ('11111111-2222-3333-4444-555555555555', 'accounts@estuary.test', '2023-08-01T00:01:02Z')
),
setup_user_grants as (
  insert into user_grants (user_id, object_role, capability) values
    ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'acmeCo/', 'admin')
),
setup_role_grants as (
  insert into role_grants (subject_role, object_role, capability) values
    ('acmeCo/', 'acmeCo/', 'admin')
),
setup_live_specs as (
    insert into live_specs (id, catalog_name, spec, built_spec, spec_type, last_build_id, last_pub_id) values
    ('1000000000000000', 'acmeCo/captureA/source-happy', '{
		"bindings": [
          {"target": "acmeCo/captureA/c1","resource": {}},
          {"target": "acmeCo/captureA/c2","disable":true,"resource": {}},
          {"target": "acmeCo/captureA/c3","resource": {}}
        ]
	}'::json, '{
      "name": "acmeCo/captureA/source-happy",
      "connector_type": "IMAGE",
      "config": {},
      "bindings": [
        {"collection":{"name":"acmeCo/captureA/c1"}, "resource_config_json":{}},
        {"collection":{"name":"acmeCo/captureA/c3"}, "resource_config_json":{}}
      ]
    }'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
    ('2000000000000000', 'acmeCo/captureB/source-empty', '{
		"bindings": [ ]
	}'::json, '{
      "name": "acmeCo/captureB/source-empty",
      "connector_type": "IMAGE",
      "config": {},
      "bindings": []
    }'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
    ('3000000000000000', 'acmeCo/captureC/source-all-disabled', '{
		"bindings": [ {"target": "acmeCo/captureC/c1","disable": true} ]
	}'::json, '{
      "name": "acmeCo/captureC/source-all-disabled",
      "connector_type": "IMAGE",
      "config": {},
      "bindings": []
    }'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),

-- We don't need built specs for materializations for these tests, so they're all null
    ('1100000000000000', 'acmeCo/matA/starting-empty', '{
      "endpoint": {"connector":{"image":"matImage:v1","config": {}}},
      "sourceCapture": "acmeCo/captureA/source-happy",
      "bindings": []
    }'::json, null, 'materialization', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
    ('1200000000000000', 'acmeCo/matA/partial', '{
      "endpoint": {"connector":{"image":"matImage:v1","config":{}}},
      "sourceCapture": "acmeCo/captureA/source-happy",
      "bindings": [
        {"source": "acmeCo/captureA/c1","resource":{"targetThingy":"cee_one"}},
        {"source": "acmeCo/captureA/c2","resource":{"targetThingy":"cee_two"}},
        {"source": "acmeCo/captureA/c99","resource":{"targetThingy":"cee_ninty_nine"}}
      ]
    }'::json, null, 'materialization', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
    
    ('1300000000000000', 'acmeCo/matB/other-bindings', '{
      "endpoint": {"connector":{"image":"matImage:v1","config":{}}},
      "sourceCapture": "acmeCo/captureB/source-empty",
      "bindings": [
        {"source": "acmeCo/captureB/c1","resource":{"targetThingy":"cee_one"}},
        {"source": "acmeCo/captureB/c99","resource":{"targetThingy":"cee_ninty_nine"}}
      ]
    }'::json, null, 'materialization', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
    ('1400000000000000', 'acmeCo/matB/already-matching', '{
      "endpoint": {"connector":{"image":"matImage:v1","config":{}}},
      "sourceCapture": "acmeCo/captureB/source-empty",
      "bindings": [
        {"source": "acmeCo/captureB/c1","resource":{"targetThingy":"cee_one"}, "disable": true}
      ]
    }'::json, null, 'materialization', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),

    ('1500000000000000', 'acmeCo/matC/extra-bindings', '{
      "endpoint": {"connector":{"image":"matImage:v1","config":{}}},
      "sourceCapture": "acmeCo/captureC/source-all-disabled",
      "bindings": [
        {"source": "acmeCo/captureC/c1", "resource": {"targetThingy": "see-won"}}
      ]
    }'::json, null, 'materialization', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
    ('1600000000000000', 'acmeCo/matC/already-matching', '{
      "endpoint": {"connector":{"image":"matImage:v1","config":{}}},
      "sourceCapture": "acmeCo/captureC/source-empty",
      "bindings": [
        {"source": "acmeCo/captureC/c1","disable": true}
      ]
    }'::json, null, 'materialization', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),

    ('2100000000000000', 'acmeCo/deleted/thing', null, null, null, 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
),
setup_connectors as (
  insert into connectors (
    id, external_url, image_name, title, 
    short_description, logo_url
  ) 
  values 
    (
      '6666666666666666', 'http://example.com', 
      'matImage', '{"en-US": "foo"}' :: json, 
      '{"en-US": "foo"}' :: json, '{"en-US": "foo"}' :: json
    )
),
setup_connector_tags as (
  -- Evolution requires the resource_spec_schema in order to get the location
  -- of the `x-collection-name` annotation.
  insert into connector_tags (
    connector_id, image_tag, protocol, 
    resource_spec_schema
  ) 
  values 
    (
      '6666666666666666', ':v1', 'materialize', 
      '{
            "type": "object",
            "properties": {
                "targetThingy": {
                    "type": "string",
                    "x-collection-name": true
                }
            }
      }'
    )
) 
select 1;