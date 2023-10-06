
create function tests.test_prune_unchanged_draft_specs()
returns setof text as $$
declare
  draft_id flowid;
  si_collection_spec json = '{"writeSchema":{},"readSchema": {"$ref":"flow://inferred-schema"},"key":["/id"]}'::json;
  reg_collection_spec json = '{"schema":{},"key":["/id"]}'::json;
begin
  
  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin');
    
  insert into inferred_schemas (collection_name, schema, flow_document) values
    ('aliceCo/collA', '{"description": "collA has a schema"}', '{}'),
    ('aliceCo/collC', '{"description": "collC has a schema"}', '{}'),
    ('aliceCo/collD', '{"description": "collD has a schema"}', '{}'),
    ('aliceCo/collE', '{"description": "collE has a schema"}', '{}'),
    ('aliceCo/collG', '{"description": "collG has a schema"}', '{}');

  insert into live_specs (catalog_name, spec_type, spec, inferred_schema_md5) values
    ('aliceCo/capA', 'capture', '{
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "config"}
          }
      },
      "bindings": []
    }', null),
    ('aliceCo/capB', 'capture', '{
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "config"}
          }
      },
      "bindings": []
    }', null),
    ('aliceCo/matA', 'materialization', '{
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "config"}
          }
      },
      "bindings": []
    }', null),
    ('aliceCo/collA', 'collection', reg_collection_spec, 'different md5 that should be ignored'),
    ('aliceCo/collB', 'collection', si_collection_spec, null),
    ('aliceCo/collC', 'collection', si_collection_spec,
      (select md5 from inferred_schemas where collection_name = 'aliceCo/collC')),
    ('aliceCo/collD', 'collection', si_collection_spec, null),
    ('aliceCo/collE', 'collection', si_collection_spec, 'mock stale md5'),
    ('aliceCo/collG', 'collection', si_collection_spec,
      (select md5 from inferred_schemas where collection_name = 'aliceCo/collG'));


  -- Drop privilege to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  insert into drafts (detail) values ('test pruning') returning id into draft_id;

  insert into draft_specs (draft_id, catalog_name, spec_type, spec) values
    -- should be pruned because it's identical to the live spec
    (draft_id, 'aliceCo/capA', 'capture', '{
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "config"}
          }
      },
      "bindings": []
    }'),
    -- should be kept because the spec is different
    (draft_id, 'aliceCo/capB', 'capture', '{
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "NEW CONFIG"}
          }
      },
      "bindings": []
    }'),
    -- should be pruned because spec is identical. Note that the inferred schema
    -- is still setup above so we can assert it is ignored when the spec does not
    -- $ref it.
    (draft_id, 'aliceCo/collA', 'collection', reg_collection_spec),
    -- should prune because spec is idential and inferred schema is still null/missing
    (draft_id, 'aliceCo/collB', 'collection', si_collection_spec),
    -- should prune because spec is identical and inferred schema md5 is the same
    (draft_id, 'aliceCo/collC', 'collection', si_collection_spec),
    -- should keep because inferred schema md5 changed from null to some
    (draft_id, 'aliceCo/collD', 'collection', si_collection_spec),
    -- should keep because inferrred schema md5 changed
    (draft_id, 'aliceCo/collE', 'collection', si_collection_spec),
    -- should keep because it is new
    (draft_id, 'aliceCo/collF', 'collection', si_collection_spec),
    -- should keep because spec changed (whitespace after "writeSchema" to document that behavior)
    (draft_id, 'aliceCo/collG', 'collection', '{
      "writeSchema":{},
      "readSchema": {"$ref": "flow://inferred-schema"},
      "key": ["/id"]
    }');
    
  return query select set_eq(
    $i$ select * from prune_unchanged_draft_specs('$i$ || draft_id || $i$') $i$,
	'{aliceCo/capA, aliceCo/collA, aliceCo/collB, aliceCo/collC}'::text[]
  );

  return query select results_eq(
    $i$ select catalog_name::text from draft_specs where draft_id = '$i$ || draft_id || $i$' order by catalog_name $i$,
	$i$ values ('aliceCo/capB'),('aliceCo/collD'),('aliceCo/collE'),('aliceCo/collF'),('aliceCo/collG') $i$
  );

end;
$$ language plpgsql;
