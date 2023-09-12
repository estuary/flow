with setup_user as (
  insert into auth.users (id) values
  ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
),
setup_user_grants as (
  insert into user_grants (user_id, object_role, capability) values
  ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'acmeCo/', 'admin')
),
setup_role_grants as (
  insert into role_grants (subject_role, object_role, capability) values
  ('acmeCo/', 'acmeCo/', 'admin')
),
setup_tenant as (
  -- Set the tenant collections quota such that publication would fail due to being over quota
  -- if the `should_prune` collection is not pruned from the publication.
  insert into tenants (tenant, collections_quota) values ('acmeCo/', 7)
),
setup_draft as (
  insert into drafts (id, user_id) values
  ('1110000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
),
setup_draft_specs as (
  insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
  ('1111000000000000', '1110000000000000', 'acmeCo/CaptureA', '{
      "bindings": [
        {"target": "acmeCo/should_stay", "resource": {"thingy": "foo"}},
        {"target": "acmeCo/should_stay2", "disable": true, "resource": {"thingy": "foo"}},
        {"target": "acmeCo/should_stay3", "disable": true, "resource": {"thingy": "foo"}},
        {"target": "acmeCo/should_prune", "disable": true, "resource": {"thingy": "bar"}}
      ],
      "endpoint": {"connector": {"image": "allowed_connector", "config": {}}}
  }'::json, 'capture'),
  -- should not be pruned because a drafted capture writes to it
  ('1111110000000000', '1110000000000000', 'acmeCo/should_stay', '{
    "schema": { "type": "object" },
    "key": ["/id"]
  }', 'collection'),
  -- should not be pruned because a live materialization reads from it
  ('1111111000000000', '1110000000000000', 'acmeCo/should_stay2', '{
    "schema": { "type": "object" },
    "key": ["/id"]
  }', 'collection'),
  -- should not be pruned because a live capture writes to it
  ('1111111100000000', '1110000000000000', 'acmeCo/should_stay3', '{
    "schema": { "type": "object" },
    "key": ["/id"]
  }', 'collection'),
 -- this one should not be pruned because a live derivation reads it
  ('1111111110000000', '1110000000000000', 'acmeCo/should_stay4', '{
    "schema": { "type": "object" },
    "key": ["/id"]
  }', 'collection'),
  -- should not be pruned because there is a corresponding live_specs row
  ('1111111111000000', '1110000000000000', 'acmeCo/should_stay5', '{
    "schema": { "type": "object" },
    "key": ["/id"]
  }', 'collection'),
  -- should not be pruned because it's a derivation
  ('1111111111100000', '1110000000000000', 'acmeCo/should_stay6', '{
    "schema": { "type": "object" },
    "key": ["/id"],
    "derive": {
      "using": {"sqlite": {}},
      "transforms": []
    }
  }', 'collection'),
  -- Prune on, oh pruny one
  ('1111111111110000', '1110000000000000', 'acmeCo/should_prune', '{
    "schema": { "type": "object" },
    "key": ["/id"]
  }', 'collection')
),
setup_live_specs as (
  insert into live_specs (id, catalog_name, spec_type, reads_from, writes_to, spec, created_at) values
    ('2222222222222220', 'acmeCo/a/materialization', 'materialization', '{acmeCo/should_stay2}', null, '{
      "endpoint": { "connector": { "image": "allowed_connector", "config": {}} },
      "bindings": [{"source": "acmeCo/should_stay2", "resource": {"thingy": "foo"}}]
    }', '2022-02-02T02:22:22Z'),
    ('2222222222222200', 'acmeCo/a/capture', 'capture', null, '{acmeCo/should_stay3}', '{
      "endpoint": { "connector": { "image": "allowed_connector", "config": {}} },
      "bindings": [{"resource": {"thingy": "foo"}, "target": "acmeCo/should_stay3"}]
    }', '2022-02-02T02:22:22Z'),
    ('2222222222222000', 'acmeCo/a/derivation', 'collection', '{acmeCo/should_stay4}', null, '{
      "schema": { "type": "object" },
      "key": ["/id"],
      "derive": {
        "using": { "sqlite": {}},
        "transforms": [{"name": "foo", "source": "acmeCo/should_stay4", "lambda": "select 1;"}]
      }
    }', '2022-02-02T02:22:22Z'),
    ('2222222222220000', 'acmeCo/should_stay2', 'collection', null, null, '{
      "schema": { "type": "object" },
      "key": ["/id"]
    }', '2022-02-02T02:22:22Z'),
    ('2222222222200000', 'acmeCo/should_stay3', 'collection', null, null, '{
      "schema": { "type": "object" },
      "key": ["/id"]
    }', '2022-02-02T02:22:22Z'),
    ('2222222222000000', 'acmeCo/should_stay4', 'collection', null, null, '{
      "schema": { "type": "object" },
      "key": ["/id"]
    }', '2022-02-02T02:22:22Z'),
    ('2222222220000000', 'acmeCo/should_stay5', 'collection', null, null, '{
      "schema": { "type": "object" },
      "key": ["/id"]
    }', '2022-02-02T02:22:22Z')
),
setup_live_spec_flows as (
  insert into live_spec_flows (source_id, target_id, flow_type) values
    ('2222222222220000', '2222222222222220', 'materialization'),
    ('2222222222222200', '2222222222200000', 'capture'),
    ('2222222222000000', '2222222222222000', 'collection')
),
setup_publications as (
  insert into publications (id, job_status, user_id, draft_id) values
  ('1111100000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1110000000000000')
),
setup_connectors as (
    insert into connectors (external_url, image_name, title, short_description, logo_url) values
        ('http://example.com', 'allowed_connector', '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json)
)
select 1;
