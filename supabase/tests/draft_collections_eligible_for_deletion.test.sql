create function tests.test_get_collections_eligible_for_deletion()
returns setof text as $$
declare
  target_capture_id flowid;
  target_draft_id flowid;

  collection_a_id flowid;
  collection_b_id flowid;
  collection_c_id flowid;
  collection_d_id flowid;
  collection_e_id flowid;

  capture_consumer_id flowid;
  materialization_consumer_id flowid;
  derivation_consumer_id flowid;
begin

  insert into user_grants (user_id, object_role, capability) values
    ('33333333-3333-3333-3333-333333333333', 'carolCo/', 'admin');

  insert into live_specs (catalog_name, spec_type, spec, writes_to, reads_from) values
    ('carolCo/capture/pending-deletion', 'capture', '{
        "endpoint": {
          "connector": {
            "image": "some image",
              "config": {"some": "config"}
            }
        },
        "bindings": []
      }',
      array[
        'carolCo/pending-deletion/collection-A',
        'carolCo/pending-deletion/collection-B',
        'carolCo/pending-deletion/collection-C',
        'carolCo/pending-deletion/collection-D',
        'carolCo/pending-deletion/collection-E'
      ],
      null),
    ('carolCo/pending-deletion/collection-A', 'collection', '{"schema":{},"key":["/id"]}', null, null),
    ('carolCo/pending-deletion/collection-B', 'collection', '{"schema":{},"key":["/id"]}', null, null),
    ('carolCo/pending-deletion/collection-C', 'collection', '{"schema":{},"key":["/id"]}', null, null),
    ('carolCo/pending-deletion/collection-D', 'collection', '{"schema":{},"key":["/id"]}', null, null),
    ('carolCo/pending-deletion/collection-E', 'collection', '{"schema":{},"key":["/id"]}', null, null),
    ('carolCo/capture/consumes-collection-A', 'capture', '{
        "endpoint": {
          "connector": {
            "image": "some image",
              "config": {"some": "config"}
            }
        },
        "bindings": []
      }',
      array['carolCo/pending-deletion/collection-A'],
      null),
    ('carolCo/materialization/consumes-collection-B', 'materialization', '{
        "endpoint": {
          "connector": {
            "image": "some image",
              "config": {"some": "config"}
            }
        },
        "bindings": []
      }',
      null,
      array['carolCo/pending-deletion/collection-B']),
    ('carolCo/derivation/consumes-collection-C', 'collection', '{
        "using": {
          "sqlite": {
            "migrations": []
            }
        },
        "transforms": []
      }',
      array['carolCo/pending-deletion/collection-C'],
      null);

  target_capture_id := (select id from live_specs where catalog_name = 'carolCo/capture/pending-deletion');

  collection_a_id := (select id from live_specs where catalog_name = 'carolCo/pending-deletion/collection-A');
  capture_consumer_id := (select id from live_specs where catalog_name = 'carolCo/capture/consumes-collection-A');

  collection_b_id := (select id from live_specs where catalog_name = 'carolCo/pending-deletion/collection-B');
  materialization_consumer_id := (select id from live_specs where catalog_name = 'carolCo/materialization/consumes-collection-B');

  collection_c_id := (select id from live_specs where catalog_name = 'carolCo/pending-deletion/collection-C');
  derivation_consumer_id := (select id from live_specs where catalog_name = 'carolCo/derivation/consumes-collection-C');

  collection_d_id := (select id from live_specs where catalog_name = 'carolCo/pending-deletion/collection-D');
  collection_e_id := (select id from live_specs where catalog_name = 'carolCo/pending-deletion/collection-E');


  insert into live_spec_flows (source_id, target_id, flow_type) values
    (target_capture_id, collection_a_id, 'capture'),
    (target_capture_id, collection_b_id, 'capture'),
    (target_capture_id, collection_c_id, 'capture'),
    (target_capture_id, collection_d_id, 'capture'),
    (target_capture_id, collection_e_id, 'capture'),
    (capture_consumer_id, collection_a_id, 'capture'),
    (collection_b_id, materialization_consumer_id, 'materialization'),
    (derivation_consumer_id, collection_c_id, 'collection');


  insert into drafts (user_id, detail) values
    ('33333333-3333-3333-3333-333333333333', 'carolCo/capture/pending-deletion');

  target_draft_id := (select id from drafts where detail = 'carolCo/capture/pending-deletion');

  insert into draft_specs (draft_id, catalog_name, spec_type, spec) values
    (target_draft_id, 'aliceCo/capture/pending-deletion', 'capture', '{
        "endpoint": {
          "connector": {
            "image": "some image",
              "config": {"some": "config"}
            }
        },
        "bindings": []
      }');

  return query select results_eq(
    $i$ select catalog_name from internal.get_collections_eligible_for_deletion('$i$ || target_capture_id || $i$') $i$,
    $i$ values ('carolCo/pending-deletion/collection-D'::catalog_name), ('carolCo/pending-deletion/collection-E'::catalog_name) $i$
  );

end;
$$ language plpgsql;