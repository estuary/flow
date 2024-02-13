create function tests.test_draft_collections_eligible_for_deletion()
returns setof text as $$
declare
  target_capture_id flowid;
  target_draft_id flowid;

  collection_a_id flowid;
  collection_b_id flowid;
  collection_c_id flowid;
  collection_d_id flowid;
  collection_e_id flowid;
  collection_f_id flowid;

  capture_consumer_id flowid;
  materialization_consumer_b_id flowid;
  materialization_consumer_f_id flowid;
  derivation_consumer_c_id flowid;
  derivation_consumer_d_id flowid;
begin

  insert into user_grants (user_id, object_role, capability) values
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin'),
    ('33333333-3333-3333-3333-333333333333', 'carolCo/', 'admin');

  insert into role_grants (subject_role, object_role, capability) values
    ('carolCo/', 'carolCo/', 'admin'),
    ('carolCo/', 'bobCo/shared/', 'admin'),
    ('bobCo/', 'bobCo/', 'admin'),
    ('bobCo/', 'carolCo/', 'admin');

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
        'carolCo/pending-deletion/collection-E',
        'carolCo/pending-deletion/collection-F'
      ],
      null),
    ('carolCo/pending-deletion/collection-A', 'collection', '{"schema":{},"key":["/id"]}', null, null),
    ('carolCo/pending-deletion/collection-B', 'collection', '{"schema":{},"key":["/id"]}', null, null),
    ('carolCo/pending-deletion/collection-C', 'collection', '{"schema":{},"key":["/id"]}', null, null),
    ('carolCo/pending-deletion/collection-D', 'collection', '{"schema":{},"key":["/id"]}', null, null),
    ('carolCo/pending-deletion/collection-E', 'collection', '{"schema":{},"key":["/id"]}', null, null),
    ('carolCo/pending-deletion/collection-F', 'collection', '{"schema":{},"key":["/id"]}', null, null),
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
      null),
    ('carolCo/derivation/consumes-collection-D', 'collection', '{
        "using": {
          "sqlite": {
            "migrations": []
            }
        },
        "transforms": []
      }',
      null,
      array['carolCo/pending-deletion/collection-D']),
    ('bobCo/materialization/consumes-collection-F', 'materialization', '{
        "endpoint": {
          "connector": {
            "image": "some image",
              "config": {"some": "config"}
            }
        },
        "bindings": []
      }',
      null,
      array['carolCo/pending-deletion/collection-F']);

  target_capture_id := (select id from live_specs where catalog_name = 'carolCo/capture/pending-deletion');

  collection_a_id := (select id from live_specs where catalog_name = 'carolCo/pending-deletion/collection-A');
  capture_consumer_id := (select id from live_specs where catalog_name = 'carolCo/capture/consumes-collection-A');

  collection_b_id := (select id from live_specs where catalog_name = 'carolCo/pending-deletion/collection-B');
  materialization_consumer_b_id := (select id from live_specs where catalog_name = 'carolCo/materialization/consumes-collection-B');

  collection_c_id := (select id from live_specs where catalog_name = 'carolCo/pending-deletion/collection-C');
  derivation_consumer_c_id := (select id from live_specs where catalog_name = 'carolCo/derivation/consumes-collection-C');

  collection_d_id := (select id from live_specs where catalog_name = 'carolCo/pending-deletion/collection-D');
  derivation_consumer_d_id := (select id from live_specs where catalog_name = 'carolCo/derivation/consumes-collection-D');

  collection_e_id := (select id from live_specs where catalog_name = 'carolCo/pending-deletion/collection-E');

  collection_f_id := (select id from live_specs where catalog_name = 'carolCo/pending-deletion/collection-F');
  materialization_consumer_f_id := (select id from live_specs where catalog_name = 'carolCo/materialization/consumes-collection-B');

  insert into live_spec_flows (source_id, target_id, flow_type) values
    (target_capture_id, collection_a_id, 'capture'),
    (target_capture_id, collection_b_id, 'capture'),
    (target_capture_id, collection_c_id, 'capture'),
    (target_capture_id, collection_d_id, 'capture'),
    (target_capture_id, collection_e_id, 'capture'),
    (target_capture_id, collection_f_id, 'capture'),
    (capture_consumer_id, collection_a_id, 'capture'),
    (collection_b_id, materialization_consumer_b_id, 'materialization'),
    (collection_f_id, materialization_consumer_f_id, 'materialization'),
    (derivation_consumer_c_id, collection_c_id, 'collection'),
    (collection_d_id, derivation_consumer_d_id, 'collection');

  insert into drafts (user_id, detail) values ('33333333-3333-3333-3333-333333333333', 'carolCo/capture/pending-deletion') returning id into target_draft_id;

  insert into draft_specs (draft_id, catalog_name, spec_type, spec) values
    (target_draft_id, 'carolCo/capture/pending-deletion', null, null);

  return query select results_eq(
    $i$ select catalog_name from get_collections_eligible_for_deletion('$i$ || target_capture_id || $i$') $i$,
    $i$ values ('carolCo/pending-deletion/collection-E'::catalog_name) $i$
  );

  -- Drop privilege to `authenticated` and authorize as Carol.
  perform set_authenticated_context('33333333-3333-3333-3333-333333333333');

  perform draft_collections_eligible_for_deletion(target_capture_id, target_draft_id);

  return query select results_eq(
    $i$ select catalog_name, spec_type from draft_specs where draft_id = '$i$ || target_draft_id || $i$' $i$,
    $i$ values (
      'carolCo/capture/pending-deletion'::catalog_name,
      null::catalog_spec_type
      ),
      (
        'carolCo/pending-deletion/collection-E'::catalog_name,
        null::catalog_spec_type
      );
    $i$
  );

end;
$$ language plpgsql;
