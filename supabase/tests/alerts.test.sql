create function tests.test_alert_data_processing_firing()
returns setof text as $$
begin
  delete from alert_subscriptions;
  delete from alert_data_processing;
  delete from alert_history;

  insert into alert_subscriptions (catalog_prefix, email) values ('aliceCo/', 'alice@example.com'), ('aliceCo/', 'bob@example.com'), ('aliceCo/', null);

  insert into alert_data_processing (catalog_name, evaluation_interval) values
    ('aliceCo/capture/three-hours', '2 hours'),
    ('aliceCo/capture/two-hours', '2 hours'),
    ('aliceCo/capture/deleted', '2 hours'),
    ('aliceCo/materialization/four-hours', '4 hours'),
    ('aliceCo/materialization/disabled', '2 hours');

    with insert_live as (
    insert into live_specs (catalog_name, spec_type, spec, created_at) values
        ('aliceCo/capture/three-hours', 'capture', '{
            "endpoint": {
            "connector": {
                "image": "some image",
                "config": {"some": "config"}
                }
            },
            "bindings": []
        }', now() - '3h'::interval),
        ('aliceCo/capture/two-hours', 'capture', '{
            "endpoint": {
            "connector": {
                "image": "some image",
                "config": {"some": "config"}
                }
            },
            "bindings": []
        }', now() - '2h'::interval),
        ('aliceCo/capture/deleted', 'capture', null, now() - '3h'::interval),
        ('aliceCo/materialization/four-hours', 'materialization', '{
            "endpoint": {
            "connector": {
                "image": "some image",
                "config": {"some": "config"}
                }
            },
            "bindings": []
        }', now() - '4h'::interval),
        ('aliceCo/materialization/disabled', 'materialization', '{
            "endpoint": {
            "connector": {
                "image": "some image",
                "config": {"some": "config"}
                }
            },
            "bindings": [],
            "shards": { "disable": true }
        }', now() - '3h'::interval)
    returning controller_task_id
    )
    insert into internal.tasks (task_id, task_type)
    select controller_task_id, 2 from insert_live;

  insert into catalog_stats_hourly (catalog_name, grain, ts, flow_document, bytes_written_by_me, bytes_written_to_me, bytes_read_by_me) values
    ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now()), '{}', 0, 0, 0),
    ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
    ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
    ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now() - '3h'::interval), '{}', 1024, 0, 0),
    ('aliceCo/capture/two-hours', 'hourly', date_trunc('hour', now()), '{}', 0, 0, 0),
    ('aliceCo/capture/two-hours', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
    ('aliceCo/capture/two-hours', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
    ('aliceCo/capture/deleted', 'hourly', date_trunc('hour', now()), '{}', 0, 0, 0),
    ('aliceCo/capture/deleted', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
    ('aliceCo/capture/deleted', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
    ('aliceCo/capture/deleted', 'hourly', date_trunc('hour', now() - '3h'::interval), '{}', 0, 0, 0),
    ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
    ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
    ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '3h'::interval), '{}', 0, 0, 0),
    ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '4h'::interval), '{}', 0, 0, 0),
    ('aliceCo/materialization/disabled', 'hourly', date_trunc('hour', now()), '{}', 0, 0, 0),
    ('aliceCo/materialization/disabled', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
    ('aliceCo/materialization/disabled', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
    ('aliceCo/materialization/disabled', 'hourly', date_trunc('hour', now() - '3h'::interval), '{}', 0, 0, 0);

  --  Assert that the three-hour capture is the only task that is returned by the view and that a row exists for the two subscribed users.
  return query select results_eq(
    $i$ select catalog_name, arguments, alert_type::alert_type, firing from internal.alert_data_movement_stalled $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "evaluation_interval" : "02:00:00", "spec_type" : "capture"}'::jsonb,
      'data_movement_stalled'::alert_type,
      true
    )
    $i$
  );

  update alert_data_processing set evaluation_interval = '2 hours'::interval where catalog_name = 'aliceCo/materialization/four-hours';

  --  Assert that the three-hour capture and the four-hour materialization are the only tasks returned by the view
  -- and that a row exists for the two subscribed users per task.
  return query select results_eq(
    $i$ select catalog_name, arguments, alert_type::alert_type, firing from internal.alert_data_movement_stalled $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "evaluation_interval" : "02:00:00", "spec_type" : "capture"}'::jsonb,
      'data_movement_stalled'::alert_type,
      true
    ),
    (
      'aliceCo/materialization/four-hours'::catalog_name,
      '{"bytes_processed" : 0, "evaluation_interval" : "02:00:00", "spec_type" : "materialization"}',
      'data_movement_stalled'::alert_type,
      true
    )
    $i$
  );

  delete from alert_subscriptions where catalog_prefix = 'aliceCo/' and email = 'bob@example.com';

  --  Assert that the three-hour capture and the four-hour materialization are the only tasks returned by the view
  -- and that a row exists for the only subscribed user.
  return query select results_eq(
    $i$ select catalog_name, arguments, alert_type::alert_type, firing from internal.alert_data_movement_stalled $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "evaluation_interval" : "02:00:00", "spec_type" : "capture"}'::jsonb,
      'data_movement_stalled'::alert_type,
      true
    ),
    (
      'aliceCo/materialization/four-hours'::catalog_name,
      '{"bytes_processed" : 0, "evaluation_interval" : "02:00:00", "spec_type" : "materialization"}'::jsonb,
      'data_movement_stalled'::alert_type,
      true
    )
    $i$
  );

  insert into catalog_stats_hourly (catalog_name, grain, ts, flow_document, bytes_read_by_me) values
    ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now()), '{}', 1024);

  --  Assert that the three-hour capture is the only task that is returned by the view and that a row exists for the only subscribed user.
  return query select results_eq(
    $i$ select catalog_name, arguments, alert_type::alert_type, firing from internal.alert_data_movement_stalled $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "evaluation_interval" : "02:00:00", "spec_type" : "capture"}'::jsonb,
      'data_movement_stalled'::alert_type,
      true
    ) $i$
  );

end;
$$ language plpgsql;

create function tests.test_controller_alerts()
returns setof text as $$
declare
  alerting_id flowid;
begin
  delete from alert_subscriptions;
  delete from alert_history;
  delete from controller_jobs;

  insert into alert_subscriptions (catalog_prefix, email, include_alert_types)
  values ('barbCo/', 'barb@example.com', enum_range(null::alert_type)),
    ('barbCo/', 'bob@example.com', array['data_movement_stalled'::alert_type]);

  insert into auth.users(id, email, raw_user_meta_data) values ('44444444-5555-6666-7777-888888888888', 'barb@example.com', '{"full_name": "Barbara Example"}');

  with insert_live as (
    insert into live_specs (catalog_name, spec_type, spec, created_at) values
      ('barbCo/test-alerting', 'capture', '{
        "endpoint": {
          "connector": {
            "image": "some image",
            "config": {"some": "config"}
          }
        },
        "bindings": []
      }', now() - '1h'::interval),
      ('barbCo/test-ok', 'capture', '{
        "endpoint": {
          "connector": {
            "image": "some image",
            "config": {"some": "config"}
          }
        },
        "bindings": []
      }', now() - '1h'::interval)
      returning id, controller_task_id
  )
  insert into internal.tasks (task_id, task_type)
  select controller_task_id, 2 from insert_live;

  select id into alerting_id from live_specs where catalog_name = 'barbCo/test-alerting';

  -- We'll expect the `data_movement_stalled` and
  -- `data_not_processed_in_interval` alerts to show up in `alert_history`. The
  -- fake alert type should be filtered out.
  insert into controller_jobs (live_spec_id, status) values
  (
    alerting_id,
    '{
      "alerts": {
        "data_movement_stalled": {"state": "firing", "otherArg": "foo"},
        "not_a_real_alert_type": {"state": "firing"},
        "data_not_processed_in_interval": {"state": "firing", "otherArg":"bar"}
      }
    }'::json
  ),
  (
    (select id from live_specs where catalog_name = 'barbCo/test-ok'),
    '{ "someOtherKey": {} }'::json
  );

  return query select results_eq(
    $i$ select ls.catalog_name::text, cj.has_alert
        from live_specs ls
        join controller_jobs cj on ls.id = cj.live_spec_id
        where ls.catalog_name like 'barbCo/%'
        order by ls.catalog_name $i$,
    $i$ values ('barbCo/test-alerting', true), ('barbCo/test-ok', false) $i$,
    'controller_jobs rows should indicate the presence alerts'
  );

  perform internal.evaluate_alert_events();

  return query select results_eq(
    $i$ select catalog_name, alert_type, arguments::jsonb from alert_history $i$,
    $i$ values (
      'barbCo/test-alerting'::catalog_name,
      'data_movement_stalled'::alert_type,
      '{"state": "firing", "otherArg": "foo", "recipients": [{"email": "barb@example.com", "full_name": "Barbara Example"}, {"email": "bob@example.com", "full_name": null}]}'::jsonb
    ),
    (
      'barbCo/test-alerting'::catalog_name,
      'data_not_processed_in_interval'::alert_type,
      '{"state": "firing", "otherArg": "bar", "recipients": [{"email": "barb@example.com", "full_name": "Barbara Example"}]}'::jsonb
    ) $i$,
    'controller alert: data_movement_stalled alert should be created with proper arguments and recipients'
  );

  -- The `data_movement_stalled` alert should resolve because it's no longer present.
  -- The `data_not_processed_in_interval` alert should resolve because it's no longer `firing`.
  update controller_jobs set status = '{
    "alerts": {
      "not_a_real_alert_type": {"state": "firing"},
      "data_not_processed_in_interval": {"state": "pending", "resolvedArg":"moar bar"}
    }
  }' where controller_jobs.live_spec_id = alerting_id;

  perform internal.evaluate_alert_events();

  return query select results_eq(
    $i$ select catalog_name, alert_type::alert_type, resolved_at is not null, resolved_arguments from alert_history $i$,
    $i$ values (
      'barbCo/test-alerting'::catalog_name,
      'data_movement_stalled'::alert_type,
      true,
      '{"state": "firing", "otherArg": "foo", "recipients": [{"email": "barb@example.com", "full_name": "Barbara Example"}, {"email": "bob@example.com", "full_name": null}]}'::jsonb
    ),
    (
          'barbCo/test-alerting'::catalog_name,
          'data_not_processed_in_interval'::alert_type,
          true,
          '{"state": "pending", "resolvedArg": "moar bar", "recipients": [{"email": "barb@example.com", "full_name": "Barbara Example"}]}'::jsonb
    ) $i$,
    'controller alert: alert should be resolved when removed from controller status'
  );

end;
$$ language plpgsql;

create function tests.test_evaluate_alert_events()
returns setof text as $$
begin
  delete from alert_subscriptions;
  delete from alert_data_processing;
  delete from alert_history;

  insert into alert_subscriptions (catalog_prefix, email) values ('aliceCo/', 'alice@example.com'), ('aliceCo/', 'bob@example.com');

  insert into alert_data_processing (catalog_name, evaluation_interval) values
    ('aliceCo/capture/three-hours', '2 hours'),
    ('aliceCo/materialization/four-hours', '4 hours');

  with insert_live as (
    insert into live_specs (catalog_name, spec_type, spec, created_at) values
        ('aliceCo/capture/three-hours', 'capture', '{
            "endpoint": {
            "connector": {
                "image": "some image",
                "config": {"some": "config"}
                }
            },
            "bindings": []
        }', now() - '3h'::interval),
        ('aliceCo/materialization/four-hours', 'materialization', '{
            "endpoint": {
            "connector": {
                "image": "some image",
                "config": {"some": "config"}
                }
            },
            "bindings": []
        }', now() - '4h'::interval)
        returning controller_task_id
  )
  insert into internal.tasks (task_id, task_type)
  select controller_task_id, 2 from insert_live;

  insert into catalog_stats_hourly (catalog_name, grain, ts, flow_document, bytes_written_by_me, bytes_written_to_me, bytes_read_by_me) values
    ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now()), '{}', 0, 0, 0),
    ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
    ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
    ('aliceCo/capture/three-hours', 'hourly', date_trunc('hour', now() - '3h'::interval), '{}', 1024, 0, 0),
    ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '1h'::interval), '{}', 0, 0, 0),
    ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '2h'::interval), '{}', 0, 0, 0),
    ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '3h'::interval), '{}', 0, 0, 0),
    ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now() - '4h'::interval), '{}', 0, 0, 0);

  perform internal.evaluate_alert_events();

  return query select results_eq(
    $i$ select catalog_name, arguments, alert_type::alert_type, fired_at, resolved_at from alert_history $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "capture"}'::jsonb,
      'data_movement_stalled'::alert_type,
      now(),
      null::timestamptz
    ) $i$,
    'standard: capture transitioned from not firing to firing'
  );

  update alert_data_processing set evaluation_interval = '2 hours'::interval where catalog_name = 'aliceCo/materialization/four-hours';

  perform internal.evaluate_alert_events();

  return query select results_eq(
    $i$ select catalog_name, arguments, alert_type::alert_type, fired_at, resolved_at  from alert_history order by catalog_name $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "capture"}'::jsonb,
      'data_movement_stalled'::alert_type,
      now(),
      null::timestamptz
    ),
    (
      'aliceCo/materialization/four-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "materialization"}'::jsonb,
      'data_movement_stalled'::alert_type,
      now(),
      null::timestamptz
    );
    $i$,
   'absent catalog stats record: materialization transitioned from not firing to firing'
  );

  insert into catalog_stats_hourly (catalog_name, grain, ts, flow_document, bytes_read_by_me) values
    ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now()), '{}', 1024);

  perform internal.evaluate_alert_events();

  return query select results_eq(
    $i$ select catalog_name, arguments, alert_type::alert_type, fired_at, resolved_at  from alert_history $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "capture"}'::jsonb,
      'data_movement_stalled'::alert_type,
      now(),
      null::timestamptz
    ),
    (
      'aliceCo/materialization/four-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "materialization"}'::jsonb,
      'data_movement_stalled'::alert_type,
      now(),
      now()
    );
    $i$,
    'absent catalog stats record: materialization transitioned from firing to not firing'
  );

end;
$$ language plpgsql;
