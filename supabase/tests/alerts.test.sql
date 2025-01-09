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
    $i$ select catalog_name, arguments::text, alert_type::alert_type, firing from internal.alert_data_movement_stalled $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "capture"}',
      'data_movement_stalled'::alert_type,
      true
    )
    $i$
  );

  update alert_data_processing set evaluation_interval = '2 hours'::interval where catalog_name = 'aliceCo/materialization/four-hours';

  --  Assert that the three-hour capture and the four-hour materialization are the only tasks returned by the view
  -- and that a row exists for the two subscribed users per task.
  return query select results_eq(
    $i$ select catalog_name, arguments::text, alert_type::alert_type, firing from internal.alert_data_movement_stalled $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "capture"}',
      'data_movement_stalled'::alert_type,
      true
    ),
    (
      'aliceCo/materialization/four-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "materialization"}',
      'data_movement_stalled'::alert_type,
      true
    )
    $i$
  );

  delete from alert_subscriptions where catalog_prefix = 'aliceCo/' and email = 'bob@example.com';

  --  Assert that the three-hour capture and the four-hour materialization are the only tasks returned by the view
  -- and that a row exists for the only subscribed user.
  return query select results_eq(
    $i$ select catalog_name, arguments::text, alert_type::alert_type, firing from internal.alert_data_movement_stalled $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "capture"}',
      'data_movement_stalled'::alert_type,
      true
    ),
    (
      'aliceCo/materialization/four-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "materialization"}',
      'data_movement_stalled'::alert_type,
      true
    )
    $i$
  );

  insert into catalog_stats_hourly (catalog_name, grain, ts, flow_document, bytes_read_by_me) values
    ('aliceCo/materialization/four-hours', 'hourly', date_trunc('hour', now()), '{}', 1024);

  --  Assert that the three-hour capture is the only task that is returned by the view and that a row exists for the only subscribed user.
  return query select results_eq(
    $i$ select catalog_name, arguments::text, alert_type::alert_type, firing from internal.alert_data_movement_stalled $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "capture"}',
      'data_movement_stalled'::alert_type,
      true
    ) $i$
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
    $i$ select catalog_name, arguments::text, alert_type::alert_type, fired_at, resolved_at from alert_history $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "capture"}',
      'data_movement_stalled'::alert_type,
      now(),
      null::timestamptz
    ) $i$,
    'standard: capture transitioned from not firing to firing'
  );

  update alert_data_processing set evaluation_interval = '2 hours'::interval where catalog_name = 'aliceCo/materialization/four-hours';

  perform internal.evaluate_alert_events();

  return query select results_eq(
    $i$ select catalog_name, arguments::text, alert_type::alert_type, fired_at, resolved_at  from alert_history order by catalog_name $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "capture"}',
      'data_movement_stalled'::alert_type,
      now(),
      null::timestamptz
    ),
    (
      'aliceCo/materialization/four-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "materialization"}',
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
    $i$ select catalog_name, arguments::text, alert_type::alert_type, fired_at, resolved_at  from alert_history $i$,
    $i$ values (
      'aliceCo/capture/three-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "capture"}',
      'data_movement_stalled'::alert_type,
      now(),
      null::timestamptz
    ),
    (
      'aliceCo/materialization/four-hours'::catalog_name,
      '{"bytes_processed" : 0, "recipients" : [{"email": "alice@example.com", "full_name": null},{"email": "bob@example.com", "full_name": null}], "evaluation_interval" : "02:00:00", "spec_type" : "materialization"}',
      'data_movement_stalled'::alert_type,
      now(),
      now()
    );
    $i$,
    'absent catalog stats record: materialization transitioned from firing to not firing'
  );

end;
$$ language plpgsql;
