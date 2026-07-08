-- Exercises the two triggers on `data_plane_private_links` after the cutover.
-- The `on_data_plane_private_links_change` trigger is now wake-only: on a
-- desired-config change (insert, config/provider update, delete) it wakes the
-- controller task with a `converge` message and no longer projects into the
-- parent data plane's `private_links` column. The
-- `data_plane_private_links_desired_edit` trigger, on a config/provider update,
-- bumps the generation and clears the observation columns. A controller-owned
-- status update (status/details/observed_at only) fires neither: it does not
-- enqueue and does not bump the generation it just pinned.

create function tests.test_data_plane_private_links_trigger()
returns setof text as $$
declare
  v_dp_id flowid := '666666666666';
  v_task_id flowid := '00:00:00:00:00:00:0c:01';
begin

  -- Controller task the data plane points at; a zero heartbeat (the column
  -- default) makes `send_to_task` append to `inbox`.
  insert into internal.tasks (task_id, task_type) values (v_task_id, 1);

  insert into public.data_planes (
    id, data_plane_name, data_plane_fqdn, hmac_keys, encrypted_hmac_keys,
    broker_address, reactor_address, ops_logs_name, ops_stats_name,
    ops_l1_events_name, ops_l1_inferred_name, ops_l1_stats_name,
    ops_l2_events_transform, ops_l2_inferred_transform, ops_l2_stats_transform,
    enable_l2, cidr_blocks, aws_iam_user_arn, gcp_service_account_email,
    azure_application_name, azure_application_client_id, controller_task_id
  ) values (
    v_dp_id, 'ops/dp/private/triggerCo/aws-1', 'dp.private.triggerCo',
    '{c2VjcmV0}', '{}', 'broker.dp.private.triggerCo', 'reactor.dp.private.triggerCo',
    'ops/tasks/private/triggerCo/logs', 'ops/tasks/private/triggerCo/stats',
    'ops/rollups/L1/private/triggerCo/events', 'ops/rollups/L1/private/triggerCo/inferred',
    'ops/rollups/L1/private/triggerCo/stats', 'from.dp.private.triggerCo',
    'from.dp.private.triggerCo', 'from.dp.private.triggerCo', false,
    '{10.30.0.0/16}', 'arn:aws:iam::444555666:user/test',
    'test-gcp@estuary-test.iam.gserviceaccount.com', 'estuary-test-app',
    '66666666-6666-6666-6666-666666666666', v_task_id
  );

  -- Zero the inbox so message-count assertions below start from a clean slate,
  -- independent of anything the data_planes insert itself may have enqueued.
  update internal.tasks set inbox = '{}' where task_id = v_task_id;

  -- Insert a link: post-cutover this wakes the controller but no longer projects
  -- into the column.
  insert into internal.data_plane_private_links (id, data_plane_id, provider, config) values
    ('00:00:00:00:00:00:0d:01', v_dp_id, 'aws',
     '{"region":"us-east-1","az_ids":["a"],"service_name":"svc-x"}'::jsonb);

  return query select is(
    (select array_length(private_links, 1) from public.data_planes where id = v_dp_id),
    null, 'insert no longer projects into private_links');
  return query select is(
    (select array_length(inbox, 1) from internal.tasks where task_id = v_task_id),
    1, 'insert enqueues one converge');
  return query select ok(
    exists(select 1 from internal.tasks t, lateral unnest(t.inbox) m
           where t.task_id = v_task_id and m ->> 1 = 'converge'),
    'the enqueued message is a converge');
  return query select is(
    (select generation from internal.data_plane_private_links where data_plane_id = v_dp_id),
    1::bigint, 'a freshly inserted link starts at generation 1');

  -- A controller-owned status update is outside both triggers' update-of scope,
  -- so it neither wakes the controller (this is what prevents a reconverge loop)
  -- nor bumps the generation the controller pinned for it.
  update internal.data_plane_private_links
     set status = 'provisioned', details = '{}'::jsonb, observed_at = now()
   where data_plane_id = v_dp_id;

  return query select is(
    (select array_length(inbox, 1) from internal.tasks where task_id = v_task_id),
    1, 'a status-only update does not enqueue');
  return query select is(
    (select generation from internal.data_plane_private_links where data_plane_id = v_dp_id),
    1::bigint, 'a status-only update does not bump the generation');

  -- A config change wakes the controller again, and the desired-edit trigger
  -- bumps the generation and clears the observed status set just above.
  update internal.data_plane_private_links
     set config = config || '{"region":"us-west-2"}'::jsonb
   where data_plane_id = v_dp_id;

  return query select is(
    (select array_length(inbox, 1) from internal.tasks where task_id = v_task_id),
    2, 'config update enqueues another converge');
  return query select is(
    (select generation from internal.data_plane_private_links where data_plane_id = v_dp_id),
    2::bigint, 'config update bumps the generation');
  return query select ok(
    (select status = 'pending' and details is null and observed_at is null
       from internal.data_plane_private_links where data_plane_id = v_dp_id),
    'config update resets the observed status columns');

  -- Delete wakes the controller so it tears the endpoint down on the next converge.
  delete from internal.data_plane_private_links where data_plane_id = v_dp_id;

  return query select is(
    (select array_length(inbox, 1) from internal.tasks where task_id = v_task_id),
    3, 'delete enqueues another converge');

end
$$ language plpgsql;
