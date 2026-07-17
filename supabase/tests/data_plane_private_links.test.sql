-- Exercises the `data_plane_private_links_desired_edit` trigger: a
-- config update bumps the generation and clears the observation
-- columns, for any writer. A controller-owned status update
-- (status/details/observed_at only) does not fire it: it must not bump the
-- generation the controller just pinned. Table changes also never project into
-- the parent data plane's legacy `private_links` column, which stays frozen
-- until it is dropped.

create function tests.test_data_plane_private_links_trigger()
returns setof text as $$
declare
  v_dp_id flowid := '666666666666';
begin

  insert into public.data_planes (
    id, data_plane_name, data_plane_fqdn, hmac_keys, encrypted_hmac_keys,
    broker_address, reactor_address, ops_logs_name, ops_stats_name,
    ops_l1_events_name, ops_l1_inferred_name, ops_l1_stats_name,
    ops_l2_events_transform, ops_l2_inferred_transform, ops_l2_stats_transform,
    enable_l2, cidr_blocks, aws_iam_user_arn, gcp_service_account_email,
    azure_application_name, azure_application_client_id
  ) values (
    v_dp_id, 'ops/dp/private/triggerCo/aws-1', 'dp.private.triggerCo',
    '{c2VjcmV0}', '{}', 'broker.dp.private.triggerCo', 'reactor.dp.private.triggerCo',
    'ops/tasks/private/triggerCo/logs', 'ops/tasks/private/triggerCo/stats',
    'ops/rollups/L1/private/triggerCo/events', 'ops/rollups/L1/private/triggerCo/inferred',
    'ops/rollups/L1/private/triggerCo/stats', 'from.dp.private.triggerCo',
    'from.dp.private.triggerCo', 'from.dp.private.triggerCo', false,
    '{10.30.0.0/16}', 'arn:aws:iam::444555666:user/test',
    'test-gcp@estuary-test.iam.gserviceaccount.com', 'estuary-test-app',
    '66666666-6666-6666-6666-666666666666'
  );

  insert into internal.data_plane_private_links (id, data_plane_id, config) values
    ('00:00:00:00:00:00:0d:01', v_dp_id,
     '{"region":"us-east-1","az_ids":["a"],"service_name":"svc-x"}'::jsonb);

  return query select is(
    (select array_length(private_links, 1) from public.data_planes where id = v_dp_id),
    null, 'insert does not project into the legacy private_links column');
  return query select is(
    (select generation from internal.data_plane_private_links where data_plane_id = v_dp_id),
    1::bigint, 'a freshly inserted link starts at generation 1');
  return query select is(
    (select provider from internal.data_plane_private_links where data_plane_id = v_dp_id),
    'aws', 'provider is generated from config');

  -- A controller-owned status update is outside the trigger's update-of scope,
  -- so it does not bump the generation the controller pinned for it.
  update internal.data_plane_private_links
     set status = 'failed', details = '{}'::jsonb, error = 'boom', observed_at = now()
   where data_plane_id = v_dp_id;

  return query select is(
    (select generation from internal.data_plane_private_links where data_plane_id = v_dp_id),
    1::bigint, 'a status-only update does not bump the generation');

  -- Assigning the existing config is a no-op: it must not invalidate a healthy
  -- observation or schedule unnecessary infrastructure work.
  update internal.data_plane_private_links
     set config = config
   where data_plane_id = v_dp_id;

  return query select ok(
    (select generation = 1 and status = 'failed' and error = 'boom'
            and details is not null and observed_at is not null
       from internal.data_plane_private_links where data_plane_id = v_dp_id),
    'an identical config update preserves generation and observation');

  -- An actual config change bumps the generation and clears the observed
  -- status set just above.
  update internal.data_plane_private_links
     set config = config || '{"region":"us-west-2"}'::jsonb
   where data_plane_id = v_dp_id;

  return query select is(
    (select generation from internal.data_plane_private_links where data_plane_id = v_dp_id),
    2::bigint, 'config update bumps the generation');
  return query select ok(
    (select status = 'pending' and details is null and error is null and observed_at is null
       from internal.data_plane_private_links where data_plane_id = v_dp_id),
    'config update resets the observed status columns');

end
$$ language plpgsql;
