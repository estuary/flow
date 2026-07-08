-- Seeds a private data plane and five private links whose providers, statuses,
-- and `updated_at` values exercise every branch of `write_private_link_statuses`:
--   svc-a       aws, pending,     old   -> matched by a published AWS endpoint
--   svc-orphan  aws, provisioned, old   -> AWS published but unmatched (demoted)
--   svc-edited  aws, provisioned, recent -> edited after the read instant (skipped)
--   svc-az      azure, pending,   old   -> Azure published nothing (untouched)
--   svc-g       gcp, provisioned, old   -> GCP published nothing (untouched)
-- The data plane has no controller_task_id, so the change trigger's wake is a
-- no-op here; only its projection into data_planes.private_links runs.
insert into public.data_planes (
    id,
    data_plane_name,
    data_plane_fqdn,
    hmac_keys,
    encrypted_hmac_keys,
    broker_address,
    reactor_address,
    ops_logs_name,
    ops_stats_name,
    ops_l1_events_name,
    ops_l1_inferred_name,
    ops_l1_stats_name,
    ops_l2_events_transform,
    ops_l2_inferred_transform,
    ops_l2_stats_transform,
    enable_l2,
    cidr_blocks,
    aws_iam_user_arn,
    gcp_service_account_email,
    azure_application_name,
    azure_application_client_id
) values (
    '555555555555',
    'ops/dp/private/testCo/aws-1',
    'dp.private.testCo',
    '{c2VjcmV0}',
    '{}',
    'broker.dp.private.testCo',
    'reactor.dp.private.testCo',
    'ops/tasks/private/testCo/logs',
    'ops/tasks/private/testCo/stats',
    'ops/rollups/L1/private/testCo/events',
    'ops/rollups/L1/private/testCo/inferred',
    'ops/rollups/L1/private/testCo/stats',
    'from.dp.private.testCo',
    'from.dp.private.testCo',
    'from.dp.private.testCo',
    false,
    '{10.20.0.0/16}',
    'arn:aws:iam::444555666:user/test',
    'test-gcp-private@estuary-test.iam.gserviceaccount.com',
    'estuary-test-app-private',
    '55555555-5555-5555-5555-555555555555'
);

insert into internal.data_plane_private_links
    (id, data_plane_id, provider, config, status, details, updated_at) values
    (
        '00:00:00:00:00:00:0b:01', '555555555555', 'aws',
        '{"region":"us-east-1","az_ids":["a"],"service_name":"svc-a"}'::jsonb,
        'pending', null, '2020-01-01T00:00:00Z'
    ),
    (
        '00:00:00:00:00:00:0b:02', '555555555555', 'aws',
        '{"region":"us-east-1","az_ids":["a"],"service_name":"svc-edited"}'::jsonb,
        'provisioned', '{"service_name":"svc-edited","stale":true}'::jsonb, '2020-06-01T00:00:00Z'
    ),
    (
        '00:00:00:00:00:00:0b:03', '555555555555', 'azure',
        '{"service_name":"svc-az","location":"eastus"}'::jsonb,
        'pending', null, '2020-01-01T00:00:00Z'
    ),
    (
        '00:00:00:00:00:00:0b:04', '555555555555', 'gcp',
        '{"service_attachment":"svc-g","region":"r","dns_zone_name":"z","dns_record_names":["n"]}'::jsonb,
        'provisioned', '{"service_attachment":"svc-g"}'::jsonb, '2020-01-01T00:00:00Z'
    ),
    (
        '00:00:00:00:00:00:0b:05', '555555555555', 'aws',
        '{"region":"us-east-1","az_ids":["a"],"service_name":"svc-orphan"}'::jsonb,
        'provisioned', '{"service_name":"svc-orphan"}'::jsonb, '2020-01-01T00:00:00Z'
    );
