-- Adds a private data plane with populated private-link config and one AWS
-- provisioning result row, plus a `read` grant for Alice on the private
-- prefix so the GraphQL authorization layer can surface it.
--
-- Loaded alongside `data_planes` and `alice` in tests that exercise the
-- typed `privateLinks` field on the dataPlanes query and the
-- `updateDataPlanePrivateLinks` mutation. Kept separate from the shared
-- `data_planes.sql` so its presence does not change every other GraphQL
-- test.
do $$
declare
  alice_private_dp_id flowid := '444444444444';

begin

  -- `hmac_keys` is populated directly (rather than via `encrypted_hmac_keys`)
  -- so the snapshot loader treats this row as live without exercising the
  -- SOPS decrypt path, which other fixtures in this directory already cover.
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
    azure_application_client_id,
    private_links,
    aws_link_endpoints
  ) values (
    alice_private_dp_id,
    'ops/dp/private/aliceCo/aws-us-east-1-c1',
    'dp.private.aliceCo',
    '{c2VjcmV0,b3RoZXI=}',
    '{}',
    'broker.dp.private.aliceCo',
    'reactor.dp.private.aliceCo',
    'ops/tasks/private/aliceCo/logs',
    'ops/tasks/private/aliceCo/stats',
    'ops/rollups/L1/private/aliceCo/events',
    'ops/rollups/L1/private/aliceCo/inferred',
    'ops/rollups/L1/private/aliceCo/stats',
    'from.dp.private.aliceCo',
    'from.dp.private.aliceCo',
    'from.dp.private.aliceCo',
    false,
    '{10.10.0.0/16}',
    'arn:aws:iam::444555666:user/test',
    'test-gcp-private@estuary-test.iam.gserviceaccount.com',
    'estuary-test-app-private',
    '44444444-4444-4444-4444-444444444444',
    array[
      '{"region":"us-east-1","az_ids":["use1-az1","use1-az2"],"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-abc123"}'::json,
      '{"service_name":"/subscriptions/x/resourceGroups/rg/providers/Microsoft.Network/privateLinkServices/svc","location":"eastus","dns_name":"privatelink.database.windows.net"}'::json,
      '{"service_attachment":"projects/p/regions/us-central1/serviceAttachments/sa","region":"us-central1","dns_zone_name":"z","dns_record_names":["r1","r2"],"all_ports":true}'::json
    ],
    array[
      '{"endpoint_id":"vpce-0123456789abcdef0","state":"available"}'::json
    ]
  );

  insert into public.role_grants (subject_role, object_role, capability) values
    ('aliceCo/', 'ops/dp/private/aliceCo/', 'read');

end
$$;
