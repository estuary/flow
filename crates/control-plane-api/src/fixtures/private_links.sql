-- Adds a private data plane plus three configured private links (one per
-- provider) as rows in `data_plane_private_links`: the AWS link is
-- `provisioned` and carries its endpoint details, the Azure and GCP links are
-- still `pending`. One AWS endpoint result is also set on the data_planes row
-- for the legacy `awsLinkEndpoints` field. Grants Alice `read` (plus the
-- `manage_data_plane` bundle) on the private prefix so the GraphQL
-- authorization layer can surface and mutate it.
--
-- Loaded alongside `data_planes` and `alice` by tests that exercise the
-- `privateLinks` field and the per-link CRUD mutations. Kept separate from the
-- shared `data_planes.sql` so its presence does not change every other GraphQL
-- test. Link ids are explicit so snapshots are deterministic.
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
      '{"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-abc123","dns_entries":[{"dns_name":"vpce-0123abc.vpce-svc-abc123.us-east-1.vpce.amazonaws.com","hosted_zone_id":"Z7HUB22EVRPK5"}]}'::json
    ]
  );

  insert into public.data_plane_private_links (id, data_plane_id, provider, config, status, details) values
    (
      '00:00:00:00:00:00:0a:01',
      alice_private_dp_id,
      'aws',
      '{"region":"us-east-1","az_ids":["use1-az1","use1-az2"],"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-abc123"}'::jsonb,
      'provisioned',
      '{"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-abc123","dns_entries":[{"dns_name":"vpce-0123abc.vpce-svc-abc123.us-east-1.vpce.amazonaws.com","hosted_zone_id":"Z7HUB22EVRPK5"}]}'::jsonb
    ),
    (
      '00:00:00:00:00:00:0a:02',
      alice_private_dp_id,
      'azure',
      '{"service_name":"/subscriptions/x/resourceGroups/rg/providers/Microsoft.Network/privateLinkServices/svc","location":"eastus","dns_name":"privatelink.database.windows.net"}'::jsonb,
      'pending',
      null
    ),
    (
      '00:00:00:00:00:00:0a:03',
      alice_private_dp_id,
      'gcp',
      '{"service_attachment":"projects/p/regions/us-central1/serviceAttachments/sa","region":"us-central1","dns_zone_name":"z","dns_record_names":["r1","r2"],"all_ports":true}'::jsonb,
      'pending',
      null
    );

  -- Mirrors what `create_data_plane.rs` installs at provisioning time:
  -- legacy `read` for RLS/`user_roles()`, and the `ManageDataPlane` bundle
  -- for the capability bits.
  insert into public.role_grants (subject_role, object_role, capability, bundles) values
    ('aliceCo/', 'ops/dp/private/aliceCo/', 'read', '{manage_data_plane}');

end
$$;
