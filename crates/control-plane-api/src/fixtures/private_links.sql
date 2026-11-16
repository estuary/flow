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
  -- SOPS-encrypted HMAC keys, as in `data_planes.sql`.
  encrypted_keys json := $sops${"hmac_keys":["ENC[AES256_GCM,data:kgzvBY4pczo=,iv:lA0c8tGWTC866p25il82vspY50Kl8oGWqJzNaw/zWyc=,tag:xA68U8wdFjlixqI8/zjQkA==,type:str]","ENC[AES256_GCM,data:q5Y5mbUzwRQ=,iv:bla/xToMmLdlf9N7z9Z2pxyWmpa0lQf9wizKspnyaTU=,tag:CR2QyhoO5crrDM6ZRoPPIw==,type:str]"],"sops":{"age":[{"recipient":"age1z2qskpk8ww2rx5frxzykryufkfm9sj03v56m3qu54y0lgndytp3sdg24vs","enc":"-----BEGIN AGE ENCRYPTED FILE-----\nYWdlLWVuY3J5cHRpb24ub3JnL3YxCi0+IFgyNTUxOSBtZ2ZwQks2T2Zna3h4OFov\nVjJTSzl4TGVyRnIrVk40bndWNVVmZFJQcXdZCkZpY3BlYnRKWUhZYlFnNk50dHh3\nbHlNY0R2bFI1S3pKVkJhbURHM3hYZFEKLS0tIDR0OTMrZEJkRHhKb0FaaXV1K2Uy\nNFd0bHd4TGFMK0xFUUFQVUtta3JHTlEKFBocCSHvSypsA7UcD7emwNDDS4Q3g2si\nqG+pyMMp2yObmT5iN/VAodv4un8Vr1LvcsKxEGXwTOjOnFCAk+I1Kw==\n-----END AGE ENCRYPTED FILE-----\n"}],"lastmodified":"2026-01-15T01:29:57Z","mac":"ENC[AES256_GCM,data:LTZlM4OPKZRu4J9HYUHn5fF6OBP8APXibP47sa/ADpMPA09h1q7Ivg+EAwJlqX9Ee9h8B7iEZD6CMxEyuz94T+nqiklBt4jZIQYq9GPd4xBp1EUvzSITmkvILYkkHh+A+vYU0cH/CemPv/w87wNt+QeNtsQj2xowufsr7RXCyXQ=,iv:z7t+spQvkzLMZZ3LA3eWmtZZdWKPVghKCP5M3HsQ6og=,tag:zrgfUxvTWsRuC8/2kEu6zw==,type:str]","unencrypted_suffix":"_unencrypted","version":"3.11.0"}}$sops$;

begin

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
    '{}',
    encrypted_keys,
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

  insert into internal.data_plane_private_links (id, data_plane_id, config, status, details) values
    (
      '00:00:00:00:00:00:0a:01',
      alice_private_dp_id,
      '{"region":"us-east-1","az_ids":["use1-az1","use1-az2"],"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-abc123"}'::jsonb,
      'provisioned',
      '{"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-abc123","dns_entries":[{"dns_name":"vpce-0123abc.vpce-svc-abc123.us-east-1.vpce.amazonaws.com","hosted_zone_id":"Z7HUB22EVRPK5"}]}'::jsonb
    ),
    (
      '00:00:00:00:00:00:0a:02',
      alice_private_dp_id,
      '{"service_name":"/subscriptions/x/resourceGroups/rg/providers/Microsoft.Network/privateLinkServices/svc","location":"eastus","dns_name":"privatelink.database.windows.net"}'::jsonb,
      'pending',
      null
    ),
    (
      '00:00:00:00:00:00:0a:03',
      alice_private_dp_id,
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
