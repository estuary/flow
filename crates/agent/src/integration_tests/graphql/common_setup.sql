-- Common setup for GraphQL integration tests.
-- Equivalent to common_setup() in mod.rs.
--
-- This migration assumes that TestHarness::init() has already run, which:
--   - Truncates all tables
--   - Sets up test connectors (source/test, materialize/test)
--   - Creates the data plane ops/dp/public/test
--   - Creates alert automation tasks
--
-- ID scheme (all flowids are macaddr8):
--   Publication IDs:
--     pub1  = '10:10:10:10:10:10:10:01'  Alice's initial catalog
--     pub2  = '10:10:10:10:10:10:10:02'  Disable capture
--     pub3  = '10:10:10:10:10:10:10:03'  Bob's materialization v0
--     pub4  = '10:10:10:10:10:10:10:04'  Bob's materialization v1
--     pub5  = '10:10:10:10:10:10:10:05'  Bob's materialization v2
--     pub6  = '10:10:10:10:10:10:10:06'  Bob's materialization v3
--     pub7  = '10:10:10:10:10:10:10:07'  Bob's materialization v4
--
--   Live spec IDs:
--     spec_a            = '20:20:20:20:20:20:20:01'  aliceCo/shared/a
--     spec_b            = '20:20:20:20:20:20:20:02'  aliceCo/shared/b
--     spec_c            = '20:20:20:20:20:20:20:03'  aliceCo/shared/c
--     spec_cap_shared   = '20:20:20:20:20:20:20:04'  aliceCo/shared/capture
--     spec_cap_private  = '20:20:20:20:20:20:20:05'  aliceCo/private/capture
--     spec_cap_disabled = '20:20:20:20:20:20:20:06'  aliceCo/shared/disabled
--     spec_mat_alice    = '20:20:20:20:20:20:20:07'  aliceCo/shared/materialize
--     spec_mat_bob      = '20:20:20:20:20:20:20:08'  bobCo/private/materialization
--
--   Controller task IDs:
--     ctrl_01..ctrl_08  = '30:30:30:30:30:30:30:01'..'30:30:30:30:30:30:30:08'
--
--   User IDs:
--     alice = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
--     bob   = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'

-- ============================================================
-- Auth users
-- ============================================================
INSERT INTO auth.users (id, email, raw_user_meta_data)
VALUES
  ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'aliceCo@graphql_tests.test',
   '{"picture": "http://aliceCo.test/avatar", "full_name": "Full (aliceCo) Name"}'),
  ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'bobCo@graphql_tests.test',
   '{"picture": "http://bobCo.test/avatar", "full_name": "Full (bobCo) Name"}');

-- ============================================================
-- Tenants (trigger auto-creates estuary_support/ role grants)
-- ============================================================
INSERT INTO tenants (tenant, detail, created_at, updated_at)
VALUES
  ('aliceCo/', 'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),
  ('bobCo/',   'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z');

-- Remove auto-inserted estuary_support/ role grants (same as harness)
DELETE FROM role_grants WHERE subject_role = 'estuary_support/';

-- ============================================================
-- User grants
-- ============================================================
INSERT INTO user_grants (user_id, object_role, capability, detail, created_at, updated_at)
VALUES
  ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'aliceCo/', 'admin', 'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),
  ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'bobCo/',   'admin', 'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z');

-- ============================================================
-- Role grants
-- ============================================================
INSERT INTO role_grants (subject_role, object_role, capability, detail, created_at, updated_at)
VALUES
  -- Alice's tenant self-grants
  ('aliceCo/', 'aliceCo/',        'write', 'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),
  ('aliceCo/', 'ops/dp/public/',  'read',  'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),
  -- Bob's tenant self-grants
  ('bobCo/',   'bobCo/',          'write', 'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),
  ('bobCo/',   'ops/dp/public/',  'read',  'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),
  -- Bob can read Alice's shared prefix
  ('bobCo/',   'aliceCo/shared/', 'read',  NULL,                      '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z');

-- ============================================================
-- Storage mappings
-- ============================================================
INSERT INTO storage_mappings (catalog_prefix, spec, detail, created_at, updated_at)
VALUES
  ('aliceCo/', json_build_object(
    'stores', '[{"provider": "GCS", "bucket": "estuary-trial", "prefix": "collection-data/"}]'::json,
    'data_planes', (SELECT json_agg(data_plane_name) FROM data_planes WHERE starts_with(data_plane_name, 'ops/dp/public/'))
  ), 'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),
  ('recovery/aliceCo/', '{"stores": [{"provider": "GCS", "bucket": "estuary-trial"}]}', 'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),
  ('bobCo/', json_build_object(
    'stores', '[{"provider": "GCS", "bucket": "estuary-trial", "prefix": "collection-data/"}]'::json,
    'data_planes', (SELECT json_agg(data_plane_name) FROM data_planes WHERE starts_with(data_plane_name, 'ops/dp/public/'))
  ), 'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),
  ('recovery/bobCo/', '{"stores": [{"provider": "GCS", "bucket": "estuary-trial"}]}', 'for test: graphql_tests', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z');

-- ============================================================
-- Alert subscriptions
-- ============================================================
INSERT INTO alert_subscriptions (catalog_prefix, email, created_at, updated_at)
VALUES
  ('aliceCo/', 'aliceCo@graphql_tests.test', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),
  ('bobCo/',   'bobCo@graphql_tests.test',   '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z');

-- ============================================================
-- Internal tasks for controller task IDs
-- ============================================================
INSERT INTO internal.tasks (task_id, task_type, wake_at)
VALUES
  ('30:30:30:30:30:30:30:01', 2, NULL),
  ('30:30:30:30:30:30:30:02', 2, NULL),
  ('30:30:30:30:30:30:30:03', 2, NULL),
  ('30:30:30:30:30:30:30:04', 2, NULL),
  ('30:30:30:30:30:30:30:05', 2, NULL),
  ('30:30:30:30:30:30:30:06', 2, NULL),
  ('30:30:30:30:30:30:30:07', 2, NULL),
  ('30:30:30:30:30:30:30:08', 2, NULL);

-- ============================================================
-- Live specs (final state after all publications)
-- ============================================================
--
-- Publication history:
--   pub1: Alice's initial catalog (7 specs: 3 collections, 3 captures, 1 materialization)
--   pub2: Disable aliceCo/shared/disabled capture
--   pub3-pub7: Bob's materialization v0-v4 (each touches collections a,b,c as dependencies)
--
-- last_pub_id: the publication that last changed the spec itself
-- last_build_id: the publication that last built/touched the spec
--   Collections were touched in pub2 (disabled capture rebuild) and pub3-7 (Bob's mat deps)
--   so their last_build_id = pub7.

INSERT INTO live_specs (
  id, catalog_name, spec_type, spec, built_spec,
  last_pub_id, last_build_id,
  reads_from, writes_to,
  connector_image_name, connector_image_tag,
  data_plane_id, controller_task_id,
  created_at, updated_at
)
VALUES
  -- aliceCo/shared/a (collection)
  ('20:20:20:20:20:20:20:01', 'aliceCo/shared/a', 'collection',
   '{"schema": {"type": "object", "properties": {"id": {"type": "string"}}}, "key": ["/id"]}',
   '{"partitionTemplate": {"name": "aliceCo/shared/a"}}',
   '10:10:10:10:10:10:10:01', '10:10:10:10:10:10:10:07',
   NULL, NULL,
   NULL, NULL,
   (SELECT id FROM data_planes WHERE data_plane_name = 'ops/dp/public/test'),
   '30:30:30:30:30:30:30:01',
   '2024-01-01T00:00:00Z', '2024-01-01T00:06:00Z'),

  -- aliceCo/shared/b (collection)
  ('20:20:20:20:20:20:20:02', 'aliceCo/shared/b', 'collection',
   '{"schema": {"type": "object", "properties": {"id": {"type": "string"}}}, "key": ["/id"]}',
   '{"partitionTemplate": {"name": "aliceCo/shared/b"}}',
   '10:10:10:10:10:10:10:01', '10:10:10:10:10:10:10:07',
   NULL, NULL,
   NULL, NULL,
   (SELECT id FROM data_planes WHERE data_plane_name = 'ops/dp/public/test'),
   '30:30:30:30:30:30:30:02',
   '2024-01-01T00:00:00Z', '2024-01-01T00:06:00Z'),

  -- aliceCo/shared/c (collection)
  ('20:20:20:20:20:20:20:03', 'aliceCo/shared/c', 'collection',
   '{"schema": {"type": "object", "properties": {"id": {"type": "string"}}}, "key": ["/id"]}',
   '{"partitionTemplate": {"name": "aliceCo/shared/c"}}',
   '10:10:10:10:10:10:10:01', '10:10:10:10:10:10:10:07',
   NULL, NULL,
   NULL, NULL,
   (SELECT id FROM data_planes WHERE data_plane_name = 'ops/dp/public/test'),
   '30:30:30:30:30:30:30:03',
   '2024-01-01T00:00:00Z', '2024-01-01T00:06:00Z'),

  -- aliceCo/shared/capture (capture, enabled)
  ('20:20:20:20:20:20:20:04', 'aliceCo/shared/capture', 'capture',
   '{"endpoint": {"connector": {"image": "source/test:test", "config": {}}}, "bindings": [{"resource": {"id": "a", "_meta": {"path": ["a"]}}, "target": "aliceCo/shared/a"}, {"resource": {"id": "b", "_meta": {"path": ["b"]}}, "target": "aliceCo/shared/b"}, {"resource": {"id": "c", "_meta": {"path": ["c"]}}, "target": "aliceCo/shared/c"}]}',
   '{"shardTemplate": {"id": "aliceCo/shared/capture"}}',
   '10:10:10:10:10:10:10:01', '10:10:10:10:10:10:10:01',
   NULL, '{"aliceCo/shared/a","aliceCo/shared/b","aliceCo/shared/c"}',
   'source/test', ':test',
   (SELECT id FROM data_planes WHERE data_plane_name = 'ops/dp/public/test'),
   '30:30:30:30:30:30:30:04',
   '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),

  -- aliceCo/private/capture (capture, enabled)
  ('20:20:20:20:20:20:20:05', 'aliceCo/private/capture', 'capture',
   '{"endpoint": {"connector": {"image": "source/test:test", "config": {}}}, "bindings": [{"resource": {"id": "a", "_meta": {"path": ["a"]}}, "target": "aliceCo/shared/a"}, {"resource": {"id": "b", "_meta": {"path": ["b"]}}, "target": "aliceCo/shared/b"}, {"resource": {"id": "c", "_meta": {"path": ["c"]}}, "target": "aliceCo/shared/c"}]}',
   '{"shardTemplate": {"id": "aliceCo/private/capture"}}',
   '10:10:10:10:10:10:10:01', '10:10:10:10:10:10:10:01',
   NULL, '{"aliceCo/shared/a","aliceCo/shared/b","aliceCo/shared/c"}',
   'source/test', ':test',
   (SELECT id FROM data_planes WHERE data_plane_name = 'ops/dp/public/test'),
   '30:30:30:30:30:30:30:05',
   '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),

  -- aliceCo/shared/disabled (capture, disabled in pub2)
  -- Disabled bindings produce empty writes_to; no live_spec_flows entries.
  ('20:20:20:20:20:20:20:06', 'aliceCo/shared/disabled', 'capture',
   '{"endpoint": {"connector": {"image": "source/test:test", "config": {}}}, "bindings": [{"disable": true, "resource": {"id": "a", "_meta": {"path": ["a"]}}, "target": "aliceCo/shared/a"}, {"disable": true, "resource": {"id": "b", "_meta": {"path": ["b"]}}, "target": "aliceCo/shared/b"}, {"disable": true, "resource": {"id": "c", "_meta": {"path": ["c"]}}, "target": "aliceCo/shared/c"}], "shards": {"disable": true}}',
   '{"shardTemplate": {"id": "aliceCo/shared/disabled"}}',
   '10:10:10:10:10:10:10:02', '10:10:10:10:10:10:10:02',
   NULL, NULL,
   'source/test', ':test',
   (SELECT id FROM data_planes WHERE data_plane_name = 'ops/dp/public/test'),
   '30:30:30:30:30:30:30:06',
   '2024-01-01T00:00:00Z', '2024-01-01T00:01:00Z'),

  -- aliceCo/shared/materialize (materialization)
  ('20:20:20:20:20:20:20:07', 'aliceCo/shared/materialize', 'materialization',
   '{"endpoint": {"connector": {"image": "materialize/test:test", "config": {}}}, "bindings": [{"fields": {"recommended": true}, "resource": {"id": "a", "_meta": {"path": ["a"]}}, "source": "aliceCo/shared/a"}, {"fields": {"recommended": true}, "resource": {"id": "b", "_meta": {"path": ["b"]}}, "source": "aliceCo/shared/b"}, {"fields": {"recommended": true}, "resource": {"id": "c", "_meta": {"path": ["c"]}}, "source": "aliceCo/shared/c"}]}',
   '{"shardTemplate": {"id": "aliceCo/shared/materialize"}}',
   '10:10:10:10:10:10:10:01', '10:10:10:10:10:10:10:01',
   '{"aliceCo/shared/a","aliceCo/shared/b","aliceCo/shared/c"}', NULL,
   'materialize/test', ':test',
   (SELECT id FROM data_planes WHERE data_plane_name = 'ops/dp/public/test'),
   '30:30:30:30:30:30:30:07',
   '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z'),

  -- bobCo/private/materialization (materialization, final state = v4)
  ('20:20:20:20:20:20:20:08', 'bobCo/private/materialization', 'materialization',
   '{"endpoint": {"connector": {"image": "materialize/test:test", "config": {"publication": 4}}}, "bindings": [{"fields": {"recommended": true}, "resource": {"id": "a", "_meta": {"path": ["a"]}}, "source": "aliceCo/shared/a"}, {"fields": {"recommended": true}, "resource": {"id": "b", "_meta": {"path": ["b"]}}, "source": "aliceCo/shared/b"}, {"fields": {"recommended": true}, "resource": {"id": "c", "_meta": {"path": ["c"]}}, "source": "aliceCo/shared/c"}]}',
   '{"shardTemplate": {"id": "bobCo/private/materialization"}}',
   '10:10:10:10:10:10:10:07', '10:10:10:10:10:10:10:07',
   '{"aliceCo/shared/a","aliceCo/shared/b","aliceCo/shared/c"}', NULL,
   'materialize/test', ':test',
   (SELECT id FROM data_planes WHERE data_plane_name = 'ops/dp/public/test'),
   '30:30:30:30:30:30:30:08',
   '2024-01-01T00:02:00Z', '2024-01-01T00:06:00Z');

-- ============================================================
-- Controller jobs
-- ============================================================
-- Collections: status OK (activation matches last_build_id, no shards)
-- Enabled captures/materializations: status WARNING (shard_status = Pending)
-- Disabled capture: status TaskDisabled (activation matches, spec.shards.disable = true)

INSERT INTO controller_jobs (live_spec_id, controller_version, status, updated_at)
VALUES
  -- Collections (OK)
  ('20:20:20:20:20:20:20:01', 1,
   '{"type": "Collection", "inferredSchema": null, "publications": {"maxObservedPubId": "10:10:10:10:10:10:10:07", "history": []}, "activation": {"lastActivated": "10:10:10:10:10:10:10:07", "lastActivatedAt": "2024-01-01T00:07:00Z"}, "alerts": {}}',
   '2024-01-01T00:07:00Z'),
  ('20:20:20:20:20:20:20:02', 1,
   '{"type": "Collection", "inferredSchema": null, "publications": {"maxObservedPubId": "10:10:10:10:10:10:10:07", "history": []}, "activation": {"lastActivated": "10:10:10:10:10:10:10:07", "lastActivatedAt": "2024-01-01T00:07:00Z"}, "alerts": {}}',
   '2024-01-01T00:07:00Z'),
  ('20:20:20:20:20:20:20:03', 1,
   '{"type": "Collection", "inferredSchema": null, "publications": {"maxObservedPubId": "10:10:10:10:10:10:10:07", "history": []}, "activation": {"lastActivated": "10:10:10:10:10:10:10:07", "lastActivatedAt": "2024-01-01T00:07:00Z"}, "alerts": {}}',
   '2024-01-01T00:07:00Z'),

  -- aliceCo/shared/capture (WARNING: shards pending)
  ('20:20:20:20:20:20:20:04', 1,
   '{"type": "Capture", "publications": {"maxObservedPubId": "10:10:10:10:10:10:10:01", "history": []}, "activation": {"lastActivated": "10:10:10:10:10:10:10:01", "lastActivatedAt": "2024-01-01T00:07:00Z", "shardStatus": {"count": 1, "status": "Pending", "firstTs": "2024-01-01T00:07:00Z", "lastTs": "2024-01-01T00:07:00Z"}}, "alerts": {}}',
   '2024-01-01T00:07:00Z'),

  -- aliceCo/private/capture (WARNING: shards pending)
  ('20:20:20:20:20:20:20:05', 1,
   '{"type": "Capture", "publications": {"maxObservedPubId": "10:10:10:10:10:10:10:01", "history": []}, "activation": {"lastActivated": "10:10:10:10:10:10:10:01", "lastActivatedAt": "2024-01-01T00:07:00Z", "shardStatus": {"count": 1, "status": "Pending", "firstTs": "2024-01-01T00:07:00Z", "lastTs": "2024-01-01T00:07:00Z"}}, "alerts": {}}',
   '2024-01-01T00:07:00Z'),

  -- aliceCo/shared/disabled (TaskDisabled: spec.shards.disable = true)
  ('20:20:20:20:20:20:20:06', 1,
   '{"type": "Capture", "publications": {"maxObservedPubId": "10:10:10:10:10:10:10:02", "history": []}, "activation": {"lastActivated": "10:10:10:10:10:10:10:02", "lastActivatedAt": "2024-01-01T00:07:00Z"}, "alerts": {}}',
   '2024-01-01T00:07:00Z'),

  -- aliceCo/shared/materialize (WARNING: shards pending)
  ('20:20:20:20:20:20:20:07', 1,
   '{"type": "Materialization", "publications": {"maxObservedPubId": "10:10:10:10:10:10:10:01", "history": []}, "activation": {"lastActivated": "10:10:10:10:10:10:10:01", "lastActivatedAt": "2024-01-01T00:07:00Z", "shardStatus": {"count": 1, "status": "Pending", "firstTs": "2024-01-01T00:07:00Z", "lastTs": "2024-01-01T00:07:00Z"}}, "alerts": {}}',
   '2024-01-01T00:07:00Z'),

  -- bobCo/private/materialization (WARNING: shards pending)
  ('20:20:20:20:20:20:20:08', 1,
   '{"type": "Materialization", "publications": {"maxObservedPubId": "10:10:10:10:10:10:10:07", "history": []}, "activation": {"lastActivated": "10:10:10:10:10:10:10:07", "lastActivatedAt": "2024-01-01T00:07:00Z", "shardStatus": {"count": 1, "status": "Pending", "firstTs": "2024-01-01T00:07:00Z", "lastTs": "2024-01-01T00:07:00Z"}}, "alerts": {}}',
   '2024-01-01T00:07:00Z');

-- ============================================================
-- Live spec flows (final state)
-- ============================================================
-- Captures write to collections:
--   source_id = capture, target_id = collection, flow_type = 'capture'
-- Materializations read from collections:
--   source_id = collection, target_id = materialization, flow_type = 'materialization'
--
-- The disabled capture has NO flow entries (all bindings disabled).

INSERT INTO live_spec_flows (source_id, target_id, flow_type)
VALUES
  -- aliceCo/shared/capture writes to a, b, c
  ('20:20:20:20:20:20:20:04', '20:20:20:20:20:20:20:01', 'capture'),
  ('20:20:20:20:20:20:20:04', '20:20:20:20:20:20:20:02', 'capture'),
  ('20:20:20:20:20:20:20:04', '20:20:20:20:20:20:20:03', 'capture'),

  -- aliceCo/private/capture writes to a, b, c
  ('20:20:20:20:20:20:20:05', '20:20:20:20:20:20:20:01', 'capture'),
  ('20:20:20:20:20:20:20:05', '20:20:20:20:20:20:20:02', 'capture'),
  ('20:20:20:20:20:20:20:05', '20:20:20:20:20:20:20:03', 'capture'),

  -- aliceCo/shared/materialize reads from a, b, c
  ('20:20:20:20:20:20:20:01', '20:20:20:20:20:20:20:07', 'materialization'),
  ('20:20:20:20:20:20:20:02', '20:20:20:20:20:20:20:07', 'materialization'),
  ('20:20:20:20:20:20:20:03', '20:20:20:20:20:20:20:07', 'materialization'),

  -- bobCo/private/materialization reads from a, b, c
  ('20:20:20:20:20:20:20:01', '20:20:20:20:20:20:20:08', 'materialization'),
  ('20:20:20:20:20:20:20:02', '20:20:20:20:20:20:20:08', 'materialization'),
  ('20:20:20:20:20:20:20:03', '20:20:20:20:20:20:20:08', 'materialization');

-- ============================================================
-- Publication specs (full publication history)
-- ============================================================
-- Each publication creates a row per spec that was directly published (not just touched).
-- "Touched" specs (rebuilt as dependencies but not changed) do NOT get publication_specs rows.

-- Pub 1: Alice's initial catalog (7 specs)
INSERT INTO publication_specs (live_spec_id, pub_id, detail, published_at, spec, spec_type, user_id)
VALUES
  -- Collections
  ('20:20:20:20:20:20:20:01', '10:10:10:10:10:10:10:01', 'Alice''s initial catalog', '2024-01-01T00:00:00Z',
   '{"schema": {"type": "object", "properties": {"id": {"type": "string"}}}, "key": ["/id"]}',
   'collection', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'),
  ('20:20:20:20:20:20:20:02', '10:10:10:10:10:10:10:01', 'Alice''s initial catalog', '2024-01-01T00:00:00Z',
   '{"schema": {"type": "object", "properties": {"id": {"type": "string"}}}, "key": ["/id"]}',
   'collection', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'),
  ('20:20:20:20:20:20:20:03', '10:10:10:10:10:10:10:01', 'Alice''s initial catalog', '2024-01-01T00:00:00Z',
   '{"schema": {"type": "object", "properties": {"id": {"type": "string"}}}, "key": ["/id"]}',
   'collection', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'),

  -- Captures (all initially enabled)
  ('20:20:20:20:20:20:20:04', '10:10:10:10:10:10:10:01', 'Alice''s initial catalog', '2024-01-01T00:00:00Z',
   '{"endpoint": {"connector": {"image": "source/test:test", "config": {}}}, "bindings": [{"resource": {"id": "a", "_meta": {"path": ["a"]}}, "target": "aliceCo/shared/a"}, {"resource": {"id": "b", "_meta": {"path": ["b"]}}, "target": "aliceCo/shared/b"}, {"resource": {"id": "c", "_meta": {"path": ["c"]}}, "target": "aliceCo/shared/c"}]}',
   'capture', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'),
  ('20:20:20:20:20:20:20:05', '10:10:10:10:10:10:10:01', 'Alice''s initial catalog', '2024-01-01T00:00:00Z',
   '{"endpoint": {"connector": {"image": "source/test:test", "config": {}}}, "bindings": [{"resource": {"id": "a", "_meta": {"path": ["a"]}}, "target": "aliceCo/shared/a"}, {"resource": {"id": "b", "_meta": {"path": ["b"]}}, "target": "aliceCo/shared/b"}, {"resource": {"id": "c", "_meta": {"path": ["c"]}}, "target": "aliceCo/shared/c"}]}',
   'capture', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'),
  ('20:20:20:20:20:20:20:06', '10:10:10:10:10:10:10:01', 'Alice''s initial catalog', '2024-01-01T00:00:00Z',
   '{"endpoint": {"connector": {"image": "source/test:test", "config": {}}}, "bindings": [{"resource": {"id": "a", "_meta": {"path": ["a"]}}, "target": "aliceCo/shared/a"}, {"resource": {"id": "b", "_meta": {"path": ["b"]}}, "target": "aliceCo/shared/b"}, {"resource": {"id": "c", "_meta": {"path": ["c"]}}, "target": "aliceCo/shared/c"}]}',
   'capture', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'),

  -- Materialization
  ('20:20:20:20:20:20:20:07', '10:10:10:10:10:10:10:01', 'Alice''s initial catalog', '2024-01-01T00:00:00Z',
   '{"endpoint": {"connector": {"image": "materialize/test:test", "config": {}}}, "bindings": [{"fields": {"recommended": true}, "resource": {"id": "a", "_meta": {"path": ["a"]}}, "source": "aliceCo/shared/a"}, {"fields": {"recommended": true}, "resource": {"id": "b", "_meta": {"path": ["b"]}}, "source": "aliceCo/shared/b"}, {"fields": {"recommended": true}, "resource": {"id": "c", "_meta": {"path": ["c"]}}, "source": "aliceCo/shared/c"}]}',
   'materialization', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa');

-- Pub 2: Disable aliceCo/shared/disabled
INSERT INTO publication_specs (live_spec_id, pub_id, detail, published_at, spec, spec_type, user_id)
VALUES
  ('20:20:20:20:20:20:20:06', '10:10:10:10:10:10:10:02', 'Disable capture', '2024-01-01T00:01:00Z',
   '{"endpoint": {"connector": {"image": "source/test:test", "config": {}}}, "bindings": [{"disable": true, "resource": {"id": "a", "_meta": {"path": ["a"]}}, "target": "aliceCo/shared/a"}, {"disable": true, "resource": {"id": "b", "_meta": {"path": ["b"]}}, "target": "aliceCo/shared/b"}, {"disable": true, "resource": {"id": "c", "_meta": {"path": ["c"]}}, "target": "aliceCo/shared/c"}], "shards": {"disable": true}}',
   'capture', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa');

-- Pub 3: Bob's materialization v0
INSERT INTO publication_specs (live_spec_id, pub_id, detail, published_at, spec, spec_type, user_id)
VALUES
  ('20:20:20:20:20:20:20:08', '10:10:10:10:10:10:10:03', 'Bob''s materialization v0', '2024-01-01T00:02:00Z',
   '{"endpoint": {"connector": {"image": "materialize/test:test", "config": {"publication": 0}}}, "bindings": [{"fields": {"recommended": true}, "resource": {"id": "a", "_meta": {"path": ["a"]}}, "source": "aliceCo/shared/a"}, {"fields": {"recommended": true}, "resource": {"id": "b", "_meta": {"path": ["b"]}}, "source": "aliceCo/shared/b"}, {"fields": {"recommended": true}, "resource": {"id": "c", "_meta": {"path": ["c"]}}, "source": "aliceCo/shared/c"}]}',
   'materialization', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb');

-- Pub 4: Bob's materialization v1
INSERT INTO publication_specs (live_spec_id, pub_id, detail, published_at, spec, spec_type, user_id)
VALUES
  ('20:20:20:20:20:20:20:08', '10:10:10:10:10:10:10:04', 'Bob''s materialization v1', '2024-01-01T00:03:00Z',
   '{"endpoint": {"connector": {"image": "materialize/test:test", "config": {"publication": 1}}}, "bindings": [{"fields": {"recommended": true}, "resource": {"id": "a", "_meta": {"path": ["a"]}}, "source": "aliceCo/shared/a"}, {"fields": {"recommended": true}, "resource": {"id": "b", "_meta": {"path": ["b"]}}, "source": "aliceCo/shared/b"}, {"fields": {"recommended": true}, "resource": {"id": "c", "_meta": {"path": ["c"]}}, "source": "aliceCo/shared/c"}]}',
   'materialization', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb');

-- Pub 5: Bob's materialization v2
INSERT INTO publication_specs (live_spec_id, pub_id, detail, published_at, spec, spec_type, user_id)
VALUES
  ('20:20:20:20:20:20:20:08', '10:10:10:10:10:10:10:05', 'Bob''s materialization v2', '2024-01-01T00:04:00Z',
   '{"endpoint": {"connector": {"image": "materialize/test:test", "config": {"publication": 2}}}, "bindings": [{"fields": {"recommended": true}, "resource": {"id": "a", "_meta": {"path": ["a"]}}, "source": "aliceCo/shared/a"}, {"fields": {"recommended": true}, "resource": {"id": "b", "_meta": {"path": ["b"]}}, "source": "aliceCo/shared/b"}, {"fields": {"recommended": true}, "resource": {"id": "c", "_meta": {"path": ["c"]}}, "source": "aliceCo/shared/c"}]}',
   'materialization', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb');

-- Pub 6: Bob's materialization v3
INSERT INTO publication_specs (live_spec_id, pub_id, detail, published_at, spec, spec_type, user_id)
VALUES
  ('20:20:20:20:20:20:20:08', '10:10:10:10:10:10:10:06', 'Bob''s materialization v3', '2024-01-01T00:05:00Z',
   '{"endpoint": {"connector": {"image": "materialize/test:test", "config": {"publication": 3}}}, "bindings": [{"fields": {"recommended": true}, "resource": {"id": "a", "_meta": {"path": ["a"]}}, "source": "aliceCo/shared/a"}, {"fields": {"recommended": true}, "resource": {"id": "b", "_meta": {"path": ["b"]}}, "source": "aliceCo/shared/b"}, {"fields": {"recommended": true}, "resource": {"id": "c", "_meta": {"path": ["c"]}}, "source": "aliceCo/shared/c"}]}',
   'materialization', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb');

-- Pub 7: Bob's materialization v4 (final)
INSERT INTO publication_specs (live_spec_id, pub_id, detail, published_at, spec, spec_type, user_id)
VALUES
  ('20:20:20:20:20:20:20:08', '10:10:10:10:10:10:10:07', 'Bob''s materialization v4', '2024-01-01T00:06:00Z',
   '{"endpoint": {"connector": {"image": "materialize/test:test", "config": {"publication": 4}}}, "bindings": [{"fields": {"recommended": true}, "resource": {"id": "a", "_meta": {"path": ["a"]}}, "source": "aliceCo/shared/a"}, {"fields": {"recommended": true}, "resource": {"id": "b", "_meta": {"path": ["b"]}}, "source": "aliceCo/shared/b"}, {"fields": {"recommended": true}, "resource": {"id": "c", "_meta": {"path": ["c"]}}, "source": "aliceCo/shared/c"}]}',
   'materialization', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb');

-- ============================================================
-- Alert history
-- ============================================================
-- Three resolved alerts (loop i=0..3), alternating auto_discover_failed / shard_failed.
-- Each alert is inserted for both aliceCo/shared/capture and aliceCo/private/capture.
-- One unresolved alert for aliceCo/shared/capture only.
--
-- Timestamps:
--   i=0: fired 2024-08-09T10:11:12Z, resolved 2024-08-09T10:41:12Z (auto_discover_failed)
--   i=1: fired 2024-08-09T10:56:12Z, resolved 2024-08-09T11:26:12Z (shard_failed)
--   i=2: fired 2024-08-09T11:41:12Z, resolved 2024-08-09T12:11:12Z (auto_discover_failed)
--   unresolved: fired 2024-08-09T12:26:12Z (shard_failed, aliceCo/shared/capture only)

INSERT INTO alert_history (catalog_name, alert_type, arguments, fired_at, resolved_at)
VALUES
  -- i=0: auto_discover_failed, resolved
  ('aliceCo/shared/capture', 'auto_discover_failed',
   '{"state": "resolved", "spec_type": "capture", "first_ts": "2024-08-09T10:11:12Z", "last_ts": null, "error": "fake alert for tests", "count": 1, "resolved_at": "2024-08-09T10:41:12Z", "recipients": []}',
   '2024-08-09T10:11:12Z', '2024-08-09T10:41:12Z'),
  ('aliceCo/private/capture', 'auto_discover_failed',
   '{"state": "resolved", "spec_type": "capture", "first_ts": "2024-08-09T10:11:12Z", "last_ts": null, "error": "fake alert for tests", "count": 1, "resolved_at": "2024-08-09T10:41:12Z", "recipients": []}',
   '2024-08-09T10:11:12Z', '2024-08-09T10:41:12Z'),

  -- i=1: shard_failed, resolved
  ('aliceCo/shared/capture', 'shard_failed',
   '{"state": "resolved", "spec_type": "capture", "first_ts": "2024-08-09T10:56:12Z", "last_ts": null, "error": "fake alert for tests", "count": 1, "resolved_at": "2024-08-09T11:26:12Z", "recipients": []}',
   '2024-08-09T10:56:12Z', '2024-08-09T11:26:12Z'),
  ('aliceCo/private/capture', 'shard_failed',
   '{"state": "resolved", "spec_type": "capture", "first_ts": "2024-08-09T10:56:12Z", "last_ts": null, "error": "fake alert for tests", "count": 1, "resolved_at": "2024-08-09T11:26:12Z", "recipients": []}',
   '2024-08-09T10:56:12Z', '2024-08-09T11:26:12Z'),

  -- i=2: auto_discover_failed, resolved
  ('aliceCo/shared/capture', 'auto_discover_failed',
   '{"state": "resolved", "spec_type": "capture", "first_ts": "2024-08-09T11:41:12Z", "last_ts": null, "error": "fake alert for tests", "count": 1, "resolved_at": "2024-08-09T12:11:12Z", "recipients": []}',
   '2024-08-09T11:41:12Z', '2024-08-09T12:11:12Z'),
  ('aliceCo/private/capture', 'auto_discover_failed',
   '{"state": "resolved", "spec_type": "capture", "first_ts": "2024-08-09T11:41:12Z", "last_ts": null, "error": "fake alert for tests", "count": 1, "resolved_at": "2024-08-09T12:11:12Z", "recipients": []}',
   '2024-08-09T11:41:12Z', '2024-08-09T12:11:12Z');

-- Unresolved alert (aliceCo/shared/capture only)
INSERT INTO alert_history (catalog_name, alert_type, arguments, fired_at)
VALUES
  ('aliceCo/shared/capture', 'shard_failed',
   '{"state": "firing", "spec_type": "capture", "first_ts": "2024-08-09T12:26:12Z", "last_ts": null, "error": "fake alert for tests", "count": 1, "resolved_at": null, "recipients": []}',
   '2024-08-09T12:26:12Z');
