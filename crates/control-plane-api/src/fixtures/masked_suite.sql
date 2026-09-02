-- Grant graph purpose-built for the masked-token no-amplification suite
-- (`server/masked_token_suite/`). Composes with the `data_planes` fixture;
-- deliberately independent of `alice`, whose shape many unrelated snapshot
-- tests pin.
--
-- Each edge exists to discriminate one walk behavior under a mask:
--
--   dana ──admin──▶ danaCo/ ──read──▶ sharedCo/data/
--     A direct legacy-admin grant (whose bits include Delegate, not Assume),
--     and a role edge reachable only while the mask leaves Delegate enabled.
--
--   dana ──{viewer,assume}──▶ assumeCo/ ──admin──▶ wideCo/
--     An Assume-bearing bundles grant: unmasked, Assume passes the edge's
--     full admin bits through to wideCo/; a mask must contain that widening
--     while still permitting traversal.
--
--   erin ──read──▶ sharedCo/
--     The lifecycle subject: her grant is upgraded and then deleted mid-test
--     to observe the same minted token across snapshot refreshes.
--
--   otherCo/ has no path from either user: the sweep's never-authorized
--   column.
do $$
declare
  data_plane_one_id flowid := '111111111111';

  dana_uid uuid := '44444444-4444-4444-4444-444444444444';
  erin_uid uuid := '55555555-5555-5555-5555-555555555555';

  dana_data_id flowid  := '000000000011';
  shared_data_id flowid := '000000000012';
  wide_data_id flowid  := '000000000013';
  other_data_id flowid := '000000000014';
  last_pub_id flowid := '000000000002';

begin

  insert into auth.users (id, email) values
    (dana_uid, 'dana@example.test'),
    (erin_uid, 'erin@example.test')
  ;
  insert into public.user_grants (user_id, object_role, capability, bundles) values
    (dana_uid, 'danaCo/', 'admin', '{}'),
    (dana_uid, 'assumeCo/', 'none', array['viewer', 'assume']::capability_bundle[]),
    (erin_uid, 'sharedCo/', 'read', '{}')
  ;
  insert into public.role_grants (subject_role, object_role, capability) values
    ('danaCo/', 'sharedCo/data/', 'read'),
    ('assumeCo/', 'wideCo/', 'admin')
  ;

  perform internal.create_task(dana_data_id, 1::smallint, '000000000000'::flowid);
  perform internal.create_task(shared_data_id, 1::smallint, '000000000000'::flowid);
  perform internal.create_task(wide_data_id, 1::smallint, '000000000000'::flowid);
  perform internal.create_task(other_data_id, 1::smallint, '000000000000'::flowid);

  insert into public.live_specs (id, controller_task_id, catalog_name, last_pub_id, spec_type, spec, built_spec, data_plane_id) values
    (dana_data_id, dana_data_id, 'danaCo/data/collection', last_pub_id, 'collection', '{}', '{"partitionTemplate":{"name":"danaCo/data/collection/gen1234"}}', data_plane_one_id),
    (shared_data_id, shared_data_id, 'sharedCo/data/collection', last_pub_id, 'collection', '{}', '{"partitionTemplate":{"name":"sharedCo/data/collection/gen1234"}}', data_plane_one_id),
    (wide_data_id, wide_data_id, 'wideCo/data/collection', last_pub_id, 'collection', '{}', '{"partitionTemplate":{"name":"wideCo/data/collection/gen1234"}}', data_plane_one_id),
    (other_data_id, other_data_id, 'otherCo/data/collection', last_pub_id, 'collection', '{}', '{"partitionTemplate":{"name":"otherCo/data/collection/gen1234"}}', data_plane_one_id)
  ;

end
$$;
