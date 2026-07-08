create function tests.test_alert_configs_basics()
returns setof text as $$
declare
  prefix_id flowid;
  name_id flowid;
begin
  delete from alert_configs;

  insert into alert_configs (catalog_prefix_or_name, config)
    values ('aliceCo/prod/', '{"taskIdle": {"threshold": "60d"}}'::jsonb)
    returning id into prefix_id;

  insert into alert_configs (catalog_prefix_or_name, config)
    values ('aliceCo/prod/source-postgres', '{"taskIdle": {"threshold": "90d"}}'::jsonb)
    returning id into name_id;

  return query select ok(prefix_id is not null and name_id is not null,
    'insert accepts both prefix and exact-name forms');

  return query select ok(prefix_id <> name_id, 'distinct ids for prefix and name rows');

  -- Uniqueness on the (text) catalog_prefix_or_name.
  return query select throws_ok(
    $q$ insert into alert_configs (catalog_prefix_or_name, config)
        values ('aliceCo/prod/', '{}'::jsonb) $q$,
    '23505',
    null,
    'uniqueness on catalog_prefix_or_name');

  -- This test covers the DB-level NFKC normalization check. It uses an FF
  -- LIGATURE (U+FB00), which NFKC maps to "ff".
  return query select throws_ok(
    $q$ insert into alert_configs (catalog_prefix_or_name, config)
        values ('aliceCo/u+FB00 ﬀ-ligature/', '{}'::jsonb) $q$,
    '23514',
    null,
    'rejects non-NFKC-normalized catalog_prefix_or_name');

  return query select lives_ok(
    $q$ insert into alert_configs (catalog_prefix_or_name, config)
        values ('bob.co/', '{}'::jsonb) $q$,
    'accepts minimal one-level prefix');

  return query select lives_ok(
    $q$ update alert_configs
        set config = '{"taskIdle": {"threshold": "30d"}}'::jsonb,
            updated_at = now()
        where catalog_prefix_or_name = 'aliceCo/prod/' $q$,
    'update by catalog_prefix_or_name works');

  delete from alert_configs;
end;
$$ language plpgsql;

create function tests.test_alert_configs_no_grant_denies_authenticated()
returns setof text as $$
begin
  delete from alert_configs;

  insert into alert_configs (catalog_prefix_or_name, config)
    values ('aliceCo/', '{}'::jsonb);

  delete from user_grants;
  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin');

  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  -- No GRANT exists for `authenticated` on alert_configs, so both SELECT and
  -- INSERT raise permission-denied errors.
  return query select throws_ok(
    $q$ select id from alert_configs where catalog_prefix_or_name = 'aliceCo/' $q$,
    '42501',
    null,
    'authenticated role cannot select alert_configs (no GRANT)');

  return query select throws_ok(
    $q$ insert into alert_configs (catalog_prefix_or_name, config)
        values ('aliceCo/x/', '{}'::jsonb) $q$,
    '42501',
    null,
    'authenticated role cannot insert into alert_configs (no GRANT)');

  set role postgres;
  delete from alert_configs;
end;
$$ language plpgsql;
