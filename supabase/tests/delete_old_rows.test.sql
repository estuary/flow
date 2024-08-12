

create function tests.test_delete_old_rows()
returns setof text as $$
declare
  test_user_id uuid = '5ec31342-bfda-43d7-ac62-107e7e2f7f96';
  test_connector_tag_id flowid = '12:34:56:78:87:65:43:21';
  old_draft_id flowid;
  new_draft_id flowid;
begin

  insert into auth.users(id, email) values (test_user_id, 'test@test.test');
  insert into connectors (id, image_name, title, short_description, logo_url, external_url) values
    ('33:33:33:33:33:33:33:97', 'captureImage', '{"en-US":"a title"}', '{"en-US":"a desc"}',
      '{"en-US":"a logo"}', 'http://foo.test');
  insert into connector_tags (id, connector_id, image_tag) values
    (test_connector_tag_id, '33:33:33:33:33:33:33:97', ':v0');

  insert into drafts (user_id, updated_at) values (test_user_id, now() - '11 days'::interval) returning id into old_draft_id;
  insert into drafts (user_id) values (test_user_id) returning id into new_draft_id;

  insert into draft_specs (draft_id, catalog_name, spec, spec_type) values
    (old_draft_id, 'a/b/c', '{}', 'capture'),
    (new_draft_id, 'a/b/d', '{}', 'capture'),
    (new_draft_id, 'a/b/e', '{}', 'materialization');

  insert into evolutions (draft_id, user_id, detail, job_status, collections) values
    (old_draft_id, test_user_id, 'should delete', '{"type":"evolutionFailed"}', '[]'),
    (new_draft_id, test_user_id, 'should retain', '{"type":"evolutionFailed"}', '[]');

  insert into discovers (draft_id, connector_tag_id, detail, capture_name, job_status, endpoint_config) values
    (old_draft_id, test_connector_tag_id, 'should delete', 'a/b/c', '{"type": "discoverFailed"}', '{}'),
    (new_draft_id, test_connector_tag_id, 'should retain', 'a/b/d', '{"type": "discoverFailed"}', '{}');


  insert into internal.log_lines (log_line, stream, logged_at, token) values
    ('should delete line', 'foo', now() - '90 days'::interval, gen_random_uuid()),
    ('should delete line too', 'foo', now() - '3 days'::interval, gen_random_uuid()),
    ('should keep line', 'foo', now() - '1 days'::interval, gen_random_uuid()),
    ('should keep line too', 'foo', now() - '5 minutes'::interval, gen_random_uuid());

  insert into catalog_stats_hourly (ts, grain, catalog_name, flow_document) values
    (now() - '90 days'::interval, 'hourly', 'a/b/c', '{"should":"delete1"}'::json),
    (now() - '31 days'::interval, 'hourly', 'a/b/c', '{"should":"delete2"}'::json),
    (now() - '29 days'::interval, 'hourly', 'a/b/c', '{"should":"keep1"}'::json),
    (now() - '5 minutes'::interval, 'hourly', 'a/b/c', '{"should":"keep2"}'::json);

  --return query select ok(internal.delete_old_drafts() = 1, 'one draft should have been deleted');
  return query select results_eq(
    $i$ select internal.delete_old_drafts() $i$,
    $i$ values (1) $i$
  );

  return query select results_eq(
    $i$ select ds.catalog_name::text
          from drafts d
          join draft_specs ds on d.id = ds.draft_id
          where d.user_id = '5ec31342-bfda-43d7-ac62-107e7e2f7f96'
          order by catalog_name asc
    $i$,
    $i$ values ('a/b/d'), ('a/b/e') $i$
  );

  return query select results_eq(
    $i$ select di.detail
          from drafts d
          join discovers di on d.id = di.draft_id
          where d.user_id = '5ec31342-bfda-43d7-ac62-107e7e2f7f96'
    $i$,
    $i$ values ('should retain') $i$
  );

  return query select results_eq(
    $i$ select e.detail
          from drafts d
          join evolutions e on d.id = e.draft_id
          where d.user_id = '5ec31342-bfda-43d7-ac62-107e7e2f7f96'
    $i$,
    $i$ values ('should retain') $i$
  );

  return query select results_eq(
    $i$ select internal.delete_old_hourly_stats() $i$,
    $i$ values (2) $i$
  );

  return query select results_eq(
    $i$ select flow_document->>'should'
          from catalog_stats_hourly
          where flow_document->>'should' is not null
          order by flow_document->>'should'
    $i$,
    $i$ values ('keep1'), ('keep2') $i$
  );

  return query select results_eq(
    $i$ select internal.delete_old_log_lines() $i$,
    $i$ values (2) $i$
  );
  return query select results_eq(
    $i$ select log_line from internal.log_lines where log_line like 'should %' order by log_line $i$,
    $i$ values ('should keep line'), ('should keep line too') $i$
  );
end;
$$ language plpgsql;
