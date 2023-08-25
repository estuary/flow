

create function tests.test_draft_deletion()
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

  insert into drafts (user_id, updated_at) values (test_user_id, now() - '31 days'::interval) returning id into old_draft_id; 
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

  return query select ok(internal.delete_old_drafts() = 1, 'one draft should have been deleted');

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
end;
$$ language plpgsql;

