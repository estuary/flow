create function tests.test_discovers_null_connector_tag()
returns setof text as $$
declare
  test_draft_id flowid;
  test_tag_id flowid;
begin
  insert into connectors (id, image_name, title, short_description, logo_url, external_url, recommended) values
    ('12:34:56:78:87:65:43:21', 'captureImage', '{"en-US":"a title"}', '{"en-US":"a desc"}', '{"en-US":"a logo"}', 'http://foo.test', true);
  insert into connector_tags (connector_id, image_tag) values
    ('12:34:56:78:87:65:43:21', ':v0')
    returning id into test_tag_id;
  insert into drafts (user_id) values
    ('11111111-1111-1111-1111-111111111111')
    returning id into test_draft_id;

  return query select lives_ok(
    format($q$ insert into discovers (capture_name, draft_id, connector_tag_id, endpoint_config)
        values ('aliceCo/capture', %L, null, '{}') $q$, test_draft_id),
    'the control-plane API may queue a NULL connector tag');

  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  return query select throws_ok(
    format($q$ insert into discovers (capture_name, draft_id, connector_tag_id, endpoint_config)
        values ('aliceCo/capture', %L, null, '{}') $q$, test_draft_id),
    '23514',
    null,
    'PostgREST may not queue a NULL connector tag');

  return query select lives_ok(
    format($q$ insert into discovers (capture_name, draft_id, connector_tag_id, endpoint_config)
        values ('aliceCo/capture', %L, %L, '{}') $q$, test_draft_id, test_tag_id),
    'PostgREST may provide a connector tag');
end;
$$ language plpgsql;
