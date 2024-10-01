

create function tests.test_next_auto_discovers()
returns setof text as $$
declare
  result_row discovers%rowtype;
  draft_id flowid;
begin
  delete from user_grants;
  delete from role_grants;
  delete from discovers;
  delete from live_specs;

  -- insert into user_grants (user_id, object_role, capability) values
  --   ('11111111-1111-1111-1111-111111111111', 'estuary_support/', 'admin');
  insert into tenants (tenant) values ('aliceCo/');
  insert into connectors (id, image_name, title, short_description, logo_url, external_url, recommended) values
	('12:34:56:78:87:65:43:21', 'captureImage', '{"en-US":"a title"}', '{"en-US":"a desc"}', '{"en-US":"a logo"}', 'http://foo.test', true);
  insert into connector_tags (connector_id, image_tag) values
	('12:34:56:78:87:65:43:21', ':v0');
  insert into live_specs (catalog_name, spec_type, spec, connector_image_name, connector_image_tag, updated_at) values
	('aliceCo/test-capture', 'capture', '{
      "autoDiscover": {"addNewBindings": true},
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "config"}
          }
      },
      "bindings": []
    }', 'captureImage', ':v0', now() - '3h'::interval),

	-- This should show up in the initial next_auto_discovers output, but not after
	-- we bump the updated_at timestamp
	('aliceCo/test-capture-recently-updated', 'capture', '{
      "autoDiscover": {"evolveIncompatibleCollections": true},
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"other": "config"}
          }
      },
      "bindings": []
    }', 'captureImage', ':v0', now() - '3h'::interval),
	-- This should show up in the initial next_auto_discovers output, but not after
	-- we create a discover
	('aliceCo/test-capture-recently-discovered', 'capture', '{
      "autoDiscover": {"evolveIncompatibleCollections": true},
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"other": "config"}
          }
      },
      "bindings": []
    }', 'captureImage', ':v0', now() - '3h'::interval),
	-- These should not show up in the output at all
	('aliceCo/test-capture-shards-disabled', 'capture', '{
	  "shards": {
        "disable": true
      },
      "autoDiscover": {"addNewBindings": true},
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"other": "config"}
          }
      },
      "bindings": []
    }', 'captureImage', ':v0', now() - '3h'::interval),
	('aliceCo/test-capture-discover-disabled', 'capture', '{
      "autoDiscover": null,
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "config"}
          }
      },
      "bindings": []
    }', 'captureImage', ':v0', now() - '3h'::interval),
	('aliceCo/test-capture-discover-disabled-too', 'capture', '{
      "endpoint": {
        "connector": {
          "image": "does not matter",
            "config": {"some": "config"}
          }
      },
      "bindings": []
    }', 'captureImage', ':v0', now() - '3h'::interval);

  -- assert that the recently-updated capture shows up in the view before we
  -- insert the recent discover.
  return query select results_eq(
    $i$ select capture_name::text from internal.next_auto_discovers order by capture_name asc $i$,
	$i$ values ('aliceCo/test-capture'),('aliceCo/test-capture-recently-discovered'),('aliceCo/test-capture-recently-updated') $i$
  );

  -- bump the live_specs updated_at timestamp and create a discover for these two captures, which should
  -- cause them to be excluded from the output.
  update live_specs set updated_at = now() where catalog_name = 'aliceCo/test-capture-recently-updated';

  insert into drafts (user_id) select id from auth.users where email = 'support@estuary.dev' returning id into draft_id;
  insert into discovers (capture_name, connector_tag_id, draft_id, endpoint_config) values (
    'aliceCo/test-capture-recently-discovered',
	(select id from connector_tags where image_tag = ':v0' limit 1),
	draft_id,
	'{"other": "config"}'
  );

  -- Assert that we've created exactly one discover
  return query select ok(internal.create_auto_discovers() = 1, 'discovers are created periodically');

  select * into result_row from discovers where auto_publish = true and capture_name = 'aliceCo/test-capture';
  return query select ok(result_row.endpoint_config::text = '{"some": "config"}', 'discover created with expected config');
  return query select ok(result_row.update_only = false, 'discover created with expected update_only');
  return query select ok(result_row.auto_evolve = false, 'discover created with expected auto_evolve');
  return query select ok(result_row.auto_publish = true, 'discover created with expected auto_publish');

end;
$$ language plpgsql;