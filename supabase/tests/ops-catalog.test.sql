
create function tests.test_create_ops_publication()
returns setof text as $test$

	delete from live_specs;
	delete from drafts;
	delete from draft_specs;
	delete from publications;
	delete from ops_catalog_template;

	-- Start out with some existing live_specs. One will be deleted, and the other updated.
	insert into live_specs (id, catalog_name, spec_type, spec, last_build_id, last_pub_id) values
    	('0202020202020202', 'ops/aliceCo/toDelete', 'collection', '{}', '0101010101010101', '0101010101010101'),
    	('0303030303030303', 'ops/aliceCo/bar', 'collection', '{}', '0101010101010101', '0101010101010101');

	-- Add a dummy catalog fixture
	insert into ops_catalog_template (id, bundled_catalog) values (
		'00:00:00:00:00:00:00:00',
		'{
			"captures": {
				"ops/TENANT/foo": {
					"endpoint": {
						"connector": { "image": "test/capture-image:v0", "config": {}}
					},
					"bindings": []
				}
			},
			"collections": {
				"ops/TENANT/bar": {
					"schema": { "type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"] },
					"key": ["/id"]
				}
			},
			"materializations": {
				"ops/TENANT/baz": {
					"endpoint": {
						"connector": { "image": "test/materialize-image:v0", "config": {}}
					},
					"bindings": []
				}
			}
		}'
	);
	
	-- Normally the user id would that of support@estuary.dev, but any id serves for the purpose of the test.
	select internal.create_ops_publication('aliceCo/', '11111111-1111-1111-1111-111111111111');
	
	select results_eq(
		$$ select draft_specs.catalog_name::text, draft_specs.spec_type::text from publications
			join draft_specs on publications.draft_id = draft_specs.draft_id
			where publications.user_id = '11111111-1111-1111-1111-111111111111'
			order by draft_specs.catalog_name asc $$,
		$$ values 
			('ops/aliceCo/bar', 'collection'),
			('ops/aliceCo/baz', 'materialization'),
			('ops/aliceCo/foo', 'capture'),
			('ops/aliceCo/toDelete', null) $$
	);
	

$test$ language sql;



