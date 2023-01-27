
create function tests.test_create_ops_publication()
returns setof text as $test$

	delete from live_specs;
	delete from drafts;
	delete from draft_specs;
	delete from publications;
	delete from ops_catalog_template;
	delete from tenants;

	-- Start with a single tenant and ops specs.
	insert into tenants (tenant, l1_stat_rollup) values	('ops/', 0);
	insert into live_specs (id, catalog_name, spec_type, spec, last_build_id, last_pub_id) values
    	('0202020202020202', 'ops/catalog-stats-L1/0', 'collection', '{}', '0101010101010101', '0101010101010101'),
    	('0303030303030303', 'ops/catalog-stats-L2/0', 'collection', '{}', '0202020202020202', '0202020202020202');

	-- Add dummy template fixtures.
	insert into ops_catalog_template (template_type, bundled_catalog) values
		(
			'tenant',
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
		),
		(
			'l1_derivation',
			'{
				"collections": {
					"ops/catalog-stats-L1/L1ID": {
						"derivation": {
							"typescript": { "module": {} },
							"transform": {
								"fromTENANTLogs": {
									"source": {
										"name": "ops/TENANT/logs"
									}
								},
								"fromTENANTStats": {
									"source": {
										"name": "ops/TENANT/stats"
									}
								}
							}
						}
					}
				}
			}'
		),
		(
			'l2_derivation',
			'{
				"collections": {
					"ops/catalog-stats-L2/0": {
						"derivation": {
							"typescript": { "module": {} },
							"transform": {
								"fromL1ID": {
									"source": {
										"name": "ops/catalog-stats-L1/L1ID"
									}
								}
							}
						}
					}
				}
			}'
		),
		(
			'materialization',
			'{
				"materializations": {
					"ops/stats-view": {
						"endpoint": {
							"connector": { "image": "test/materialize-image:v0", "config": {}}
						},
						"bindings": []
					}
				}
			}'
		);

	-- Creation of a new tenant using an existing l1_stat_rollup. This will create an update of the
	-- existing level 1 and level 2 derivations.
	insert into tenants (tenant, l1_stat_rollup) values	('aliceCo/', 0);
	-- Normally the user id would that of support@estuary.dev, but any id serves for the purpose of the test.
	select internal.create_ops_publication('aliceCo/', '11111111-1111-1111-1111-111111111111');

	select results_eq(
		$$ select draft_specs.catalog_name::text, draft_specs.spec_type::text, draft_specs.expect_pub_id::text from publications
			join draft_specs on publications.draft_id = draft_specs.draft_id
			where publications.user_id = '11111111-1111-1111-1111-111111111111'
			order by draft_specs.catalog_name asc $$,
		$$ values
			('ops/aliceCo/bar', 'collection', null),
			('ops/aliceCo/baz', 'materialization', null),
			('ops/aliceCo/foo', 'capture', null),
			('ops/catalog-stats-L1/0', 'collection', '01:01:01:01:01:01:01:01'),
			('ops/catalog-stats-L2/0', 'collection', '02:02:02:02:02:02:02:02'),
			('ops/stats-view', 'materialization', null) $$
	);

	-- Creation of a different tenant using a new l1_stat_rollup. This will create a new level 1
	-- derivation and an update to the existing level 2 derivation.
	insert into tenants (tenant, l1_stat_rollup) values	('bobCo/', 1);
	select internal.create_ops_publication('bobCo/', '22222222-2222-2222-2222-222222222222');

	select results_eq(
		$$ select draft_specs.catalog_name::text, draft_specs.spec_type::text, draft_specs.expect_pub_id::text from publications
			join draft_specs on publications.draft_id = draft_specs.draft_id
			where publications.user_id = '22222222-2222-2222-2222-222222222222'
			order by draft_specs.catalog_name asc $$,
		$$ values
			('ops/bobCo/bar', 'collection', null),
			('ops/bobCo/baz', 'materialization', null),
			('ops/bobCo/foo', 'capture', null),
			('ops/catalog-stats-L1/1', 'collection', null),
			('ops/catalog-stats-L2/0', 'collection', '02:02:02:02:02:02:02:02'),
			('ops/stats-view', 'materialization', null) $$
	);

$test$ language sql;



