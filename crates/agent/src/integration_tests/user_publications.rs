mod harness;

use agent::controllers::ControllerHandler;
use agent::{ControlPlane, PGControlPlane};
use harness::TestHarness;
use serde_json::value::RawValue;

#[tokio::test]
#[serial_test::serial]
async fn publications_and_controllers_happy_path() {
    let mut harness = TestHarness::init("test_publications").await;

    eprintln!("built a harness");

    let user_id = harness.setup_tenant("cats").await;
    eprintln!("created user: {user_id}");
    let draft_models: models::Catalog = serde_json::from_value(serde_json::json!({
        "collections": {
            "cats/noms": {
                "writeSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "readSchema": {
                    "allOf": [
                        { "$ref": "flow://write-schema" },
                        { "$ref": "flow://inferred-schema" }
                    ]
                },
                "key": ["/id"]
            }
        },
        "captures": {
            "cats/capture": {
                "endpoint": {
                    "connector": {
                        "image": "ghcr.io/estuary/source-hello-world:dev",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "name": "greetings",
                            "prefix": "Hello {}!"
                        },
                        "target": "cats/noms"
                    }
                ]
            }
        }
    }))
    .unwrap();
    let draft = tables::DraftCatalog::from(draft_models);
    let first_pub_result = harness
        .user_publication(user_id, format!("initial publication"), draft)
        .await;

    assert!(first_pub_result.job_status.is_success());

    let pub_id: agent_sql::Id = first_pub_result.publication_id.into();
    let results = sqlx::query!(
        r#"
        select ls.catalog_name as "catalog_name: String",
        ls.controller_next_run is not null and ls.controller_next_run <= now() as "controller_next_run_queued: bool",
        cj.controller_version,
        cj.status as "controller_status: agent_sql::TextJson<Box<RawValue>>",
        ls.built_spec is not null as "built_spec_set: bool",
        ls.last_build_id = ls.last_pub_id as "last_build_id_matches: bool",
        ls.spec_type as "spec_type: String",
        ls.spec as "spec: agent_sql::TextJson<Box<RawValue>>",
        ls.reads_from as "reads_from: Vec<String>",
        ls.writes_to as "writes_to: Vec<String>",
        array(select lst.catalog_name
            from live_spec_flows lsf
            join live_specs lst on lsf.target_id = lst.id
            where lsf.source_id = ls.id
            order by lst.catalog_name) as "flows_writes_to: Vec<String>",
        array(select lss.catalog_name
            from live_spec_flows lsf
            join live_specs lss on lsf.source_id = lss.id
            where lsf.target_id = ls.id
            order by lss.catalog_name) as "flows_reads_from: Vec<String>",
        ls.spec::text is not distinct from ps.spec::text as "pub_spec_matches: bool",
        ls.spec_type is not distinct from ps.spec_type as "pub_spec_type_matches: bool"

        from live_specs ls
        join publication_specs ps on ls.id = ps.live_spec_id and ps.pub_id = ls.last_pub_id
        join controller_jobs cj on ls.id = cj.live_spec_id
        where ls.last_pub_id = $1
        order by ls.catalog_name;
        "#,
        pub_id as agent_sql::Id,
    )
    .fetch_all(&harness.pool)
    .await
    .expect("failed to execute verify query");

    insta::assert_debug_snapshot!("initial-publication-result", results);

    // Both controllers should have run
    let controller_states = harness.run_next_controllers().await;
    let names = controller_states
        .iter()
        .map(|s| s.catalog_name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(vec!["cats/capture", "cats/noms"], names);
    assert!(controller_states[0].next_run.is_none());
    assert!(controller_states[1].next_run.is_some()); // collection must have next run to check inferred schema
                                                      // There should be an inferred schema status present, which indicates that the schema has not been updated yet
    let inferred_schema_status = controller_states[1]
        .current_status
        .unwrap_collection()
        .inferred_schema
        .as_ref()
        .unwrap();
    assert!(inferred_schema_status.schema_last_updated.is_none());
    assert!(inferred_schema_status.schema_md5.is_none());

    // Update the inferred schema in the database
    let inferred_schema = harness::mock_inferred_schema("cats/noms", 1);
    let expect_md5 = inferred_schema.md5.clone();
    harness.upsert_inferred_schema(inferred_schema).await;

    // Run controllers and expect that the inferred schema has been published
    let collection_state = harness.run_next_controllers().await;
    assert_eq!(1, collection_state.len());
    assert_eq!("cats/noms", collection_state[0].catalog_name.as_str());
    let collection_status = collection_state[0].current_status.unwrap_collection();
    let inferred_schema_status = collection_status
        .inferred_schema
        .as_ref()
        .expect("missing inferred schema status");
    assert!(inferred_schema_status.schema_last_updated.is_some());
    assert_eq!(inferred_schema_status.schema_md5, Some(expect_md5));
    assert_eq!(1, collection_status.publications.history.len());
    let collection_pub = &collection_status.publications.history[0];
    let collection_pub_id = collection_pub.id;
    assert!(
        collection_pub_id > first_pub_result.publication_id,
        "publication ids must increase monotonicly"
    );
    assert!(collection_pub.result.as_ref().unwrap().is_success());

    // The collection controller should run again next, since it was just published. It should not
    // have had anything to do this time, though.
    let collection_state = harness.run_next_controllers().await;
    assert_eq!(1, collection_state.len());
    assert_eq!("cats/noms", collection_state[0].catalog_name.as_str());
    // Should still just have 1 publication
    assert_eq!(
        1,
        collection_state[0]
            .current_status
            .unwrap_collection()
            .publications
            .history
            .len()
    );

    // Next the capture controller should run, and publish in response to the collection publication
    let capture_state = harness.run_next_controllers().await;
    assert_eq!(1, capture_state.len());
    assert_eq!("cats/capture", capture_state[0].catalog_name.as_str());
    let capture_pub = &capture_state[0]
        .current_status
        .unwrap_capture()
        .publications
        .history[0];
    assert!(capture_pub.is_success());
    assert_eq!(collection_pub_id, capture_pub.id);

    todo!("delete stuff that was created by this test");
}
