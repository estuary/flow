use crate::{evolution::EvolveRequest, ControlPlane};
use serde_json::Value;
use tables::DraftRow;

use crate::integration_tests::harness::{draft_catalog, TestHarness};

#[tokio::test]
#[serial_test::serial]
async fn test_collection_evolution() {
    let mut harness = TestHarness::init("test_auto_discovers").await;

    let user_id = harness.setup_tenant("moths").await;

    let mut draft = test_catalog();
    // Set is_touch: true on all the draft specs, so we can assert that evolutions will
    // set it to false for any specs that it modifies.
    for c in draft.captures.iter_mut() {
        c.is_touch = true;
    }
    for c in draft.collections.iter_mut() {
        c.is_touch = true;
    }
    for c in draft.materializations.iter_mut() {
        c.is_touch = true;
    }

    // First test an evolution that affects only drafted specs, before any live specs exist
    let req = vec![
        EvolveRequest::of("moths/collectionA"),
        EvolveRequest::of("moths/collectionB").with_new_name("moths/new-collectionB"),
        EvolveRequest::of("moths/collectionC"),
        EvolveRequest::of("moths/collectionD").with_new_name("moths/new-collectionD"),
    ];
    let output = harness
        .control_plane()
        .evolve_collections(draft, req.clone())
        .await
        .expect("evolution failed");
    insta::assert_debug_snapshot!("first-evolution-draft", output);

    // Now publish the pre-evolution version of the catalog, and do an evolution
    // of that into a blank draft. We'll expect to see the same changes as
    // before, but now they'll be applied to the live specs versions instead of
    // already drafted specs.
    let pub_result = harness
        .user_publication(user_id, "initial-evolution-pub", test_catalog())
        .await;
    assert!(pub_result.status.is_success());

    let second_evo_output = harness
        .control_plane()
        .evolve_collections(tables::DraftCatalog::default(), req.clone())
        .await
        .unwrap();

    assert_eq!(2, second_evo_output.draft.captures.len());
    assert_eq!(2, second_evo_output.draft.materializations.len());
    assert_eq!(output.actions, second_evo_output.actions);

    for (first, second) in output
        .draft
        .captures
        .iter()
        .zip(second_evo_output.draft.captures.iter())
    {
        assert_same(first, second);
    }
    for (first, second) in output
        .draft
        .materializations
        .iter()
        .zip(second_evo_output.draft.materializations.iter())
    {
        assert_same(first, second);
    }
    for (first, second) in output
        .draft
        .collections
        .iter()
        .zip(second_evo_output.draft.collections.iter())
    {
        assert_same(first, second);
    }

    // Now start with a partial draft, and evolve into that and ensure that we preserve
    // any drafted changes.
    let mut partial_draft = test_catalog();
    partial_draft
        .captures
        .retain(|c| c.catalog_name().as_str() == "moths/captureB");
    partial_draft
        .materializations
        .retain(|c| c.catalog_name().as_str() == "moths/materializationB");
    partial_draft
        .collections
        .retain(|c| c.catalog_name().as_str() == "moths/collectionA");
    // This collection has the same name as the one we're evolving into, so it
    // should be used instead of the collectionB live spec.
    let draft_new_collection_b: models::CollectionDef = serde_json::from_value(serde_json::json!({
        "schema": {
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "addedDraftProperty": { "type": "string" }
            },
            "required": ["id"]
        },
        "key": [ "/id" ]
    }))
    .unwrap();
    partial_draft.collections.insert(tables::DraftCollection {
        scope: tables::synthetic_scope("collection", "moths/new-collectionB"),
        collection: models::Collection::new("moths/new-collectionB"),
        expect_pub_id: Some(models::Id::from_hex("001122334455667788").unwrap()),
        model: Some(draft_new_collection_b.clone()),
        is_touch: false,
    });

    let req = vec![
        EvolveRequest::of("moths/collectionA"),
        EvolveRequest::of("moths/collectionB")
            .with_new_name("moths/new-collectionB")
            .with_materializations(["moths/materializationA"]),
        EvolveRequest::of("moths/collectionC").with_materializations(["moths/materializationA"]),
        EvolveRequest::of("moths/collectionD").with_new_name("moths/new-collectionD"),
    ];
    let mut third_output = harness
        .control_plane()
        .evolve_collections(partial_draft, req)
        .await
        .unwrap();

    redact_expect_pub_id(&mut third_output.draft);
    insta::assert_debug_snapshot!("third-output", third_output);

    // Error cases
    // Ensure we return reasonable errors when the `current_name` mentions a
    // collection that doesn't exist or isn't authorized.
    let current_collection_missing = vec![
        EvolveRequest::of("moths/missing-collection-backfill"),
        EvolveRequest::of("moths/missing-collection-rename").with_version_increment(),
    ];
    let output = harness
        .control_plane()
        .evolve_collections(tables::DraftCatalog::default(), current_collection_missing)
        .await
        .unwrap();
    assert!(!output.is_success());
    insta::assert_debug_snapshot!(output.draft.errors, @r###"
    [
        Error {
            scope: flow://collection/moths/missing-collection-backfill,
            error: nothing to update for collection 'moths/missing-collection-backfill',
        },
        Error {
            scope: flow://collection/moths/missing-collection-rename,
            error: missing spec for collection 'moths/missing-collection-rename',
        },
    ]
    "###);
}

fn redact_expect_pub_id(draft: &mut tables::DraftCatalog) {
    for row in draft.captures.iter_mut() {
        row.expect_pub_id = None;
    }
    for row in draft.collections.iter_mut() {
        row.expect_pub_id = None;
    }
    for row in draft.materializations.iter_mut() {
        row.expect_pub_id = None;
    }
}

fn assert_same<R>(l: &R, r: &R)
where
    R: tables::DraftRow,
    R::Key: std::fmt::Debug,
{
    assert_eq!(
        l.catalog_name(),
        r.catalog_name(),
        "name mismatch\nleft: {l:?}\nright: {r:?}"
    );
    assert_eq!(
        l.model(),
        r.model(),
        "model mismatch\nleft: {l:?}\nright: {r:?}"
    );
    assert_eq!(
        l.is_touch(),
        r.is_touch(),
        "is_touch mismatch\nleft: {l:?}\nright: {r:?}"
    );
}

fn test_catalog() -> tables::DraftCatalog {
    draft_catalog(serde_json::json!({
        "captures": {
            "moths/captureA": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": { "foo": "bar" }
                    }
                },
                "bindings": [
                    { "target": "moths/collectionA", "resource": { "id": "A" }},
                    { "target": "moths/collectionB", "resource": { "id": "B" }},
                ]
            },
            "moths/captureB": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": { "foo": "bar" }
                    }
                },
                "bindings": [
                    { "target": "moths/collectionC", "resource": { "id": "C" }},
                    { "target": "moths/collectionD", "resource": { "id": "D" }},
                ]
            }
        },
        "collections": {
            "moths/collectionA": test_collection_spec(),
            "moths/collectionB": test_collection_spec(),
            "moths/collectionC": test_collection_spec(),
            "moths/collectionD": test_collection_spec(),
        },
        "materializations": {
            "moths/materializationA": {
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": { "foo": "bar" }
                    }
                },
                "bindings": [
                    {"source": "moths/collectionA", "resource": { "id": "A" }},
                    {"source": "moths/collectionB", "resource": { "id": "B" }},
                    {"source": "moths/collectionC", "resource": { "id": "C" }},
                ]
            },
            "moths/materializationB": {
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": { "foo": "bar" }
                    }
                },
                "bindings": [
                    {"source": "moths/collectionB", "resource": { "id": "B" }},
                    {"source": "moths/collectionC", "backfill": 9, "resource": { "id": "C" }},
                    {"source": "moths/collectionD", "resource": { "id": "D" }}
                ]
            }
        }

    }))
}

fn test_collection_spec() -> Value {
    serde_json::json!({
        "schema": {
            "type": "object",
            "properties": {
                "id": { "type": "string" },
            },
            "required": ["id"]
        },
        "key": [ "/id" ]
    })
}
