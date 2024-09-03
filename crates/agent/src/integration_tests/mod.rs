//! # Control plane integration tests
//!
//! These tests cover end-to-end scenarios involving the control plane. The data plane and
//! connectors are not exercised as part of these.
mod dependencies_and_activations;
pub mod harness;
mod locking_retries;
mod null_bytes;
mod quotas;
mod schema_evolution;
mod source_captures;
mod unknown_connectors;
mod user_publications;

// TODO(johnny): Do we need this? It's used in only one integration test.
fn live_to_draft(live: tables::LiveCatalog) -> tables::DraftCatalog {
    let tables::LiveCatalog {
        captures,
        collections,
        materializations,
        tests,
        ..
    } = live;

    let captures = captures.into_iter().map(
        |tables::LiveCapture {
             capture,
             last_pub_id,
             model,
             ..
         }| tables::DraftCapture {
            scope: tables::synthetic_scope(models::CatalogType::Capture.to_string(), &capture),
            capture,
            expect_pub_id: Some(last_pub_id),
            model: Some(model),
        },
    );
    let collections = collections.into_iter().map(
        |tables::LiveCollection {
             collection,
             last_pub_id,
             model,
             ..
         }| tables::DraftCollection {
            scope: tables::synthetic_scope(
                models::CatalogType::Collection.to_string(),
                &collection,
            ),
            collection,
            expect_pub_id: Some(last_pub_id),
            model: Some(model),
        },
    );
    let materializations = materializations.into_iter().map(
        |tables::LiveMaterialization {
             materialization,
             last_pub_id,
             model,
             ..
         }| tables::DraftMaterialization {
            scope: tables::synthetic_scope(
                models::CatalogType::Materialization.to_string(),
                &materialization,
            ),
            materialization,
            expect_pub_id: Some(last_pub_id),
            model: Some(model),
        },
    );
    let tests = tests.into_iter().map(
        |tables::LiveTest {
             test,
             last_pub_id,
             model,
             ..
         }| tables::DraftTest {
            scope: tables::synthetic_scope(models::CatalogType::Test.to_string(), &test),
            test,
            expect_pub_id: Some(last_pub_id),
            model: Some(model),
        },
    );

    tables::DraftCatalog {
        captures: captures.collect(),
        collections: collections.collect(),
        materializations: materializations.collect(),
        tests: tests.collect(),
        ..Default::default()
    }
}
