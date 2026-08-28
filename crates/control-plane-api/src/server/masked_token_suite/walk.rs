//! The subset property, the traversal semantics under a mask, and the
//! legacy-metadata probe — all with tokens produced by the real mint.

use super::{DANA, authorize_collection, mint};
use crate::test_server;

/// The collections of the `masked_suite` fixture, one per grant-graph path
/// class: direct grant, Delegate role-edge hop, Assume role-edge hop, and
/// no path at all.
const COLLECTIONS: [&str; 4] = [
    "danaCo/data/collection",
    "sharedCo/data/collection",
    "wideCo/data/collection",
    "otherCo/data/collection",
];

/// The headline no-amplification property, as an implication sweep: for
/// every mask in a family crossing the fixture's path classes, a minted
/// token's authorized set is a subset of its user's unmasked authorized
/// set, and a mask naming every bundle behaves exactly as no mask at all.
///
/// The grid itself is snapshotted so a legitimate behavior change reads as
/// a reviewable diff. Beyond the subset relation it exhibits the traversal
/// semantics: a mask without `Delegate` walk-denies the role-edge hop with
/// the requirement pre-check satisfied, a mask without `Assume` walk-denies
/// the Assume hop, and a mask below the Viewer threshold is the structured
/// pre-check refusal that discloses nothing about grants — the `empty` row
/// is the identity-only token, which authenticates everywhere and
/// authorizes nothing (that its identity still *operates* is pinned by the
/// revocation step in `guards_and_compat`). Denial messages carry the
/// caller's email — identity claims flowed from the mint through
/// enforcement.
#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../fixtures", scripts("data_planes", "masked_suite"))
)]
async fn test_no_amplification_sweep(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let server = test_server::TestServer::start(
        pool.clone(),
        test_server::snapshot(pool.clone(), false).await,
    )
    .await;
    let dana = server.make_access_token(DANA, Some("dana@example.test"));

    let all_bundles: Vec<&str> = models::authz::CapabilityBundle::ALL
        .iter()
        .map(|b| b.name())
        .collect();

    let masks: Vec<(&str, Option<Vec<&str>>)> = vec![
        ("unmasked", None),
        ("empty", Some(vec![])),
        ("unknown-only", Some(vec!["NotARealBundle"])),
        ("CatalogRead", Some(vec!["CatalogRead"])),
        ("Delegate", Some(vec!["Delegate"])),
        ("Viewer", Some(vec!["Viewer"])),
        ("Viewer+unknown", Some(vec!["Viewer", "FutureCapability"])),
        ("Viewer+Delegate", Some(vec!["Viewer", "Delegate"])),
        ("Viewer+Assume", Some(vec!["Viewer", "Assume"])),
        ("Admin", Some(vec!["Admin"])),
        ("all-bundles", Some(all_bundles)),
    ];

    let mut grid = String::new();
    let mut outcomes: Vec<(&str, Vec<String>)> = Vec::new();

    for (label, mask) in &masks {
        let token = match mask {
            None => dana.clone(),
            Some(mask) => mint(&server, &dana, mask).await,
        };

        let mut row = Vec::new();
        grid.push_str(&format!("{label}:\n"));
        for collection in COLLECTIONS {
            let outcome = authorize_collection(&server, &token, collection, "read").await;
            grid.push_str(&format!("  {collection} => {outcome}\n"));
            row.push(outcome);
        }
        outcomes.push((label, row));
    }

    // Masked-allowed implies unmasked-allowed: no mask amplifies.
    let allowed = |row: &[String]| -> Vec<usize> {
        (0..row.len())
            .filter(|i| row[*i].starts_with("OK"))
            .collect()
    };
    let unmasked = &outcomes[0].1;
    for (label, row) in &outcomes[1..] {
        for i in allowed(row) {
            assert!(
                unmasked[i].starts_with("OK"),
                "mask {label} authorized {} which the unmasked walk denies",
                COLLECTIONS[i],
            );
        }
    }

    // A mask naming every bundle is byte-identical to no mask: the masked-in
    // positive counterpart, generalized.
    let (label, row) = outcomes.last().unwrap();
    assert_eq!(*label, "all-bundles");
    assert_eq!(row, unmasked);

    insta::assert_snapshot!(grid, @r#"
    unmasked:
      danaCo/data/collection => OK danaCo/data/collection/gen1234/
      sharedCo/data/collection => OK sharedCo/data/collection/gen1234/
      wideCo/data/collection => OK wideCo/data/collection/gen1234/
      otherCo/data/collection => 403 dana@example.test is not authorized to otherCo/data/collection for Read
    empty:
      danaCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
      sharedCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
      wideCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
      otherCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
    unknown-only:
      danaCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
      sharedCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
      wideCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
      otherCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
    CatalogRead:
      danaCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["JournalRead","ViewDataPlanePrivateNetworking"]}
      sharedCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["JournalRead","ViewDataPlanePrivateNetworking"]}
      wideCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["JournalRead","ViewDataPlanePrivateNetworking"]}
      otherCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["JournalRead","ViewDataPlanePrivateNetworking"]}
    Delegate:
      danaCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
      sharedCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
      wideCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
      otherCo/data/collection => 403 {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, JournalRead, ViewDataPlanePrivateNetworking","missing_capabilities":["CatalogRead","JournalRead","ViewDataPlanePrivateNetworking"]}
    Viewer:
      danaCo/data/collection => OK danaCo/data/collection/gen1234/
      sharedCo/data/collection => 403 dana@example.test is not authorized to sharedCo/data/collection for Read
      wideCo/data/collection => 403 dana@example.test is not authorized to wideCo/data/collection for Read
      otherCo/data/collection => 403 dana@example.test is not authorized to otherCo/data/collection for Read
    Viewer+unknown:
      danaCo/data/collection => OK danaCo/data/collection/gen1234/
      sharedCo/data/collection => 403 dana@example.test is not authorized to sharedCo/data/collection for Read
      wideCo/data/collection => 403 dana@example.test is not authorized to wideCo/data/collection for Read
      otherCo/data/collection => 403 dana@example.test is not authorized to otherCo/data/collection for Read
    Viewer+Delegate:
      danaCo/data/collection => OK danaCo/data/collection/gen1234/
      sharedCo/data/collection => OK sharedCo/data/collection/gen1234/
      wideCo/data/collection => 403 dana@example.test is not authorized to wideCo/data/collection for Read
      otherCo/data/collection => 403 dana@example.test is not authorized to otherCo/data/collection for Read
    Viewer+Assume:
      danaCo/data/collection => OK danaCo/data/collection/gen1234/
      sharedCo/data/collection => 403 dana@example.test is not authorized to sharedCo/data/collection for Read
      wideCo/data/collection => OK wideCo/data/collection/gen1234/
      otherCo/data/collection => 403 dana@example.test is not authorized to otherCo/data/collection for Read
    Admin:
      danaCo/data/collection => OK danaCo/data/collection/gen1234/
      sharedCo/data/collection => OK sharedCo/data/collection/gen1234/
      wideCo/data/collection => 403 dana@example.test is not authorized to wideCo/data/collection for Read
      otherCo/data/collection => 403 dana@example.test is not authorized to otherCo/data/collection for Read
    all-bundles:
      danaCo/data/collection => OK danaCo/data/collection/gen1234/
      sharedCo/data/collection => OK sharedCo/data/collection/gen1234/
      wideCo/data/collection => OK wideCo/data/collection/gen1234/
      otherCo/data/collection => 403 dana@example.test is not authorized to otherCo/data/collection for Read
    "#);
}

/// Requested-but-unheld capabilities are inert: a mask may enable `Admin`,
/// but dana's live grant at the role-edge hop is `read`, and the minted
/// token gets exactly what the unmasked walk would grant there — no more.
#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../fixtures", scripts("data_planes", "masked_suite"))
)]
async fn test_unheld_bits_are_inert(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let server = test_server::TestServer::start(
        pool.clone(),
        test_server::snapshot(pool.clone(), false).await,
    )
    .await;
    let dana = server.make_access_token(DANA, Some("dana@example.test"));

    // The mask enables every Admin-bundle bit, and Write is still refused
    // at sharedCo/: the mask is a ceiling, never a grant, and the walk
    // decides from live grants. The same request under danaCo/'s direct
    // admin grant succeeds, so the refusal is the grant's, not the mask's.
    let masked_admin = mint(&server, &dana, &["Admin"]).await;
    let outcome =
        authorize_collection(&server, &masked_admin, "sharedCo/data/collection", "write").await;
    insta::assert_snapshot!(outcome, @"403 dana@example.test is not authorized to sharedCo/data/collection for Write");

    let outcome =
        authorize_collection(&server, &masked_admin, "danaCo/data/collection", "write").await;
    assert!(outcome.starts_with("OK"), "{outcome}");
}

/// Legacy response metadata attenuates to null under the mask rather than
/// leaking, and never authorizes. Dana's unmasked twin sees the role-edge
/// referent with its legacy `read` label and full gated fields; under a
/// minted `["Viewer"]` mask (no `Delegate`, so the referent's effective
/// bits are empty) the same ref presents as inaccessible — `userCapability`
/// null and gated fields null together, exactly as an unmasked user sees a
/// ref they can't access. The accessible danaCo ref still reports its
/// literal legacy label: at or above the threshold the label is
/// informational and may read broader than the mask, per decision 3.
#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../fixtures", scripts("data_planes", "masked_suite"))
)]
async fn test_legacy_metadata_cannot_authorize(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    // danaCo's collection names the role-edge collection as a write target,
    // giving the masked query an accessible ref which references one the
    // mask denies.
    sqlx::query(
        "UPDATE public.live_specs
         SET writes_to = ARRAY['sharedCo/data/collection']::catalog_name[]
         WHERE catalog_name = 'danaCo/data/collection'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let server = test_server::TestServer::start(
        pool.clone(),
        test_server::snapshot(pool.clone(), false).await,
    )
    .await;
    let dana = server.make_access_token(DANA, Some("dana@example.test"));

    let query = serde_json::json!({
        "query": r#"
        query {
            liveSpecs(by: { names: ["danaCo/data/collection"] }) {
                edges {
                    node {
                        catalogName
                        userCapability
                        liveSpec {
                            writesTo {
                                edges {
                                    node {
                                        catalogName
                                        userCapability
                                        liveSpec { catalogType }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    "#});

    let unmasked: serde_json::Value = server.graphql(&query, Some(&dana)).await;
    insta::assert_json_snapshot!(unmasked, @r#"
    {
      "data": {
        "liveSpecs": {
          "edges": [
            {
              "node": {
                "catalogName": "danaCo/data/collection",
                "liveSpec": {
                  "writesTo": {
                    "edges": [
                      {
                        "node": {
                          "catalogName": "sharedCo/data/collection",
                          "liveSpec": {
                            "catalogType": "collection"
                          },
                          "userCapability": "read"
                        }
                      }
                    ]
                  }
                },
                "userCapability": "admin"
              }
            }
          ]
        }
      }
    }
    "#);

    let masked = mint(&server, &dana, &["Viewer"]).await;
    let masked: serde_json::Value = server.graphql(&query, Some(&masked)).await;
    insta::assert_json_snapshot!(masked, @r#"
    {
      "data": {
        "liveSpecs": {
          "edges": [
            {
              "node": {
                "catalogName": "danaCo/data/collection",
                "liveSpec": {
                  "writesTo": {
                    "edges": [
                      {
                        "node": {
                          "catalogName": "sharedCo/data/collection",
                          "liveSpec": null,
                          "userCapability": null
                        }
                      }
                    ]
                  }
                },
                "userCapability": "admin"
              }
            }
          ]
        }
      }
    }
    "#);
}
