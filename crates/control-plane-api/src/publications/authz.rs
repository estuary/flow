//! The user- and spec-authorization rules of a publication, as evaluated at
//! API admission. Everything here is pure: the caller fetches rows and routes
//! the outcome through `Envelope::authorization_outcome`, which decides whether
//! a denial is terminal or provisional against a stale Snapshot.
//!
//! The publications executor applies the same rules inline in
//! `specs::resolve_live_specs`. The two are deliberately not shared, because
//! the executor's checks exist only for rows which arrive through PostgREST.
//! The executor also records every denial as a draft error, while admission
//! stops at the first: a denied request fails as a whole, and naming further
//! catalog names would reveal more of the catalog to a caller the first
//! denial already excludes.
//!
//! Rules:
//! - A drafted spec must itself hold read grants to its sources and write
//!   grants to its targets, which is the same walk which authorizes it to run.
//! - The user needs `CatalogRead` to every live spec which is referenced but
//!   not drafted: they must be allowed to know it exists. Ops collections are
//!   exempt because they're injected into every build.
//!
//! The user's `SpecEdit` to every drafted name is not a rule here. The caller
//! authorizes it on the staged names before the draft is loaded, so that a
//! caller who may not edit a name learns nothing from the draft's contents,
//! and expansion only adds the connected tasks the caller may edit.

use std::collections::HashSet;

/// Evaluates the authorization rules of a publication, stopping at the first
/// denial, which is a `permission_denied` status naming the catalog name it's
/// about. `draft` must already be expanded, with the user's `SpecEdit` to
/// every drafted name already authorized, and `live_rows` are the rows
/// fetched for every drafted or referenced catalog name plus the ops
/// collections, existing or not.
///
/// Rows are evaluated in catalog-name order, regardless of their given order,
/// and the rules of a name in rule order, so the denial returned for a given
/// draft and Snapshot is deterministic.
pub fn evaluate(
    subject: &models::authz::Subject,
    snapshot: &crate::Snapshot,
    draft: &tables::DraftCatalog,
    live_rows: &[crate::live_specs::LiveSpec],
) -> Result<(), tonic::Status> {
    let drafted_names = draft.all_spec_names().collect::<HashSet<_>>();
    let ops_collection_names = super::specs::get_ops_collection_names();

    let mut rows = live_rows.iter().collect::<Vec<_>>();
    rows.sort_by(|l, r| l.catalog_name.cmp(&r.catalog_name));

    for row in rows {
        let catalog_name = row.catalog_name.as_str();

        if drafted_names.contains(catalog_name) {
            let (_catalog_type, reads_from, writes_to) =
                super::specs::spec_meta(draft, catalog_name);

            // The spec-to-spec rules are evaluated with
            // `Snapshot::is_role_authorized`, the same grant walk which
            // authorizes a running task.
            //
            // Denial messages render the grants the spec holds by virtue of
            // its own name: those whose `subject_role` prefixes it. This is
            // only the directly-held grants, not the transitive reach of the
            // walk, and exists solely to make the error actionable.
            let spec_grants = || {
                serde_json::to_string_pretty(
                    &snapshot
                        .role_grants
                        .iter()
                        .filter(|grant| catalog_name.starts_with(grant.subject_role.as_str()))
                        .collect::<Vec<_>>(),
                )
                .unwrap()
            };
            for source in &reads_from {
                if !snapshot.is_role_authorized(
                    catalog_name,
                    source.as_str(),
                    models::Capability::Read,
                ) {
                    return Err(tonic::Status::permission_denied(format!(
                        "Specification '{catalog_name}' is not read-authorized to '{source}'.\nAvailable grants are: {}",
                        spec_grants(),
                    )));
                }
            }
            for target in &writes_to {
                if !snapshot.is_role_authorized(
                    catalog_name,
                    target.as_str(),
                    models::Capability::Write,
                ) {
                    return Err(tonic::Status::permission_denied(format!(
                        "Specification '{catalog_name}' is not write-authorized to '{target}'.\nAvailable grants are: {}",
                        spec_grants(),
                    )));
                }
            }
        } else if !ops_collection_names.contains(catalog_name) {
            // The user must hold `CatalogRead` to a referenced live spec
            // because a drafted spec references it, whether or not it exists.
            // The user needs no further bits: the spec is authorized
            // separately for what it reads and writes.
            if !snapshot.is_user_authorized(
                subject,
                catalog_name,
                models::authz::Capability::CatalogRead,
            ) {
                return Err(tonic::Status::permission_denied(format!(
                    "User is not authorized to read '{catalog_name}'"
                )));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::publications::test_support::{alice, role_grant, snapshot_with_grants, user_grant};
    use models::authz::{CapabilityBundle, Subject};
    use std::collections::BTreeSet;

    /// A row as `fetch_live_specs` returns it for a name, whether or not a
    /// live spec exists. The rules never inspect the spec itself.
    fn row(catalog_name: &str) -> crate::live_specs::LiveSpec {
        crate::live_specs::LiveSpec {
            id: models::Id::zero(),
            last_pub_id: models::Id::zero(),
            last_build_id: models::Id::zero(),
            data_plane_id: models::Id::zero(),
            catalog_name: catalog_name.to_string(),
            spec_type: None,
            spec: None,
            built_spec: None,
            inferred_schema_md5: None,
            dependency_hash: None,
        }
    }

    /// Rows for every drafted or referenced name of `draft`, plus the ops
    /// collections, in name order.
    fn rows_of(draft: &tables::DraftCatalog) -> Vec<crate::live_specs::LiveSpec> {
        let mut names = draft
            .all_catalog_names()
            .iter()
            .map(|name| name.to_string())
            .collect::<BTreeSet<_>>();
        names.extend(super::super::specs::get_ops_collection_names());
        names.iter().map(|name| row(name)).collect()
    }

    fn render(outcome: Result<(), tonic::Status>) -> String {
        match outcome {
            Ok(()) => "authorized".to_string(),
            Err(status) => format!("{:?}: {}", status.code(), status.message()),
        }
    }

    fn materialization(name: &str, sources: &[&str]) -> (String, models::AnySpec) {
        let mut model = models::MaterializationDef::example();
        let binding = model.bindings.pop().unwrap();
        model.bindings = sources
            .iter()
            .map(|source| models::MaterializationBinding {
                source: models::Source::Collection(models::Collection::new(*source)),
                ..binding.clone()
            })
            .collect();
        (name.to_string(), models::AnySpec::Materialization(model))
    }

    fn capture(name: &str, target: &str) -> (String, models::AnySpec) {
        let mut model = models::CaptureDef::example();
        model.bindings[0].target = models::Collection::new(target);
        (name.to_string(), models::AnySpec::Capture(model))
    }

    fn draft_of(specs: Vec<(String, models::AnySpec)>) -> tables::DraftCatalog {
        let mut draft = tables::DraftCatalog::default();
        for (name, model) in specs {
            let scope = tables::synthetic_scope(model.catalog_type(), &name);
            draft.add_any_spec(&name, scope, None, model, false);
        }
        draft
    }

    #[test]
    fn test_user_rules_on_drafted_and_referenced_names() {
        // `aliceCo/out/mat` references a collection of its own tenant, one of
        // another tenant, and an injected ops collection.
        let draft = draft_of(vec![materialization(
            "aliceCo/out/mat",
            &[
                "aliceCo/data/foo",
                "bobCo/missing",
                "ops.us-central1.v1/logs",
            ],
        )]);
        // Rows are reversed to show that the first denial is the one of the
        // least catalog name, not the first row.
        let mut rows = rows_of(&draft);
        rows.reverse();

        // Every spec-to-spec grant the materialization needs is present, so
        // each denial below is attributable to the user rules alone.
        let snapshot = || {
            snapshot_with_grants(
                vec![user_grant(
                    alice(),
                    "aliceCo/",
                    models::Capability::Admin,
                    &[],
                )],
                vec![
                    role_grant("aliceCo/out/", "aliceCo/data/", models::Capability::Read),
                    role_grant("aliceCo/", "bobCo/", models::Capability::Read),
                    role_grant("aliceCo/", "ops.us-central1.v1/", models::Capability::Read),
                ],
            )
        };
        let unrestricted = Subject::unrestricted(alice());
        let viewer_mask = Subject {
            capability_mask: Some(CapabilityBundle::Viewer.capabilities()),
            ..unrestricted.clone()
        };
        let edit_only_mask = Subject {
            capability_mask: Some(models::authz::Capability::SpecEdit.into()),
            ..unrestricted.clone()
        };
        let scoped_to_in = Subject {
            prefix_scope: Some("aliceCo/in/".to_string()),
            ..unrestricted.clone()
        };

        // A viewer mask is denied `CatalogRead` to `bobCo/missing`, because a
        // mask without `Delegate` ends the grant walk at the user's own grant.
        // A `SpecEdit`-only mask and a prefix scope are each denied
        // `aliceCo/data/foo` before `bobCo/missing`.
        let cases = [
            ("unrestricted", &unrestricted),
            ("viewer mask", &viewer_mask),
            ("SpecEdit-only mask", &edit_only_mask),
            ("scoped to aliceCo/in/", &scoped_to_in),
        ];
        let out = cases
            .iter()
            .map(|(label, subject)| {
                let snapshot = snapshot();
                let outcome = evaluate(subject, &snapshot, &draft, &rows);
                // Evaluation is pure: the caller decides whether to refresh.
                assert!(!snapshot.revoke.is_cancelled());
                format!("[{label}] {}", render(outcome))
            })
            .collect::<Vec<_>>()
            .join("\n");
        insta::assert_snapshot!(out, @"
        [unrestricted] authorized
        [viewer mask] PermissionDenied: User is not authorized to read 'bobCo/missing'
        [SpecEdit-only mask] PermissionDenied: User is not authorized to read 'aliceCo/data/foo'
        [scoped to aliceCo/in/] PermissionDenied: User is not authorized to read 'aliceCo/data/foo'
        ");
    }

    #[test]
    fn test_ops_collections_are_exempt_from_catalog_read() {
        // The specs hold read grants to everything they reference, and a
        // viewer mask without `Delegate` keeps the user from reaching any of
        // it. Only the ops collection is exempt from the user's `CatalogRead`.
        let snapshot = snapshot_with_grants(
            vec![user_grant(
                alice(),
                "aliceCo/",
                models::Capability::Admin,
                &[],
            )],
            vec![
                role_grant(
                    "aliceCo/out/",
                    "ops.us-central1.v1/",
                    models::Capability::Read,
                ),
                role_grant("aliceCo/out/", "bobCo/", models::Capability::Read),
            ],
        );
        let viewer_mask = Subject {
            capability_mask: Some(CapabilityBundle::Viewer.capabilities()),
            ..Subject::unrestricted(alice())
        };

        let draft = draft_of(vec![materialization(
            "aliceCo/out/mat",
            &["ops.us-central1.v1/logs"],
        )]);
        let rows = rows_of(&draft);
        insta::assert_snapshot!(render(evaluate(&viewer_mask, &snapshot, &draft, &rows)), @"authorized");

        let draft = draft_of(vec![materialization(
            "aliceCo/out/mat",
            &["bobCo/x", "ops.us-central1.v1/logs"],
        )]);
        let rows = rows_of(&draft);
        insta::assert_snapshot!(render(evaluate(&viewer_mask, &snapshot, &draft, &rows)), @"PermissionDenied: User is not authorized to read 'bobCo/x'");
    }

    #[test]
    fn test_spec_to_spec_rules() {
        // Alice may edit and read everything here, but the specs themselves
        // hold no grants to their sources and targets.
        let snapshot = snapshot_with_grants(
            vec![user_grant(
                alice(),
                "aliceCo/",
                models::Capability::Admin,
                &[],
            )],
            vec![role_grant(
                "aliceCo/out/",
                "aliceCo/other/",
                models::Capability::Read,
            )],
        );
        let subject = Subject::unrestricted(alice());

        // Sources are evaluated in name order, so of two unauthorized sources
        // only the first is reported.
        let draft = draft_of(vec![materialization(
            "aliceCo/out/mat",
            &["aliceCo/data/foo", "aliceCo/data/bar"],
        )]);
        let rows = rows_of(&draft);
        insta::assert_snapshot!(render(evaluate(&subject, &snapshot, &draft, &rows)), @r#"
        PermissionDenied: Specification 'aliceCo/out/mat' is not read-authorized to 'aliceCo/data/bar'.
        Available grants are: [
          {
            "subject_role": "aliceCo/out/",
            "object_role": "aliceCo/other/",
            "capability": "read",
            "bundles": []
          }
        ]
        "#);

        // A capture's write denial precedes the materialization's read
        // denial only because its name sorts first.
        let draft = draft_of(vec![
            materialization("aliceCo/out/mat", &["aliceCo/data/foo"]),
            capture("aliceCo/in/cap", "aliceCo/data/foo"),
        ]);
        let rows = rows_of(&draft);
        insta::assert_snapshot!(render(evaluate(&subject, &snapshot, &draft, &rows)), @"
        PermissionDenied: Specification 'aliceCo/in/cap' is not write-authorized to 'aliceCo/data/foo'.
        Available grants are: []
        ");
    }
}
