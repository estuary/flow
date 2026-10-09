//! Admission of a user publication through the API: the caller's full
//! Subject, capability mask and prefix scope included, is authorized against
//! every catalog name the publication touches before a row is queued for the
//! publications executor.

use super::Initialize;
use anyhow::Context;

/// Authorizes a publication of `draft_id` for `subject`, with the subject's
/// full restrictions, using the same rules the executor applies, and queues
/// it. Returns the id of the queued publication. Permission denials carry a
/// `tonic::Status` so request handlers can return HTTP 307 for provisional
/// failures.
pub async fn create(
    pool: &sqlx::PgPool,
    snapshot: &crate::Snapshot,
    subject: &models::authz::Subject,
    draft_id: models::Id,
    dry_run: bool,
    default_data_plane: Option<&str>,
    detail: Option<&str>,
) -> anyhow::Result<models::Id> {
    // The staged catalog names, read with ownership enforced. The join from
    // `drafts` keeps an owned, empty draft apart from a missing one: the
    // former is a single row with a null name.
    let staged_catalog_names = sqlx::query_scalar!(
        r#"
        SELECT ds.catalog_name AS "catalog_name?"
        FROM drafts d
        LEFT JOIN draft_specs ds ON ds.draft_id = d.id
        WHERE d.id = $1 AND d.user_id = $2
        ORDER BY ds.catalog_name
        "#,
        draft_id as models::Id,
        subject.user_id,
    )
    .fetch_all(pool)
    .await?;
    if staged_catalog_names.is_empty() {
        anyhow::bail!("draft not found");
    }

    // Every staged name needs `SpecEdit` before anything about the draft's
    // contents is loaded. The names come straight from `draft_specs`, so a
    // deletion whose live specification no longer exists is authorized by
    // its name alone, and an unauthorized caller cannot learn whether it
    // exists. Names are sorted, so the first denial is deterministic.
    if let Some(name) = staged_catalog_names.iter().flatten().find(|name| {
        !snapshot.is_user_authorized(subject, name, models::authz::Capability::SpecEdit)
    }) {
        anyhow::bail!(tonic::Status::permission_denied(format!(
            "User is not authorized to create or change '{name}'"
        )));
    }

    let mut draft = crate::draft::load_draft(draft_id, pool).await?;
    if !draft.errors.is_empty() {
        let mut errors = draft
            .errors
            .iter()
            .map(|error| format!("{}: {:#}", error.scope, error.error))
            .collect::<Vec<_>>();
        errors.sort();
        anyhow::bail!("draft has errors: {}", errors.join("; "));
    }

    // Expansion is the executor's own step, run here with the caller's
    // restrictions: it adds the live tasks connected to drafted collections
    // which the caller may edit, so that the rules below also cover them.
    super::ExpandDraft {
        filter_user_can_edit: true,
        snapshot,
    }
    .initialize(pool, subject, &mut draft)
    .await?;

    // The remaining rules are evaluated over a row for every drafted or
    // referenced name plus the ops collections, existing or not, which is
    // the same set the executor resolves.
    let mut names = draft
        .all_catalog_names()
        .into_iter()
        .map(str::to_string)
        .collect::<std::collections::BTreeSet<_>>();
    names.extend(super::specs::get_ops_collection_names());
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();
    let rows = crate::live_specs::fetch_live_specs(&names, pool)
        .await
        .context("fetching live specs")?;
    super::authz::evaluate(subject, snapshot, &draft, &rows)?;

    // Ownership is re-checked within the insert, and the foreign key from
    // `draft_id` holds the draft row for the statement's duration. The
    // `create_publication_task` trigger schedules the executor within it.
    // `pub_id` is set explicitly because the `flowid` domain would otherwise
    // default it to a generated id, and it must be null until the executor
    // records a successful build's id.
    sqlx::query_scalar!(
        r#"
        INSERT INTO publications (user_id, draft_id, dry_run, data_plane_name, detail, pub_id)
        SELECT d.user_id, d.id, $3, $4, $5, NULL
        FROM drafts d WHERE d.id = $1 AND d.user_id = $2
        RETURNING id AS "id!: models::Id"
        "#,
        draft_id as models::Id,
        subject.user_id,
        dry_run,
        default_data_plane,
        detail,
    )
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("draft not found"))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::publications::test_support::{
        alice, role_grant, seed_alice_catalog, snapshot_with_grants, user_grant,
    };
    use models::authz::{CapabilityBundle, Subject};

    type Spec<'n> = (&'n str, models::CatalogType, serde_json::Value);

    async fn insert_draft(pool: &sqlx::PgPool, specs: &[Spec<'_>]) -> models::Id {
        let draft_id: models::Id =
            sqlx::query_scalar("INSERT INTO drafts (user_id) VALUES ($1) RETURNING id")
                .bind(alice())
                .fetch_one(pool)
                .await
                .unwrap();
        for (name, spec_type, model) in specs {
            sqlx::query(
                "INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec) VALUES ($1, $2, $3, $4)",
            )
            .bind(draft_id)
            .bind(name)
            .bind(spec_type)
            .bind(model)
            .execute(pool)
            .await
            .unwrap();
        }
        draft_id
    }

    fn collection(name: &str) -> Spec<'_> {
        (
            name,
            models::CatalogType::Collection,
            serde_json::to_value(models::CollectionDef::example()).unwrap(),
        )
    }

    fn materialization<'n>(name: &'n str, source: &str) -> Spec<'n> {
        let mut model = models::MaterializationDef::example();
        model.bindings[0].source = models::Source::Collection(models::Collection::new(source));
        (
            name,
            models::CatalogType::Materialization,
            serde_json::to_value(model).unwrap(),
        )
    }

    /// The rule and catalog name a denial is about, as admission folds the
    /// name into its message while the executor carries it in the scope.
    fn rule_of(message: &str, name: Option<&str>) -> String {
        let quoted = || {
            let start = message.find('\'').unwrap() + 1;
            let end = message[start..].find('\'').unwrap() + start;
            message[start..end].to_string()
        };
        let name = name.map(str::to_string).unwrap_or_else(quoted);
        let rule = if message.starts_with("User is not authorized to create or change") {
            "SpecEdit"
        } else if message.starts_with("User is not authorized to read") {
            "CatalogRead"
        } else if message.contains("is not read-authorized to") {
            "SpecRead"
        } else if message.contains("is not write-authorized to") {
            "SpecWrite"
        } else {
            panic!("unexpected denial: {message}")
        };
        format!("{rule} {name}")
    }

    // Admission denies a publication exactly when the executor records an
    // authorization error for it, and its one denial is among the
    // executor's. The two implementations are independent, so this is what
    // keeps their rules the same.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn admission_and_executor_agree(pool: sqlx::PgPool) {
        seed_alice_catalog(&pool).await;

        let admin = || user_grant(alice(), "aliceCo/", models::Capability::Admin, &[]);
        // The seeded capture and materialization of `aliceCo/data/foo` are
        // expanded into every draft of it, and need these grants.
        let reads_data = || role_grant("aliceCo/out/", "aliceCo/data/", models::Capability::Read);
        let writes_data = || role_grant("aliceCo/in/", "aliceCo/data/", models::Capability::Write);
        let unrestricted = Subject::unrestricted(alice());
        let viewer_mask = Subject {
            capability_mask: Some(CapabilityBundle::Viewer.capabilities()),
            ..unrestricted.clone()
        };
        let scoped_to_data = Subject {
            prefix_scope: Some("aliceCo/data/".to_string()),
            ..unrestricted.clone()
        };

        let cases = [
            (
                "own collection",
                vec![collection("aliceCo/data/foo")],
                snapshot_with_grants(vec![admin()], vec![reads_data(), writes_data()]),
                &unrestricted,
            ),
            (
                "viewer mask",
                vec![collection("aliceCo/data/foo")],
                snapshot_with_grants(vec![admin()], vec![reads_data(), writes_data()]),
                &viewer_mask,
            ),
            (
                "foreign name",
                vec![collection("bobCo/data/foo")],
                snapshot_with_grants(vec![admin()], vec![reads_data(), writes_data()]),
                &unrestricted,
            ),
            (
                "drafted ops collection",
                vec![collection("ops.us-central1.v1/logs")],
                snapshot_with_grants(vec![admin()], vec![reads_data(), writes_data()]),
                &unrestricted,
            ),
            (
                "spec without a read grant",
                vec![materialization("aliceCo/out/mat", "bobCo/readable/c")],
                snapshot_with_grants(
                    vec![
                        admin(),
                        user_grant(alice(), "bobCo/readable/", models::Capability::Read, &[]),
                    ],
                    vec![reads_data(), writes_data()],
                ),
                &unrestricted,
            ),
            (
                "expanded task without a read grant",
                vec![collection("aliceCo/data/foo")],
                snapshot_with_grants(vec![admin()], vec![writes_data()]),
                &unrestricted,
            ),
            (
                "expanded tasks filtered by scope",
                vec![collection("aliceCo/data/foo")],
                snapshot_with_grants(vec![admin()], vec![]),
                &scoped_to_data,
            ),
        ];

        let mut out = Vec::new();
        for (label, specs, snapshot, subject) in &cases {
            let draft_id = insert_draft(&pool, specs).await;

            let mut draft = crate::draft::load_draft(draft_id, &pool).await.unwrap();
            super::super::ExpandDraft {
                filter_user_can_edit: true,
                snapshot,
            }
            .initialize(&pool, subject, &mut draft)
            .await
            .unwrap();
            let executor =
                super::super::specs::resolve_live_specs(subject, &draft, &pool, snapshot, true)
                    .await
                    .unwrap()
                    .live
                    .errors
                    .iter()
                    .map(|error| {
                        let (_, name) = tables::parse_synthetic_scope(&error.scope).unwrap();
                        rule_of(&format!("{:#}", error.error), Some(&name))
                    })
                    .collect::<Vec<_>>();

            let admission = create(&pool, snapshot, subject, draft_id, true, None, None)
                .await
                .err()
                .map(|error| match error.downcast::<tonic::Status>() {
                    Ok(status) => rule_of(status.message(), None),
                    Err(error) => panic!("{label}: not a denial: {error:#}"),
                });

            assert_eq!(admission.is_some(), !executor.is_empty(), "{label}");
            if let Some(denial) = &admission {
                assert!(
                    executor.contains(denial),
                    "{label}: {denial} not in {executor:?}"
                );
            }
            out.push(format!("{label}: {admission:?} of {executor:?}"));
        }
        insta::assert_snapshot!(out.join("\n"), @r#"
        own collection: None of []
        viewer mask: Some("SpecEdit aliceCo/data/foo") of ["SpecEdit aliceCo/data/foo"]
        foreign name: Some("SpecEdit bobCo/data/foo") of ["SpecEdit bobCo/data/foo"]
        drafted ops collection: Some("SpecEdit ops.us-central1.v1/logs") of ["SpecEdit ops.us-central1.v1/logs"]
        spec without a read grant: Some("SpecRead aliceCo/out/mat") of ["SpecRead aliceCo/out/mat"]
        expanded task without a read grant: Some("SpecRead aliceCo/out/materialize-bar") of ["SpecRead aliceCo/out/materialize-bar"]
        expanded tasks filtered by scope: None of []
        "#);
    }

    // The insert re-checks that the caller still owns the draft, because
    // nothing holds it between the preflight and the insert.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn insert_rechecks_draft_ownership(pool: sqlx::PgPool) {
        let draft_id = insert_draft(&pool, &[collection("aliceCo/data/foo")]).await;
        let snapshot = snapshot_with_grants(
            vec![user_grant(
                alice(),
                "aliceCo/",
                models::Capability::Admin,
                &[],
            )],
            vec![],
        );

        // Each query of `create` acquires the pool's one connection. The
        // name check opens it, which `before_acquire` does not see: the hook
        // first runs for the query after it, and deletes the draft then.
        // `load_draft` reads nothing and raises no error, so only the insert
        // can notice.
        let deleted = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hooked = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .before_acquire({
                let deleted = deleted.clone();
                move |conn, _meta| {
                    let first = !deleted.swap(true, std::sync::atomic::Ordering::SeqCst);
                    Box::pin(async move {
                        if first {
                            sqlx::query("DELETE FROM drafts WHERE id = $1")
                                .bind(draft_id)
                                .execute(&mut *conn)
                                .await?;
                        }
                        Ok(true)
                    })
                }
            })
            .connect_with((*pool.connect_options()).clone())
            .await
            .unwrap();

        let error = create(
            &hooked,
            &snapshot,
            &Subject::unrestricted(alice()),
            draft_id,
            false,
            None,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "draft not found");
        assert!(deleted.load(std::sync::atomic::Ordering::SeqCst));
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM publications")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
