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
    // deletion whose live specification does not exist is authorized by
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

    // Expansion is the executor's own step, which it runs as the unrestricted
    // account: it adds the live tasks connected to drafted collections which
    // the account may edit. It's repeated here as the executor runs it, so
    // that the expanded set is the one it will build and touch, and then
    // every expanded name is authorized against the caller's full Subject.
    // Expanding with the caller's restrictions instead would omit a task the
    // token cannot edit, which the executor would then add back and touch
    // without this check.
    super::ExpandDraft {
        filter_user_can_edit: true,
        snapshot,
    }
    .initialize(
        pool,
        &models::authz::Subject::unrestricted(subject.user_id),
        &mut draft,
    )
    .await?;

    let expanded_names = draft
        .all_spec_names()
        .collect::<std::collections::BTreeSet<_>>();
    if let Some(name) = expanded_names.iter().find(|name| {
        !snapshot.is_user_authorized(subject, name, models::authz::Capability::SpecEdit)
    }) {
        anyhow::bail!(tonic::Status::permission_denied(format!(
            "User is not authorized to create or change '{name}'"
        )));
    }

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

    // The insert selects from `drafts` under the same ownership predicate as
    // the preflight, so a draft deleted or re-owned since then inserts
    // nothing rather than queuing a publication of it. The
    // `create_publication_task` trigger schedules the executor within the
    // statement. `pub_id` is set explicitly because the `flowid` domain
    // would otherwise default it to a generated id, and it must be null
    // until the executor records the effective publication id of a success.
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
