use super::Initialize;

/// Queues a publication of `draft_id` for the publications executor, after
/// authorizing `subject`, with its capability mask and prefix scope, against
/// the user rules the executor applies. Returns the id and logs token of the
/// queued publication. Permission denials are a `tonic::Status` in the error
/// chain, which callers pass to `Envelope::authorization_outcome`.
pub async fn submit(
    pool: &sqlx::PgPool,
    snapshot: &crate::Snapshot,
    subject: &models::authz::Subject,
    draft_id: models::Id,
    dry_run: bool,
    default_data_plane: Option<&str>,
    detail: Option<&str>,
) -> anyhow::Result<(models::Id, uuid::Uuid)> {
    // An owned, empty draft yields one row with a null `catalog_name`, while a
    // missing or unowned draft yields none.
    let staged_names = sqlx::query_scalar!(
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
    if staged_names.is_empty() {
        anyhow::bail!("draft not found");
    }

    // Every staged name needs `SpecEdit`, checked from `draft_specs` because
    // the loaded draft omits a deletion of a nonexistent live spec: checking
    // only the loaded draft would deny deleting an existing spec but queue
    // deleting a nonexistent one, telling the caller which it is. Names are
    // sorted, so the first denial is deterministic.
    if let Some(name) = staged_names.iter().flatten().find(|name| {
        !snapshot.is_user_authorized(subject, name, models::authz::Capability::SpecEdit)
    }) {
        anyhow::bail!(tonic::Status::permission_denied(format!(
            "User is not authorized to create or change '{name}'"
        )));
    }

    // The executor loads the draft again and reports its load errors.
    let mut draft = crate::draft::load_draft(draft_id, pool).await?;

    // Expand under `Subject::unrestricted`, as the executor does, so the
    // expanded set matches what it builds.
    super::ExpandDraft {
        filter_user_can_edit: true,
        snapshot,
        expected_content_hash: None,
    }
    .initialize(
        pool,
        &models::authz::Subject::unrestricted(subject.user_id),
        &mut draft,
    )
    .await?;

    // The user rules of `specs::resolve_live_specs`: every drafted name,
    // expanded ones included, needs `SpecEdit`, and every other referenced
    // name except the ops collections needs `CatalogRead`.
    let drafted_names = draft
        .all_spec_names()
        .collect::<std::collections::HashSet<_>>();
    let ops_collection_names = super::specs::get_ops_collection_names();
    for name in draft.all_catalog_names() {
        let (capability, action) = if drafted_names.contains(name) {
            (models::authz::Capability::SpecEdit, "create or change")
        } else if ops_collection_names.contains(name) {
            continue;
        } else {
            (models::authz::Capability::CatalogRead, "read")
        };
        if !snapshot.is_user_authorized(subject, name, capability) {
            anyhow::bail!(tonic::Status::permission_denied(format!(
                "User is not authorized to {action} '{name}'"
            )));
        }
    }

    // Re-selecting the owned draft from `drafts` like this prevents queueing a
    // publication for a draft that no longer exists, if it was deleted after
    // the checks above. The `create_publication_task` trigger creates the
    // executor's task within this statement, so the row and its task are
    // created together or not at all.
    let draft_content_hash = crate::draft::content_hash(&draft);
    let row = sqlx::query!(
        r#"
        INSERT INTO publications (user_id, draft_id, dry_run, data_plane_name, detail, draft_content_hash)
        SELECT d.user_id, d.id, $3, $4, $5, $6
        FROM drafts d WHERE d.id = $1 AND d.user_id = $2
        RETURNING id AS "id!: models::Id", logs_token
        "#,
        draft_id as models::Id,
        subject.user_id,
        dry_run,
        default_data_plane,
        detail,
        draft_content_hash,
    )
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("draft not found"))?;

    Ok((row.id, row.logs_token))
}
