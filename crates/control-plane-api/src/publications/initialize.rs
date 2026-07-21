use crate::Snapshot;
use anyhow::Context;
use itertools::Itertools;
use models::Capability;
use std::{future::Future, sync::Arc};
use uuid::Uuid;

/// Initialize a draft prior to build/validation. This may add additional specs to the draft.
pub trait Initialize: Send + Sync {
    fn initialize(
        &self,
        db: &sqlx::PgPool,
        user_id: Uuid,
        draft: &mut tables::DraftCatalog,
        snapshot_watch: &Arc<dyn tokens::Watch<Snapshot>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// A no-op `Initialize` impl, for when you don't want to expand the draft.
pub struct NoopInitialize;
impl Initialize for NoopInitialize {
    async fn initialize(
        &self,
        _db: &sqlx::PgPool,
        _user_id: Uuid,
        _draft: &mut tables::DraftCatalog,
        _snapshot_watch: &Arc<dyn tokens::Watch<Snapshot>>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<I1, I2> Initialize for (I1, I2)
where
    I1: Initialize,
    I2: Initialize,
{
    async fn initialize(
        &self,
        db: &sqlx::PgPool,
        user_id: Uuid,
        draft: &mut tables::DraftCatalog,
        snapshot_watch: &Arc<dyn tokens::Watch<Snapshot>>,
    ) -> anyhow::Result<()> {
        self.0
            .initialize(db, user_id, draft, snapshot_watch)
            .await?;
        self.1
            .initialize(db, user_id, draft, snapshot_watch)
            .await?;
        Ok(())
    }
}

/// An `Initialize` that expands the draft to touch live specs that read from or write to
/// any drafted collections. This may optionally filter the specs based on whether the user
/// has `admin` capability to them.
pub struct ExpandDraft {
    /// Whether to filter specs based on the user's capability. If true, then only specs for which
    /// the user has `admin` capability will be added to the draft.
    pub filter_user_has_admin: bool,
}

impl Initialize for ExpandDraft {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        err,
        fields(filter_user_has_admin = self.filter_user_has_admin)
    )]
    async fn initialize(
        &self,
        db: &sqlx::PgPool,
        user_id: Uuid,
        draft: &mut tables::DraftCatalog,
        snapshot_watch: &Arc<dyn tokens::Watch<Snapshot>>,
    ) -> anyhow::Result<()> {
        // Expand the set of drafted specs to include any tasks that read from or write to any of
        // the published collections. We do this so that validation can catch any inconsistencies
        // or failed tests that may be introduced by the publication.
        let drafted_collections = draft
            .collections
            .iter()
            .map(|d| d.collection.as_str())
            .collect::<Vec<_>>();
        let all_drafted_specs = draft.all_spec_names().collect::<Vec<_>>();

        let capability_filter = if self.filter_user_has_admin {
            Some(Capability::Admin)
        } else {
            None
        };
        let snapshot = snapshot_watch.token();
        let snapshot = snapshot.result().unwrap();
        let prefixes_and_capabilities = snapshot.prefix_and_capabilities_per_user(user_id);

        let expanded_catalog = crate::live_specs::get_connected_live_specs(
            // user_id,
            &drafted_collections,
            &all_drafted_specs,
            capability_filter,
            db,
            &prefixes_and_capabilities,
        )
        .await?;
        tracing::debug!(
            expanded_names = %expanded_catalog.all_spec_names().format(","),
            "expanded draft"
        );

        draft.add_live(expanded_catalog);

        Ok(())
    }
}

pub struct RuntimeV2Rollout {
    /// When true, newly-created captures without an explicit flag run runtime v2.
    pub new_captures: bool,
}

impl Initialize for RuntimeV2Rollout {
    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn initialize(
        &self,
        db: &sqlx::PgPool,
        _user_id: Uuid,
        draft: &mut tables::DraftCatalog,
        _snapshot_watch: &Arc<dyn tokens::Watch<Snapshot>>,
    ) -> anyhow::Result<()> {
        if !self.new_captures {
            return Ok(());
        }
        let flag = models::Token::new(models::ENABLE_RUNTIME_V2);

        // Candidate captures: drafted, not a touch or deletion, whose model
        // hasn't set the flag explicitly.
        let is_candidate = |row: &tables::DraftCapture| {
            !row.is_touch
                && row
                    .model
                    .as_ref()
                    .is_some_and(|model| !model.shards.flags.contains_key(&flag))
        };
        let candidates = draft
            .captures
            .iter()
            .filter(|row| is_candidate(row))
            .map(|row| row.capture.to_string())
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(());
        }

        // Only *new* captures are enabled. A candidate that already has a
        // non-tombstone live spec is an update, so it's left as-is. A tombstone
        // (`spec is null`, a deleted spec a controller hasn't yet reaped) is
        // excluded by `spec is not null`, so re-creating a capture counts as new.
        let existing: std::collections::HashSet<String> = sqlx::query!(
            r#"select catalog_name
               from live_specs
               where catalog_name = any($1::text[]) and spec is not null"#,
            &candidates as &[String],
        )
        .fetch_all(db)
        .await
        .context("fetching existing capture names")?
        .into_iter()
        .map(|row| row.catalog_name)
        .collect();

        for row in draft.captures.iter_mut() {
            if is_candidate(row) && !existing.contains(row.capture.as_str()) {
                row.model
                    .as_mut()
                    .unwrap()
                    .shards
                    .flags
                    .insert(flag.clone(), models::Token::new("true"));
            }
        }

        Ok(())
    }
}
