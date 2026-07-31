use anyhow::Context;
use control_plane_api::Snapshot;
use control_plane_api::publications::{Row, fetch_publication};
use models::draft_error;
use tracing::info;

use control_plane_api::{
    draft,
    publications::{
        ClearDraftErrors, DefaultRetryPolicy, DraftPublication, ExpandDraft, JobStatus,
        PruneUnboundCollections, PublicationResult, Publisher, RuntimeV2Rollout, StatusType,
        UpdatePublicationsRow, delete_draft, resolve, specs,
    },
};

pub struct PublicationsExecutor {
    pub publisher: Publisher,
    pub pg_pool: sqlx::PgPool,
    /// Authorization Snapshot watch. Each poll pins one Snapshot from this
    /// watch: first to cheaply defer while it remains stale for a queued
    /// publication (see `Snapshot::taken_after`), and then to serve
    /// every authorization decision of the publication itself.
    pub snapshot_watch: std::sync::Arc<dyn tokens::Watch<Snapshot>>,
    /// When true, newly-created captures are published onto runtime v2; see [`RuntimeV2Rollout`].
    pub runtime_v2_new_captures: bool,
    /// When true, newly-created materializations are published onto runtime v2; see [`RuntimeV2Rollout`].
    pub runtime_v2_new_materializations: bool,
    /// When true, newly-created derivations are published onto runtime v2; see [`RuntimeV2Rollout`].
    pub runtime_v2_new_derivations: bool,
}

/// Poll state persisted to `internal.tasks` between polls, and therefore
/// shared with whichever agent instance dequeues the next poll.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct PublicationState {
    /// The instant a Snapshot must postdate (per `Snapshot::taken_after`)
    /// before this publication is retried: the queued time its prior attempt
    /// anchored authorization staleness on. While set, polls defer — without
    /// loading or building the draft — until the local Snapshot satisfies it.
    /// Optional so that reschedules for other, future reasons aren't bound to
    /// this check.
    #[serde(default)]
    pub awaiting_snapshot_after: Option<tokens::DateTime>,
}

impl automations::Executor for PublicationsExecutor {
    const TASK_TYPE: automations::TaskType = automations::task_types::PUBLICATIONS;

    /// We don't do anything with the inbox except log it, so this is just a
    /// generic JSON value.
    type Receive = serde_json::Value;
    /// `None` — the common, never-deferred case — round-trips as the JSON
    /// `null` that stateless polls have always persisted, keeping in-flight
    /// tasks readable across a deploy in either direction.
    type State = Option<PublicationState>;
    type Outcome = automations::Action;

    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        task_id: models::Id,
        _parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> anyhow::Result<Self::Outcome> {
        tracing::debug!(?inbox, "starting publication task");
        let row = fetch_publication(task_id, pool).await?;
        let action = self.handle_task(row, state).await?;

        // Always clear inbox, or else we'll get re-polled.
        inbox.clear();
        // A publication is normally `Done` at the end — we don't retry failures
        // because a user is likely waiting and can retry themselves. The one
        // exception is a stale authorization snapshot, where `handle_task`
        // returns a `Sleep` and we re-poll until a fresher snapshot decides it.
        Ok(action)
    }
}

impl PublicationsExecutor {
    async fn handle_task(
        &self,
        row: Row,
        state: &mut Option<PublicationState>,
    ) -> anyhow::Result<automations::Action> {
        let id = row.id;

        // First ensure that the publication status is queued. Otherwise,
        // there's nothing for us to do.
        match serde_json::from_str::<'_, JobStatus>(row.job_status.get()) {
            Ok(status) if status.r#type == StatusType::Queued => { /* continue to publish */ }
            Ok(other) => {
                tracing::warn!(?other, "skipping publication which is no longer queued");
                return Ok(automations::Action::Done);
            }
            Err(error) => {
                // Weird edge case, but we don't update the status so that we
                // don't destroy the evidence. Return immediately and consider
                // the task completed so that the user can update the status
                // back to queued if they want.
                tracing::error!(?error, "failed to parse publication job status");
                return Ok(automations::Action::Done);
            }
        }

        // Pin one Snapshot for this poll: the deferral decision and every
        // authorization decision of the publication observe the same view.
        let snapshot = self.snapshot_watch.token();
        let snapshot = snapshot.result().unwrap();

        // A prior attempt was denied under a Snapshot that was not
        // authoritative for this publication. Defer — without loading or
        // building the draft — until this instance's Snapshot is, at which
        // point the retry is guaranteed to classify deterministically.
        if let Some(anchor) = state.as_ref().and_then(|s| s.awaiting_snapshot_after) {
            if !snapshot.taken_after(anchor) {
                snapshot.revoke.cancel();
                return Ok(automations::Action::Sleep(
                    Snapshot::STALE_RETRY_WAKE
                        .to_std()
                        .expect("wake interval is positive"),
                ));
            }
        }

        let dry_run = row.dry_run;
        let draft_id = row.draft_id;
        let queued_at = row.updated_at;

        let time_queued = chrono::Utc::now().signed_duration_since(row.updated_at);

        let (status, draft_errors, final_pub_id) = match self.process(row, snapshot).await {
            Ok(result) => {
                if dry_run {
                    specs::add_built_specs_to_draft_specs(draft_id, &result.built, &self.pg_pool)
                        .await
                        .context("adding built specs to draft")?;
                }
                let errors = result.draft_errors();
                let final_id = if result.status.is_success() {
                    // This `pub_id` is _not_ the same as the `id` of the `publications` table.
                    Some(result.pub_id)
                } else {
                    None
                };
                (result.status, errors, final_id)
            }
            Err(error) if validation::is_authz_snapshot_stale(&error) => {
                // An authorization denial was evaluated against a Snapshot that
                // isn't authoritative for this publication. `Publisher::publish`
                // already requested an early refresh; record the instant an
                // authoritative Snapshot must postdate and reschedule, so that
                // re-polls defer cheaply until one lands rather than reporting
                // a failure.
                tracing::info!(
                    pub_id = %id, %time_queued,
                    "publication authorization snapshot is stale; rescheduling"
                );
                state.get_or_insert_default().awaiting_snapshot_after = Some(queued_at);
                return Ok(automations::Action::Sleep(
                    Snapshot::STALE_RETRY_WAKE
                        .to_std()
                        .expect("wake interval is positive"),
                ));
            }
            Err(error) => {
                tracing::warn!(?error, pub_id = %id, "build finished with error");
                let errors = vec![draft_error::Error {
                    catalog_name: String::new(),
                    scope: None,
                    detail: format!("{error:#}"),
                }];
                (StatusType::PublishFailed.into(), errors, None)
            }
        };

        if !status.is_success() || dry_run {
            let mut txn = self.pg_pool.begin().await?;
            // Remove draft errors from a previous publication attempt.
            draft::delete_errors(draft_id, &mut txn)
                .await
                .context("clearing old errors")?;
            draft::insert_errors(draft_id, draft_errors, &mut txn).await?;
            resolve(id, &status, final_pub_id, &mut txn).await?;
            txn.commit()
                .await
                .context("committing failed publication transaction")?;
        }

        info!(%id, %time_queued, ?status, "publication finished");

        // As a separate transaction, delete the draft. Note that the user technically could
        // have inserted or updated draft specs after we started the publication, and those
        // would still be removed by this.
        if status.is_success() && !dry_run {
            delete_draft(draft_id, &self.pg_pool).await?;
        }
        Ok(automations::Action::Done)
    }

    #[tracing::instrument(skip_all, fields(
        pub_row_id = %row.id,
        %row.draft_id,
        %row.dry_run,
        %row.user_id,
    ))]
    async fn process(&self, row: Row, snapshot: &Snapshot) -> anyhow::Result<PublicationResult> {
        info!(
            %row.logs_token,
            %row.created_at,
            %row.updated_at,
            data_plane_name = %row.data_plane_name.as_deref().unwrap_or_default(),
            "processing publication",
        );

        let draft = draft::load_draft(row.draft_id.into(), &self.pg_pool).await?;
        tracing::debug!(
            n_drafted = draft.all_spec_names().count(),
            errors = draft.errors.len(),
            "resolved draft specifications"
        );
        if !draft.errors.is_empty() {
            return Ok(PublicationResult::new(
                row.id.into(),
                row.user_id,
                row.detail,
                row.updated_at,
                build::Output {
                    draft,
                    ..Default::default()
                },
                tables::Errors::default(),
                StatusType::BuildFailed.into(),
                0, //retry_count
            ));
        }

        let publication_op = DraftPublication {
            user_id: row.user_id,
            logs_token: row.logs_token,
            dry_run: row.dry_run,
            detail: row.detail.clone(),
            draft,
            // `updated_at` is the instant this row entered `queued`, and is
            // stable across our reschedules. Authorization denials evaluated
            // against a snapshot older than it are treated as not-yet-observed
            // and retried rather than reported.
            started_at: Some(row.updated_at),
            snapshot,
            verify_user_authz: true,
            default_data_plane_name: row.data_plane_name.clone().filter(|s| !s.is_empty()),
            initialize: (
                RuntimeV2Rollout {
                    new_captures: self.runtime_v2_new_captures,
                    new_materializations: self.runtime_v2_new_materializations,
                    new_derivations: self.runtime_v2_new_derivations,
                },
                ExpandDraft {
                    filter_user_has_admin: true,
                },
            ),
            finalize: PruneUnboundCollections,
            retry: DefaultRetryPolicy,
            with_commit: (
                UpdatePublicationsRow { id: row.id },
                ClearDraftErrors {
                    draft_id: row.draft_id,
                },
            ),
        };
        let result = self.publisher.publish(publication_op).await?;

        Ok(result)
    }
}
