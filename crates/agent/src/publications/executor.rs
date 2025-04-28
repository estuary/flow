use agent_sql::publications::{fetch_publication, Row};
use anyhow::Context;
use models::draft_error;
use tracing::info;

use crate::{
    draft,
    publications::{
        specs, DefaultRetryPolicy, DraftPublication, ExpandDraft, JobStatus,
        PruneUnboundCollections, PublicationResult, Publisher,
    },
};

use super::MakeConnectors;

impl<MC: MakeConnectors> automations::Executor for Publisher<MC> {
    const TASK_TYPE: automations::TaskType = automations::task_types::PUBLICATIONS;

    /// We don't do anything with the inbox except log it, so this is just a
    /// generic JSON value.
    type Receive = serde_json::Value;
    type State = ();
    type Outcome = automations::Action;

    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        task_id: models::Id,
        _parent_id: Option<models::Id>,
        _state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> anyhow::Result<Self::Outcome> {
        tracing::debug!(?inbox, "starting publication task");
        let row = fetch_publication(task_id, pool).await?;
        self.handle_task(row).await?;

        // Always clear inbox, or else we'll get re-polled.
        inbox.clear();
        // Publication tasks are always done at the end. We don't retry because there is likely
        // a user waiting for the result, who could easily retry the operation themselves.
        Ok(automations::Action::Done)
    }
}

impl<MC: MakeConnectors> Publisher<MC> {
    async fn handle_task(&self, row: Row) -> anyhow::Result<()> {
        let id = row.id;

        // First ensure that the publication status is queued. Otherwise,
        // there's nothing for us to do.
        match serde_json::from_str(row.job_status.get()) {
            Ok(JobStatus::Queued) => { /* continue to publish */ }
            Ok(other) => {
                tracing::warn!(?other, "skipping publication which is no longer queued");
                return Ok(());
            }
            Err(error) => {
                // Weird edge case, but we don't update the status so that we
                // don't destroy the evidence. Return immediately and consider
                // the task completed so that the user can update the status
                // back to queued if they want.
                tracing::error!(?error, "failed to parse publication job status");
                return Ok(());
            }
        }

        let dry_run = row.dry_run;
        let draft_id = row.draft_id;

        let time_queued = chrono::Utc::now().signed_duration_since(row.updated_at);

        let (status, draft_errors, final_pub_id) = match self.process(row).await {
            Ok(result) => {
                if dry_run {
                    specs::add_built_specs_to_draft_specs(draft_id, &result.built, &self.db)
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
            Err(error) => {
                tracing::warn!(?error, pub_id = %id, "build finished with error");
                let errors = vec![draft_error::Error {
                    catalog_name: String::new(),
                    scope: None,
                    detail: format!("{error:#}"),
                }];
                (JobStatus::PublishFailed, errors, None)
            }
        };

        if !status.is_success() || dry_run {
            let mut txn = self.db.begin().await?;
            // Remove draft errors from a previous publication attempt.
            agent_sql::drafts::delete_errors(draft_id, &mut txn)
                .await
                .context("clearing old errors")?;
            draft::insert_errors(draft_id, draft_errors, &mut txn).await?;
            agent_sql::publications::resolve(id, &status, final_pub_id, &mut txn).await?;
            txn.commit()
                .await
                .context("committing failed publication transaction")?;
        }

        info!(%id, %time_queued, ?status, "publication finished");

        // As a separate transaction, delete the draft. Note that the user technically could
        // have inserted or updated draft specs after we started the publication, and those
        // would still be removed by this.
        if status.is_success() && !dry_run {
            agent_sql::publications::delete_draft(draft_id, &self.db).await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(
        pub_row_id = %row.id,
        %row.draft_id,
        %row.dry_run,
        %row.user_id,
    ))]
    async fn process(&self, row: Row) -> anyhow::Result<PublicationResult> {
        info!(
            %row.logs_token,
            %row.created_at,
            %row.updated_at,
            %row.data_plane_name,
            "processing publication",
        );

        let draft = crate::draft::load_draft(row.draft_id.into(), &self.db).await?;
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
                JobStatus::BuildFailed {
                    incompatible_collections: Vec::new(),
                    evolution_id: None,
                },
                0, //retry_count
            ));
        }

        let publication_op = DraftPublication {
            user_id: row.user_id,
            logs_token: row.logs_token,
            dry_run: row.dry_run,
            detail: row.detail.clone(),
            draft,
            verify_user_authz: true,
            default_data_plane_name: Some(row.data_plane_name.clone()).filter(|s| !s.is_empty()),
            initialize: ExpandDraft {
                filter_user_has_admin: true,
            },
            finalize: PruneUnboundCollections,
            retry: DefaultRetryPolicy,
            with_commit: (
                super::UpdatePublicationsRow { id: row.id },
                super::ClearDraftErrors {
                    draft_id: row.draft_id,
                },
            ),
        };
        let result = self.publish(publication_op).await?;

        Ok(result)
    }
}
