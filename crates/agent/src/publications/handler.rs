use agent_sql::publications::Row;
use anyhow::Context;
use models::draft_error;
use tracing::info;

use crate::{
    draft,
    publications::{
        initialize::UpdateInferredSchemas, specs, DefaultRetryPolicy, DraftPublication,
        ExpandDraft, IncompatibleCollection, JobStatus, PruneUnboundCollections, PublicationResult,
        Publisher,
    },
    HandleResult, Handler,
};

#[async_trait::async_trait]
impl Handler for Publisher {
    async fn handle(
        &mut self,
        pg_pool: &sqlx::PgPool,
        allow_background: bool,
    ) -> anyhow::Result<HandleResult> {
        let mut txn = pg_pool.begin().await?;

        let row: Row = match agent_sql::publications::dequeue(&mut txn, allow_background).await? {
            None => return Ok(HandleResult::NoJobs),
            Some(row) => row,
        };

        let id = row.pub_id;
        let background = row.background;
        let dry_run = row.dry_run;
        let draft_id = row.draft_id;

        // Remove draft errors from a previous publication attempt.
        agent_sql::drafts::delete_errors(row.draft_id, &mut txn)
            .await
            .context("clearing old errors")?;

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

        draft::insert_errors(draft_id, draft_errors, &mut txn).await?;

        info!(%id, %time_queued, %background, ?status, "publication finished");
        agent_sql::publications::resolve(id, &status, final_pub_id, &mut txn).await?;

        txn.commit().await?;

        // As a separate transaction, delete the draft. Note that the user technically could
        // have inserted or updated draft specs after we started the publication, and those
        // would still be removed by this.
        if status.is_success() && !dry_run {
            agent_sql::publications::delete_draft(draft_id, pg_pool).await?;
        }
        Ok(HandleResult::HadJob)
    }

    fn table_name(&self) -> &'static str {
        "publications"
    }
}

impl Publisher {
    #[tracing::instrument(skip_all, fields(
        pub_row_id = %row.pub_id,
        %row.draft_id,
        %row.dry_run,
        %row.user_id,
        %row.background,
    ))]
    pub async fn process(&mut self, row: Row) -> anyhow::Result<PublicationResult> {
        info!(
            %row.logs_token,
            %row.created_at,
            %row.updated_at,
            %row.data_plane_name,
            "processing publication",
        );
        if row.background {
            // Terminal error: background publications are no longer supported.
            return Ok(PublicationResult::new(
                row.pub_id,
                row.user_id,
                row.detail,
                row.updated_at,
                build::Output::default(),
                tables::Errors::default(),
                JobStatus::DeprecatedBackground,
                0,
            ));
        }

        let draft = crate::draft::load_draft(row.draft_id.into(), &self.db).await?;
        tracing::debug!(
            n_drafted = draft.all_spec_names().count(),
            errors = draft.errors.len(),
            "resolved draft specifications"
        );
        if !draft.errors.is_empty() {
            return Ok(PublicationResult::new(
                row.pub_id.into(),
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
            initialize: (
                UpdateInferredSchemas,
                ExpandDraft {
                    filter_user_has_admin: true,
                },
            ),
            finalize: PruneUnboundCollections,
            retry: DefaultRetryPolicy,
        };
        let mut result = self.publish(publication_op).await?;

        // If this is a result of a build failure, then we may need to create an evolutions job in response.
        if let JobStatus::BuildFailed {
            incompatible_collections,
            evolution_id,
        } = &mut result.status
        {
            if !incompatible_collections.is_empty() && row.auto_evolve {
                let collections = to_evolutions_collections(&incompatible_collections);
                let detail = format!(
                    "system created in response to failed publication: {}",
                    row.pub_id
                );
                let next_job = agent_sql::evolutions::create(
                    &self.db,
                    row.user_id,
                    row.draft_id,
                    collections,
                    true, // auto_publish
                    detail,
                )
                .await
                .context("creating evolutions job")?;
                *evolution_id = Some(next_job.into());
            }
        }
        Ok(result)
    }
}

fn to_evolutions_collections(
    incompatible_collections: &[IncompatibleCollection],
) -> Vec<serde_json::Value> {
    incompatible_collections
        .iter()
        .map(|ic| {
            // Do we need to re-create the whole collection, or can we just re-create materialization bindings?
            let (new_name, materializations) = if ic.requires_recreation.is_empty() {
                // Since we're not re-creating the collection, specify only the materializations that
                // have failed validation. This avoids potentially backfilling other materializations
                // unnecessarily.
                (None, ic.affected_materializations.iter().map(|c| c.name.clone()).collect())
            } else {
                tracing::debug!(reasons = ?ic.requires_recreation, collection = %ic.collection, "will attempt to re-create collection");
                // Since we are re-creating the collection, all materializations will be affected.
                (Some(crate::next_name(&ic.collection)), Vec::new())
            };
            serde_json::to_value(crate::evolution::EvolveRequest {
                current_name: ic.collection.clone(),
                new_name,
                materializations,
            })
            .unwrap()
        })
        .collect()
}
