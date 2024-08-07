use agent_sql::{publications::Row, Capability};
use anyhow::Context;
use tracing::info;

use crate::{
    draft,
    publications::{specs, JobStatus, PublicationResult, Publisher},
    HandleResult, Handler,
};

use super::IncompatibleCollection;

#[async_trait::async_trait]
impl Handler for Publisher {
    async fn handle(
        &mut self,
        pg_pool: &sqlx::PgPool,
        allow_background: bool,
    ) -> anyhow::Result<HandleResult> {
        loop {
            let mut txn = pg_pool.begin().await?;

            let row: Row =
                match agent_sql::publications::dequeue(&mut txn, allow_background).await? {
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

            let (status, draft_errors) = match self.process(row).await {
                Ok(result) => {
                    let errors = result.draft_errors();
                    (result.status, errors)
                }
                Err(error) => {
                    tracing::warn!(?error, pub_id = %id, "build finished with error");
                    let errors = vec![draft::Error {
                        catalog_name: String::new(),
                        scope: None,
                        detail: format!("{error:#}"),
                    }];
                    (JobStatus::PublishFailed, errors)
                }
            };

            draft::insert_errors(draft_id, draft_errors, &mut txn).await?;

            info!(%id, %time_queued, %background, ?status, "build finished");
            agent_sql::publications::resolve(id, &status, &mut txn).await?;

            txn.commit().await?;

            // As a separate transaction, delete the draft. Note that the user technically could
            // have inserted or updated draft specs after we started the publication, and those
            // would still be removed by this.
            if (status.is_success() || status.is_empty_draft()) && !dry_run {
                agent_sql::publications::delete_draft(draft_id, pg_pool).await?;
            }
            return Ok(HandleResult::HadJob);
        }
    }

    fn table_name(&self) -> &'static str {
        "publications"
    }
}

impl Publisher {
    pub async fn process(&mut self, row: Row) -> anyhow::Result<PublicationResult> {
        info!(
            %row.created_at,
            %row.draft_id,
            %row.dry_run,
            %row.logs_token,
            %row.updated_at,
            %row.user_id,
            %row.background,
            "processing publication",
        );

        let mut attempt = 0;
        loop {
            attempt += 1;
            let draft = specs::load_draft(row.draft_id.into(), &self.db).await?;
            // let all_drafted_specs = draft
            //     .all_spec_names()
            //     .map(|n| n.to_string())
            //     .collect::<BTreeSet<_>>();
            tracing::debug!(
                %attempt,
                n_drafted = draft.all_spec_names().count(),
                errors = draft.errors.len(),
                //spec_names = ?all_drafted_specs,
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
                ));
            }
            let mut result = self.try_process(&row, draft).await?;

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

            let JobStatus::ExpectPubIdMismatch { failures } = &result.status else {
                return Ok(result);
            };
            if attempt == Publisher::MAX_OPTIMISTIC_LOCKING_RETRIES {
                tracing::error!(%attempt, ?failures, "giving up after maximum number of optimistic locking retries");
                return Ok(result);
            } else {
                // TODO: increment a prometheus counter of lock failures
                tracing::info!(%attempt, ?failures, "retrying after optimistic locking failure");
            }
        }
    }

    #[tracing::instrument(err, skip_all, fields(id=%row.pub_id, user_id=%row.user_id, updated_at=%row.updated_at))]
    pub async fn try_process(
        &mut self,
        row: &Row,
        mut draft: tables::DraftCatalog,
    ) -> anyhow::Result<PublicationResult> {
        // Expand the set of drafted specs to include any tasks that read from or write to any of
        // the published collections. We do this so that validation can catch any inconsistencies
        // or failed tests that may be introduced by the publication.
        let drafted_collections = draft
            .collections
            .iter()
            .map(|d| d.collection.as_str())
            .collect::<Vec<_>>();
        let all_drafted_specs = draft.all_spec_names().collect::<Vec<_>>();
        let expanded_rows = agent_sql::live_specs::fetch_expanded_live_specs(
            row.user_id,
            &drafted_collections,
            &all_drafted_specs,
            &self.db,
        )
        .await?;
        let mut expanded_names = Vec::with_capacity(expanded_rows.len());
        for exp in expanded_rows {
            if !exp
                .user_capability
                .map(|c| c == Capability::Admin)
                .unwrap_or(false)
            {
                // Skip specs that the user doesn't have permission to change, as it would just
                // cause errors during the build.
                continue;
            }
            let Some(spec_type) = exp.spec_type.map(Into::into) else {
                anyhow::bail!("missing spec_type for expanded row: {:?}", exp.catalog_name);
            };
            let Some(model_json) = &exp.spec else {
                anyhow::bail!("missing spec for expanded row: {:?}", exp.catalog_name);
            };
            let scope = tables::synthetic_scope(spec_type, &exp.catalog_name);
            if let Err(e) = draft.add_spec(
                spec_type,
                &exp.catalog_name,
                scope,
                Some(exp.last_pub_id.into()),
                Some(&model_json),
            ) {
                draft.errors.push(e);
            }
            expanded_names.push(exp.catalog_name);
        }
        if !draft.errors.is_empty() {
            return Ok(PublicationResult::new(
                row.pub_id.into(),
                row.user_id,
                row.detail.clone(),
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
            ));
        }
        tracing::debug!(
            n_expanded = expanded_names.len(),
            ?expanded_names,
            "expanded draft"
        );

        let built = self
            .build(
                row.user_id,
                row.pub_id.into(),
                row.detail.clone(),
                draft,
                row.logs_token,
            )
            .await?;
        if built.has_errors() {
            return Ok(built.build_failed());
        }

        if row.dry_run {
            // Add built specs to the draft for dry runs after rolling back other changes that do
            // not apply to dry runs.
            specs::add_built_specs_to_draft_specs(row.draft_id, &built.output, &self.db)
                .await
                .context("adding built specs to draft")?;

            return Ok(built.into_result(chrono::Utc::now(), JobStatus::Success));
        };

        self.commit(built).await.context("committing publication")
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
