use agent_sql::{publications::Row, Capability};
use anyhow::Context;
use tracing::info;

use crate::{
    draft,
    publications::{specs, JobStatus, PublicationResult, Publisher},
    HandleResult, Handler,
};

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
            let process_result = self.process(row, pg_pool, false).await;

            let result = match process_result {
                Ok(result) => result,
                Err(err) if crate::is_acquire_lock_error(&err) => {
                    tracing::info!(%id, %time_queued, "cannot acquire all row locks for publication (will retry)");
                    // Since we failed to acquire a necessary row lock, wait a short
                    // while and then try again.
                    txn.rollback().await?;
                    // The sleep is really just so we don't spam the DB in a busy
                    // loop.  I arrived at these values via the very scientific 😉
                    // process of reproducing failures using a couple of different
                    // values and squinting at the logs in my terminal. In
                    // practice, it's common for another agent process to pick up
                    // the job while this one is sleeping, which is why I didn't
                    // see a need for jitter. All agents process the job queue in
                    // the same order, so the next time any agent polls the
                    // handler, it should get this same job, since we've released
                    // the lock on the job row.
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                Err(other_err) => return Err(other_err),
            };

            draft::insert_errors(draft_id, result.draft_errors(), &mut txn).await?;

            info!(%id, %time_queued, %background, status = ?result.publication_status, "build finished");
            agent_sql::publications::resolve(id, &result.publication_status, &mut txn).await?;

            txn.commit().await?;

            // As a separate transaction, delete the draft if it has no draft_specs.
            // The user could have raced an insertion of a new spec.
            if (result.publication_status.is_success()
                || result.publication_status.is_empty_draft())
                && !dry_run
            {
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
    #[tracing::instrument(err, skip_all, fields(id=%row.pub_id, user_id=%row.user_id, updated_at=%row.updated_at))]
    pub async fn process(
        &mut self,
        row: Row,
        db: &sqlx::PgPool,
        test_run: bool,
    ) -> anyhow::Result<PublicationResult> {
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

        let mut draft = specs::load_draft(row.draft_id.into(), db).await?;
        let all_drafted_specs = draft.all_spec_names().collect::<Vec<_>>();
        tracing::debug!(
            n_drafted = all_drafted_specs.len(),
            errors = draft.errors.len(),
            spec_names = ?all_drafted_specs,
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
                JobStatus::BuildFailed {
                    incompatible_collections: Vec::new(),
                    evolution_id: None,
                },
            ));
        }

        // Expand the set of drafted specs to include any tasks that read from or write to any of
        // the published collections. We do this so that validation can catch any inconsistencies
        // or failed tests that may be introduced by the publication.
        let drafted_collections = draft
            .collections
            .iter()
            .map(|d| d.collection.as_str())
            .collect::<Vec<_>>();
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
                row.detail,
                row.updated_at,
                build::Output {
                    draft,
                    ..Default::default()
                },
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

        // TODO: get publication handler tests working again
        // if test_run {
        //     return Ok(PublicationResult {
        //         completed_at: Utc::now(),
        //         publication_id: row.pub_id.into(),
        //         draft: hack_draft_catalog,
        //         live: hack_live_catalog,
        //         validated: Default::default(),
        //         publication_status: JobStatus::success(Vec::new()),
        //     });
        // }

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
            return Ok(built.into_result(
                chrono::Utc::now(),
                JobStatus::BuildFailed {
                    incompatible_collections: Vec::new(),
                    evolution_id: None,
                },
            ));
        }

        if row.dry_run {
            // Add built specs to the draft for dry runs after rolling back other changes that do
            // not apply to dry runs.
            specs::add_built_specs_to_draft_specs(row.draft_id, &built.output, db)
                .await
                .context("adding built specs to draft")?;

            return Ok(built.into_result(chrono::Utc::now(), JobStatus::success(Vec::new())));
        };

        self.commit(row.logs_token, built, db)
            .await
            .context("committing publication")
    }
}
