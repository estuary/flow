use std::collections::HashSet;

use crate::controllers::PublicationResult;

use self::builds::IncompatibleCollection;
use self::validation::ControlPlane;
use super::{
    draft::{self, Error},
    logs, HandleResult, Handler, Id,
};
use agent_sql::{connector_tags::UnknownConnector, publications::Row, CatalogType};
use anyhow::Context;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::info;

pub mod builds;
mod linked_materializations;
pub mod specs;
mod storage;
mod validation;

/// JobStatus is the possible outcomes of a handled draft submission.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    BuildFailed {
        #[serde(skip_serializing_if = "Vec::is_empty")]
        incompatible_collections: Vec<IncompatibleCollection>,
        #[serde(skip_serializing_if = "Option::is_none")]
        evolution_id: Option<Id>,
    },
    TestFailed,
    PublishFailed,
    Success {
        /// If any materializations are to be updated in response to this publication,
        /// their publication ids will be included here. This is purely informational.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        linked_materialization_publications: Vec<Id>,
    },
    /// Returned when there are no draft specs (after pruning unbound
    /// collections). There will not be any `draft_errors` in this case, because
    /// there's no `catalog_name` to associate with an error. And it may not be
    /// desirable to treat this as an error, depending on the scenario.
    EmptyDraft,
}

impl JobStatus {
    pub fn is_success(&self) -> bool {
        // TODO: should EmptyDraft also be considered successful?
        match self {
            JobStatus::Success { .. } => true,
            _ => false,
        }
    }
    fn is_empty_draft(&self) -> bool {
        matches!(self, JobStatus::EmptyDraft)
    }

    fn success(materialization_pubs: impl Into<Vec<Id>>) -> JobStatus {
        JobStatus::Success {
            linked_materialization_publications: materialization_pubs.into(),
        }
    }
    fn build_failed(incompatible_collections: Vec<IncompatibleCollection>) -> JobStatus {
        JobStatus::BuildFailed {
            incompatible_collections,
            evolution_id: None,
        }
    }
}

/// A PublishHandler is a Handler which publishes catalog specifications.
pub struct PublishHandler {
    agent_user_email: String,
    allow_local: bool,
    bindir: String,
    broker_address: url::Url,
    builds_root: url::Url,
    connector_network: String,
    consumer_address: url::Url,
    control_plane: ControlPlane,
    logs_tx: logs::Tx,
}

impl PublishHandler {
    pub fn new(
        agent_user_email: impl Into<String>,
        allow_local: bool,
        bindir: &str,
        broker_address: &url::Url,
        builds_root: &url::Url,
        connector_network: &str,
        consumer_address: &url::Url,
        logs_tx: &logs::Tx,
        pool: Option<&sqlx::PgPool>,
    ) -> Self {
        Self {
            agent_user_email: agent_user_email.into(),
            allow_local,
            bindir: bindir.to_string(),
            broker_address: broker_address.clone(),
            builds_root: builds_root.clone(),
            connector_network: connector_network.to_string(),
            consumer_address: consumer_address.clone(),
            control_plane: ControlPlane::new(pool),
            logs_tx: logs_tx.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Handler for PublishHandler {
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

            let time_queued = chrono::Utc::now().signed_duration_since(row.updated_at);
            let process_result = self.process(row, &mut txn, false).await;

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
            info!(%id, %time_queued, %background, status = ?result.publication_status, "build finished");
            agent_sql::publications::resolve(id, &result.publication_status, &mut txn).await?;

            crate::controllers::observe_publication(&mut txn, &result)
                .await
                .context("controllers::observe_publication")?;

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

impl PublishHandler {
    #[tracing::instrument(err, skip_all, fields(id=?row.pub_id))]
    async fn process(
        &mut self,
        row: Row,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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

        // Remove draft errors from a previous publication attempt.
        agent_sql::drafts::delete_errors(row.draft_id, txn)
            .await
            .context("clearing old errors")?;

        // Create a savepoint "noop" we can roll back to.
        agent_sql::publications::savepoint_noop(txn)
            .await
            .context("creating savepoint")?;

        let spec_rows =
            specs::resolve_specifications(row.draft_id, row.pub_id, row.user_id, txn).await?;
        tracing::debug!(specs = %spec_rows.len(), "resolved specifications");

        let (hack_draft_catalog, mut hack_live_catalog) = specs::to_catalog(&spec_rows)?;
        if !hack_draft_catalog.errors.is_empty() {
            let errors = hack_draft_catalog
                .errors
                .iter()
                .map(Error::from_tables_error)
                .collect();
            return stop_with_errors(
                hack_draft_catalog,
                hack_live_catalog,
                Default::default(),
                errors,
                JobStatus::build_failed(Vec::new()),
                row,
                txn,
            )
            .await;
        }

        // Keep track of which collections are being deleted so that we can account for them
        // while resolving "remote" collection specs during the build.
        let deleted_collections: HashSet<String> = spec_rows
            .iter()
            .filter_map(|r| match r.live_type {
                Some(CatalogType::Collection) if r.draft_type.is_none() => {
                    Some(r.catalog_name.clone())
                }
                _ => None,
            })
            .collect();

        let mut draft_catalog = models::Catalog::default();
        let mut live_catalog = models::Catalog::default();

        let errors = draft::extend_catalog(
            &mut live_catalog,
            spec_rows.iter().filter_map(|r| {
                r.live_type.map(|t| {
                    (
                        t,
                        r.catalog_name.as_str(),
                        r.live_spec.as_ref().unwrap().0.as_ref(),
                    )
                })
            }),
        );
        if !errors.is_empty() {
            anyhow::bail!("unexpected errors from live specs: {errors:?}");
        }

        let errors = draft::extend_catalog(
            &mut draft_catalog,
            spec_rows.iter().filter_map(|r| {
                r.draft_type.map(|t| {
                    (
                        t,
                        r.catalog_name.as_str(),
                        r.draft_spec.as_ref().unwrap().0.as_ref(),
                    )
                })
            }),
        );
        if !errors.is_empty() {
            return stop_with_errors(
                hack_draft_catalog,
                hack_live_catalog,
                Default::default(),
                errors,
                JobStatus::build_failed(Vec::new()),
                row,
                txn,
            )
            .await;
        }

        if let Err((errors, incompatible_collections)) =
            specs::validate_transition(&draft_catalog, &live_catalog, row.pub_id, &spec_rows)
        {
            return stop_with_errors(
                hack_draft_catalog,
                hack_live_catalog,
                Default::default(),
                errors,
                JobStatus::build_failed(incompatible_collections),
                row,
                txn,
            )
            .await;
        }

        let live_spec_ids: Vec<_> = spec_rows.iter().map(|row| row.live_spec_id).collect();
        let prev_quota_usage =
            agent_sql::publications::find_tenant_quotas(live_spec_ids.clone(), txn).await?;

        for spec_row in &spec_rows {
            specs::apply_updates_for_row(
                &draft_catalog,
                row.detail.as_ref(),
                row.pub_id,
                spec_row,
                row.user_id,
                &mut *txn,
            )
            .await
            .with_context(|| format!("applying spec updates for {}", spec_row.catalog_name))?;
        }

        let pruned_collections =
            agent_sql::publications::prune_unbound_collections(row.pub_id, txn).await?;
        if !pruned_collections.is_empty() {
            tracing::info!(?pruned_collections, "pruned unbound collections");
        }
        let pruned_collections = pruned_collections.into_iter().collect::<HashSet<_>>();

        if spec_rows.len() - pruned_collections.len() == 0 {
            return stop_with_errors(
                hack_draft_catalog,
                hack_live_catalog,
                Default::default(),
                Vec::new(),
                JobStatus::EmptyDraft,
                row,
                txn,
            )
            .await;
        }

        let errors = specs::enforce_resource_quotas(&spec_rows, prev_quota_usage, txn).await?;
        if !errors.is_empty() {
            return stop_with_errors(
                hack_draft_catalog,
                hack_live_catalog,
                Default::default(),
                errors,
                JobStatus::build_failed(Vec::new()),
                row,
                txn,
            )
            .await;
        }

        let unknown_connectors =
            agent_sql::connector_tags::resolve_unknown_connectors(live_spec_ids, txn).await?;

        let errors: Vec<Error> = unknown_connectors
            .into_iter()
            .map(
                |UnknownConnector {
                     catalog_name,
                     image_name,
                 }| Error {
                    catalog_name,
                    detail: format!("Forbidden connector image '{}'", image_name),
                    ..Default::default()
                },
            )
            .collect();

        if !errors.is_empty() {
            return stop_with_errors(
                hack_draft_catalog,
                hack_live_catalog,
                Default::default(),
                errors,
                JobStatus::build_failed(Vec::new()),
                row,
                txn,
            )
            .await;
        }

        let expanded_rows = specs::expanded_specifications(row.user_id, &spec_rows, txn).await?;
        tracing::debug!(specs = %expanded_rows.len(), "resolved expanded specifications");
        specs::add_expanded_specs(&mut hack_live_catalog, &expanded_rows)?;

        // Touch all expanded specifications to update their build ID.
        // TODO(johnny): This can potentially deadlock. We may eventually want
        // to catch this condition and gracefully roll-back the transaction to
        // allow it to be re-attempted. BUT I'm avoiding this extra code path
        // (and the potential for new bugs) until we actually see this in practice.
        // Current behavior is that the agent will crash and restart, and the
        // publication will then go on to retry as desired.
        agent_sql::publications::update_expanded_live_specs(
            &expanded_rows
                .iter()
                .map(|r| r.live_spec_id)
                .collect::<Vec<_>>(),
            row.pub_id,
            &mut *txn,
        )
        .await
        .context("updating build_id of expanded specifications")?;

        let errors = draft::extend_catalog(
            &mut draft_catalog,
            expanded_rows
                .iter()
                .map(|r| (r.live_type, r.catalog_name.as_str(), r.live_spec.0.as_ref())),
        );
        if !errors.is_empty() {
            anyhow::bail!("unexpected errors from expanded specs: {errors:?}");
        }

        let errors = storage::inject_mappings(
            spec_rows
                .iter()
                .map(|r| r.catalog_name.as_ref())
                .chain(expanded_rows.iter().map(|r| r.catalog_name.as_ref())),
            &mut draft_catalog,
            txn,
        )
        .await?;
        if !errors.is_empty() {
            return stop_with_errors(
                hack_draft_catalog,
                hack_live_catalog,
                Default::default(),
                errors,
                JobStatus::build_failed(Vec::new()),
                row,
                txn,
            )
            .await;
        }

        let errors =
            linked_materializations::validate_source_captures(txn, &draft_catalog, &spec_rows)
                .await?;
        if !errors.is_empty() {
            return stop_with_errors(
                hack_draft_catalog,
                hack_live_catalog,
                Default::default(),
                errors,
                JobStatus::build_failed(Vec::new()),
                row,
                txn,
            )
            .await;
        }

        if test_run {
            return Ok(PublicationResult {
                completed_at: Utc::now(),
                publication_id: row.pub_id.into(),
                draft: hack_draft_catalog,
                live: hack_live_catalog,
                validated: Default::default(),
                publication_status: JobStatus::success(Vec::new()),
            });
        }

        let tmpdir_handle = tempfile::TempDir::new().context("creating tempdir")?;
        let tmpdir = tmpdir_handle.path();

        let build_output = builds::build_catalog(
            self.allow_local,
            &self.builds_root,
            &draft_catalog,
            &self.connector_network,
            self.control_plane
                .with_deleted_collections(deleted_collections),
            row.logs_token,
            &self.logs_tx,
            row.pub_id,
            tmpdir,
        )
        .await?;

        let errors = builds::draft_errors(&build_output);
        if !errors.is_empty() {
            // If there's a build error, then it's possible that it's due to incompatible collection changes.
            // We'll report those in the status so that the UI can present a dialog allowing users to take action.
            let incompatible_collections = builds::get_incompatible_collections(&build_output);
            return stop_with_errors(
                hack_draft_catalog,
                hack_live_catalog,
                build_output.into_parts().1,
                errors,
                JobStatus::build_failed(incompatible_collections),
                row,
                txn,
            )
            .await;
        }

        if draft_catalog.tests.len() > 0 {
            let data_plane_job = builds::data_plane(
                &self.connector_network,
                &self.bindir,
                row.logs_token,
                &self.logs_tx,
                tmpdir,
            );
            let test_jobs = builds::test_catalog(
                &self.connector_network,
                &self.bindir,
                row.logs_token,
                &self.logs_tx,
                row.pub_id,
                tmpdir,
            );

            // Drive the data-plane and test jobs, until test jobs complete.
            tokio::pin!(test_jobs);
            let errors: Vec<Error> = tokio::select! {
                r = data_plane_job => {
                    tracing::error!(?r, "test data-plane exited unexpectedly");
                    test_jobs.await // Wait for test jobs to finish.
                }
                r = &mut test_jobs => r,
            }?;

            if !errors.is_empty() {
                return stop_with_errors(
                    hack_draft_catalog,
                    hack_live_catalog,
                    build_output.into_parts().1,
                    errors,
                    JobStatus::TestFailed,
                    row,
                    txn,
                )
                .await;
            }
        }

        if row.dry_run {
            agent_sql::publications::rollback_noop(txn)
                .await
                .context("rolling back to savepoint")?;

            // Add built specs to the draft for dry runs after rolling back other changes that do
            // not apply to dry runs.
            specs::add_built_specs_to_draft_specs(&spec_rows, &build_output, txn)
                .await
                .context("adding built specs to draft")?;

            return Ok(PublicationResult {
                completed_at: Utc::now(),
                publication_id: row.pub_id.into(),
                draft: hack_draft_catalog,
                live: hack_live_catalog,
                validated: build_output.into_parts().1,
                publication_status: JobStatus::success(Vec::new()),
            });
        }

        // Add built specs to the live spec when publishing a build.
        specs::add_build_output_to_live_specs(&spec_rows, &pruned_collections, &build_output, txn)
            .await
            .context("adding built specs to live specs")?;

        let errors = builds::deploy_build(
            &self.bindir,
            &self.broker_address,
            &self.connector_network,
            &self.consumer_address,
            &expanded_rows,
            row.logs_token,
            &self.logs_tx,
            row.pub_id,
            &spec_rows,
            &pruned_collections,
        )
        .await
        .context("deploying build")?;
        // ensure that this tempdir doesn't get dropped before `deploy_build` is called, which depends on the files being there.
        std::mem::drop(tmpdir_handle);

        if !errors.is_empty() {
            return stop_with_errors(
                hack_draft_catalog,
                hack_live_catalog,
                build_output.into_parts().1,
                errors,
                JobStatus::PublishFailed,
                row,
                txn,
            )
            .await;
        }

        let maybe_source_captures =
            linked_materializations::collect_source_capture_names(&spec_rows, &draft_catalog);

        // The final step of publishing is to create additional publications for any
        // materializations which happen to have a `sourceCapture` matching any of the
        // captures we may have just published. It's important that this be done last
        // so that this function can observe the `live_specs` that we just updated.
        let pub_ids = linked_materializations::create_linked_materialization_publications(
            &self.agent_user_email,
            build_output.built_captures(),
            maybe_source_captures,
            txn,
        )
        .await
        .context("creating linked materialization publications")?;

        Ok(PublicationResult {
            completed_at: Utc::now(),
            publication_id: row.pub_id.into(),
            draft: hack_draft_catalog,
            live: hack_live_catalog,
            validated: build_output.into_parts().1,
            publication_status: JobStatus::success(pub_ids),
        })
    }
}

async fn stop_with_errors(
    draft: tables::DraftCatalog,
    live: tables::LiveCatalog,
    validated: tables::Validations,
    errors: Vec<Error>,
    mut job_status: JobStatus,
    row: Row,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<PublicationResult> {
    agent_sql::publications::rollback_noop(txn)
        .await
        .context("rolling back to savepoint")?;

    draft::insert_errors(row.draft_id, errors, txn).await?;

    // If this is a result of a build failure, then we may need to create an evolutions job in response.
    if let JobStatus::BuildFailed {
        incompatible_collections,
        evolution_id,
    } = &mut job_status
    {
        if !incompatible_collections.is_empty() && row.auto_evolve {
            let collections = create_evolutions_requests(&incompatible_collections);
            let detail = format!(
                "system created in response to failed publication: {}",
                row.pub_id
            );
            let next_job = agent_sql::evolutions::create(
                txn,
                row.user_id,
                row.draft_id,
                collections,
                true, // auto_publish
                detail,
                row.background,
            )
            .await
            .context("creating evolutions job")?;
            *evolution_id = Some(next_job);
        }
    }

    Ok(PublicationResult {
        draft,
        live,
        completed_at: chrono::Utc::now(),
        publication_id: row.pub_id.into(),
        validated,
        publication_status: job_status,
    })
}

fn create_evolutions_requests(
    incompatible_collections: &[IncompatibleCollection],
) -> Vec<serde_json::Value> {
    incompatible_collections
        .iter()
        .map(|ic| {
            // Do we need to re-create the whole collection, or can we just re-create materialization bindings?
            let (new_name, materializations) = if ic.requires_recreation.is_empty() {
                // Since we're not re-creating the collection, restrict the
                // evolution to only those materializations that have actually
                // failed validation.
                (None, ic.affected_materializations.iter().map(|m| m.name.clone()).collect())
            } else {
                tracing::debug!(reasons = ?ic.requires_recreation, collection = %ic.collection, "will attempt to re-create collection");
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

#[cfg(test)]
mod test {
    use super::{
        builds::{AffectedConsumer, ReCreateReason},
        *,
    };
    use crate::evolution::EvolveRequest;

    #[test]
    fn test_create_evolutions_requests() {
        let input = &[
            IncompatibleCollection {
                collection: "test/collectionA".to_string(),
                requires_recreation: vec![ReCreateReason::KeyChange],
                affected_materializations: vec![AffectedConsumer {
                    name: "test/materializationA".to_string(),
                    fields: Vec::new(),
                }],
            },
            IncompatibleCollection {
                collection: "test/collectionB".to_string(),
                requires_recreation: Vec::new(),
                affected_materializations: vec![
                    AffectedConsumer {
                        name: "test/materializationB".to_string(),
                        fields: Vec::new(),
                    },
                    AffectedConsumer {
                        name: "test/materializationB2".to_string(),
                        fields: Vec::new(),
                    },
                ],
            },
            IncompatibleCollection {
                collection: "test/collectionC".to_string(),
                requires_recreation: Vec::new(),
                affected_materializations: Vec::new(),
            },
        ];

        let result = serde_json::to_string(&create_evolutions_requests(input)).unwrap();
        let requests: Vec<EvolveRequest> = serde_json::from_str(&result).unwrap();

        let expected = vec![
            EvolveRequest {
                current_name: "test/collectionA".to_string(),
                new_name: Some("test/collectionA_v2".to_string()),
                materializations: Vec::new(),
            },
            EvolveRequest {
                current_name: "test/collectionB".to_string(),
                new_name: None,
                materializations: vec![
                    "test/materializationB".to_string(),
                    "test/materializationB2".to_string(),
                ],
            },
            EvolveRequest {
                current_name: "test/collectionC".to_string(),
                new_name: None,
                materializations: Vec::new(),
            },
        ];
        assert_eq!(expected, requests);
    }
}
