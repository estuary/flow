use std::collections::BTreeMap;

use self::builds::IncompatibleCollection;
use super::{draft, logs, Id};
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;

pub mod builds;
mod handler;
// TODO: port linked_materializations over to a controller
//mod linked_materializations;
mod quotas;
pub mod specs;

#[cfg(test)]
mod tests;

/// Represents a publication that has just completed.
#[derive(Debug)]
pub struct PublicationResult {
    pub publication_id: models::Id,
    pub user_id: Uuid,
    pub detail: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub draft: tables::DraftCatalog,
    /// The state of any related live_specs, prior to the draft being published
    pub live: tables::LiveCatalog,
    /// The build specifications that were output from a successful publication.
    /// Will be empty if the publication was not successful.
    pub built: tables::Validations,
    /// The final status of the publication. Note that this is not neccessarily `Success`,
    /// even if there are no `errors`.
    pub publication_status: JobStatus,
}

impl PublicationResult {
    pub fn new(
        publication_id: models::Id,
        user_id: Uuid,
        detail: Option<String>,
        start_time: DateTime<Utc>,
        built: build::Output,
        status: JobStatus,
    ) -> Self {
        Self {
            publication_id,
            user_id,
            detail,
            started_at: start_time,
            completed_at: Utc::now(),
            draft: built.draft,
            live: built.live,
            built: built.built,
            publication_status: status,
        }
    }

    pub fn draft_errors(&self) -> Vec<draft::Error> {
        self.draft
            .errors
            .iter()
            .map(draft::Error::from_tables_error)
            .chain(self.live.errors.iter().map(draft::Error::from_tables_error))
            .chain(
                self.built
                    .errors
                    .iter()
                    .map(draft::Error::from_tables_error),
            )
            .collect()
    }
}

/// Represents an optimistic lock failure when trying to update live specs.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LockFailure {
    pub catalog_name: String,
    pub expect_pub_id: models::Id,
    pub last_pub_id: Option<models::Id>,
}

// TODO: consider having JobStatus indicate whether we've committed to the database, but failed to apply shard/journal specs
/// JobStatus is the possible outcomes of a handled draft submission.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    BuildFailed {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        incompatible_collections: Vec<IncompatibleCollection>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        evolution_id: Option<Id>,
    },
    TestFailed,
    PublishFailed,
    Success {
        /// If any materializations are to be updated in response to this publication,
        /// their publication ids will be included here. This is purely informational.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        linked_materialization_publications: Vec<Id>,
    },
    /// Returned when there are no draft specs (after pruning unbound
    /// collections). There will not be any `draft_errors` in this case, because
    /// there's no `catalog_name` to associate with an error. And it may not be
    /// desirable to treat this as an error, depending on the scenario.
    EmptyDraft,
    /// One or more expected `last_pub_id`s did not match the actual `last_pub_id`, indicating that specs
    /// have been changed since the draft was created.
    ExpectPubIdMismatch {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        failures: Vec<LockFailure>,
    },
}

impl JobStatus {
    pub fn is_success(&self) -> bool {
        // TODO: should EmptyDraft also be considered successful?
        match self {
            JobStatus::Success { .. } => true,
            _ => false,
        }
    }

    pub fn has_incompatible_collections(&self) -> bool {
        matches!(self, JobStatus::BuildFailed { incompatible_collections, .. } if !incompatible_collections.is_empty())
    }

    fn is_empty_draft(&self) -> bool {
        matches!(self, JobStatus::EmptyDraft)
    }

    fn success(materialization_pubs: impl Into<Vec<Id>>) -> JobStatus {
        JobStatus::Success {
            linked_materialization_publications: materialization_pubs.into(),
        }
    }
    pub fn build_failed(incompatible_collections: Vec<IncompatibleCollection>) -> JobStatus {
        JobStatus::BuildFailed {
            incompatible_collections,
            evolution_id: None,
        }
    }
}

/// A PublishHandler is a Handler which publishes catalog specifications.
#[derive(Debug, Clone)]
pub struct Publisher {
    agent_user_email: String,
    test_mode: bool,
    allow_local: bool,
    bindir: String,
    broker_address: url::Url,
    builds_root: url::Url,
    connector_network: String,
    consumer_address: url::Url,
    logs_tx: logs::Tx,
    build_id_gen: models::IdGenerator,
    db: sqlx::PgPool,
}

impl Publisher {
    pub fn new(
        agent_user_email: impl Into<String>,
        allow_local: bool,
        test_mode: bool,
        bindir: &str,
        broker_address: &url::Url,
        builds_root: &url::Url,
        connector_network: &str,
        consumer_address: &url::Url,
        logs_tx: &logs::Tx,
        pool: sqlx::PgPool,
        build_id_gen: models::IdGenerator,
    ) -> Self {
        Self {
            agent_user_email: agent_user_email.into(),
            allow_local,
            test_mode,
            bindir: bindir.to_string(),
            broker_address: broker_address.clone(),
            builds_root: builds_root.clone(),
            connector_network: connector_network.to_string(),
            consumer_address: consumer_address.clone(),
            logs_tx: logs_tx.clone(),
            build_id_gen,
            db: pool,
        }
    }
}

pub struct UncommittedBuild {
    publication_id: models::Id,
    build_id: models::Id,
    user_id: Uuid,
    detail: Option<String>,
    started_at: DateTime<Utc>,
    output: build::Output,
    live_spec_ids: BTreeMap<String, models::Id>,
}
impl UncommittedBuild {
    pub fn start_time(&self) -> DateTime<Utc> {
        self.started_at
    }

    pub fn has_errors(&self) -> bool {
        self.errors().next().is_some()
    }

    pub fn errors(&self) -> impl Iterator<Item = &tables::Error> {
        self.output.errors()
    }

    // TODO: can we compute the JobStatus inside into_result?
    pub fn into_result(self, completed_at: DateTime<Utc>, status: JobStatus) -> PublicationResult {
        let UncommittedBuild {
            publication_id,
            user_id,
            detail,
            started_at,
            output,
            live_spec_ids: _,
            // TODO: propagate build_id onto publication result
            build_id: _,
        } = self;
        let build::Output { draft, live, built } = output;
        PublicationResult {
            user_id,
            detail,
            publication_id,
            started_at,
            completed_at,
            draft,
            live,
            built,
            publication_status: status,
        }
    }
}

impl Into<build::Output> for UncommittedBuild {
    fn into(self) -> build::Output {
        self.output
    }
}

impl Publisher {
    #[tracing::instrument(level = "info", skip(self, draft))]
    pub async fn build(
        &mut self,
        user_id: Uuid,
        publication_id: models::Id,
        detail: Option<String>,
        draft: tables::DraftCatalog,
        logs_token: sqlx::types::Uuid,
    ) -> anyhow::Result<UncommittedBuild> {
        let start_time = Utc::now();
        let build_id = self.build_id_gen.next();

        // Ensure that all the connector images are allowed. It's critical that we do this before
        // calling `build_catalog` in order to prevent the user from running arbitrary images
        // during the build process. Note that this check will need replaced with a more general
        // authorization check once we start supporting user-provided images.
        let forbidden_images = specs::check_connector_images(&draft, &self.db)
            .await
            .context("checking connector images")?;
        if !forbidden_images.is_empty() {
            let mut built = tables::Validations::default();
            built.errors = forbidden_images;
            let output = build::Output {
                draft,
                built,
                live: Default::default(),
            };
            return Ok(UncommittedBuild {
                publication_id,
                build_id,
                user_id,
                detail,
                started_at: start_time,
                output,
                live_spec_ids: BTreeMap::new(),
            });
        }

        let (live_catalog, live_spec_ids) =
            specs::resolve_live_specs(user_id, &draft, &self.db).await?;
        if !live_catalog.errors.is_empty() {
            for error in live_catalog.errors.iter() {
                tracing::error!(?error, "error resolving live specs");
            }
            anyhow::bail!("resolved LiveCatalog contained errors");
        }

        let inferred_schemas = live_catalog
            .inferred_schemas
            .iter()
            .map(|s| s.collection_name.as_str())
            .collect::<Vec<_>>();
        let live_spec_names = live_catalog.all_spec_names().collect::<Vec<_>>();
        let draft_spec_names = draft.all_spec_names().collect::<Vec<_>>();
        tracing::info!(
            ?inferred_schemas,
            ?live_spec_names,
            ?draft_spec_names,
            "resolved publication specs"
        );

        let tmpdir_handle = tempfile::TempDir::new().context("creating tempdir")?;
        let tmpdir = tmpdir_handle.path();
        //let log_handler = logs::ops_handler(self.logs_tx.clone(), "build".to_string(), logs_token);
        let mut built = builds::build_catalog(
            self.test_mode,
            self.allow_local,
            &self.builds_root,
            draft,
            live_catalog,
            self.connector_network.clone(),
            publication_id,
            build_id,
            tmpdir,
            self.logs_tx.clone(),
            logs_token,
        )
        .await?;

        // TODO: prune unbound collections from the draft catalog. This needs to be done in rust rather than in
        // sql. It's still required because discovers need to create drafts that include the collections for
        // disabled bindings, in case a user enables a binding in the UI.
        // let pruned_collections =
        //     agent_sql::publications::prune_unbound_collections(row.pub_id, txn).await?;
        // if !pruned_collections.is_empty() {
        //     tracing::info!(?pruned_collections, "pruned unbound collections");
        // }
        // let pruned_collections = pruned_collections.into_iter().collect::<HashSet<_>>();
        // if spec_rows.len() - pruned_collections.len() == 0 {
        //     return stop_with_errors(
        //         hack_draft_catalog,
        //         hack_live_catalog,
        //         Default::default(),
        //         Vec::new(),
        //         JobStatus::EmptyDraft,
        //         row,
        //         txn,
        //     )
        //     .await;
        // }

        if built.live.tests.len() > 0 && !self.test_mode {
            let data_plane_job = builds::data_plane(
                &self.connector_network,
                &self.bindir,
                logs_token,
                &self.logs_tx,
                tmpdir,
            );
            let test_jobs = builds::test_catalog(
                &self.connector_network,
                &self.bindir,
                logs_token,
                &self.logs_tx,
                build_id,
                tmpdir,
            );

            // Drive the data-plane and test jobs, until test jobs complete.
            tokio::pin!(test_jobs);
            let errors: Vec<tables::Error> = tokio::select! {
                r = data_plane_job => {
                    tracing::error!(?r, "test data-plane exited unexpectedly");
                    test_jobs.await // Wait for test jobs to finish.
                }
                r = &mut test_jobs => r,
            }?;

            if !errors.is_empty() {
                built.built.errors.extend(errors.into_iter());
            }
        }

        Ok(UncommittedBuild {
            publication_id,
            build_id,
            user_id,
            detail,
            started_at: start_time,
            output: built,
            live_spec_ids,
        })
    }

    pub async fn commit(
        &self,
        logs_token: Uuid,
        mut uncommitted: UncommittedBuild,
        pool: &sqlx::PgPool,
    ) -> anyhow::Result<PublicationResult> {
        anyhow::ensure!(
            !uncommitted.has_errors(),
            "cannot commit uncommitted build that has errors"
        );
        let mut txn = pool.begin().await?;

        let failures = specs::persist_updates(&mut uncommitted, &mut txn).await?;
        let completed_at = Utc::now();
        if !failures.is_empty() {
            return Ok(
                uncommitted.into_result(completed_at, JobStatus::ExpectPubIdMismatch { failures })
            );
        }

        let quota_errors =
            self::quotas::check_resource_quotas(&uncommitted.output, &mut txn).await?;
        if !quota_errors.is_empty() {
            uncommitted
                .output
                .built
                .errors
                .extend(quota_errors.into_iter());
            return Ok(uncommitted.into_result(completed_at, JobStatus::PublishFailed));
        }

        txn.commit()
            .await
            .context("committing publication transaction")?;

        if self.test_mode {
            eprintln!("skipping deploy for test");
            return Ok(uncommitted.into_result(completed_at, JobStatus::success(Vec::new())));
        }
        let deploy_errors = builds::deploy_build(
            &self.bindir,
            &self.broker_address,
            &self.connector_network,
            &self.consumer_address,
            logs_token,
            &self.logs_tx,
            uncommitted.build_id,
            &uncommitted.output,
        )
        .await
        .context("deploying build")?;
        if !deploy_errors.is_empty() {
            uncommitted
                .output
                .built
                .errors
                .extend(deploy_errors.into_iter());
            return Ok(uncommitted.into_result(completed_at, JobStatus::PublishFailed));
        }
        Ok(uncommitted.into_result(completed_at, JobStatus::success(Vec::new())))
    }
}
