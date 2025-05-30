use std::collections::BTreeMap;

use crate::proxy_connectors::MakeConnectors;

use super::logs;
use anyhow::Context;
use chrono::{DateTime, Utc};
use sqlx::types::Uuid;
use tables::LiveRow;

pub mod builds;
mod commit;
mod executor;
mod finalize;
mod incompatible_collections;
mod initialize;
mod retry;

mod quotas;
pub mod specs;

pub use self::commit::{ClearDraftErrors, NoopWithCommit, UpdatePublicationsRow, WithCommit};
pub use self::finalize::{FinalizeBuild, NoopFinalize, PruneUnboundCollections};
pub use self::initialize::{ExpandDraft, Initialize, NoopInitialize};
pub use self::retry::{DefaultRetryPolicy, DoNotRetry, RetryPolicy};
pub use models::publications::{
    AffectedConsumer, IncompatibleCollection, JobStatus, LockFailure, ReCreateReason, RejectedField,
};

use models::draft_error;

/// Represents a desire to publish the given `draft`, along with associated metadata and behavior
/// for handling draft initialization, build finalizing, and retrying failures.
pub struct DraftPublication<Init: Initialize, Fin: FinalizeBuild, Ret: RetryPolicy, C: WithCommit> {
    /// The id of the user that is publishing the draft.
    pub user_id: Uuid,
    /// Write logs to `internal.log_lines` using this token.
    pub logs_token: Uuid,
    /// Whether to stop after building. If `dry_run` is `true`, then the build will not be
    /// committed, even if it is successful. Validations will be run as normal.
    pub dry_run: bool,
    /// The draft catalog to publish. Note that only the `collections`, `captures`,
    /// `materializations`, and `tests` will be used. Other fields on the draft will be ignored.
    pub draft: tables::DraftCatalog,
    /// Detail message to associate with this publication.
    pub detail: Option<String>,
    /// Whether to check user permissions when publishing specs. If this is false, then all
    /// permission checks will be skipped, and the publication may modify any specs.
    pub verify_user_authz: bool,
    /// Default data plane to use for publishing new specs. This is optional only when the
    /// publication _only_ updates and/or deletes existing live specs.
    pub default_data_plane_name: Option<String>,
    /// Initializes the associated `draft`. This will be passed a mutable copy of the `draft` prior
    /// to build/validation of each attempt.
    pub initialize: Init,
    /// Finalizes the result of a build, potentially modifying the result. The `UncommittedBuild`
    /// is passed to this function, regardless of whether it was successful. If the build contains
    /// any errors after this function returns, then it will be considered failed.
    pub finalize: Fin,
    /// Determines whether a failed publication should be retried. The retry policy is consulted
    /// regardless of whether errors originate from the build or commit phase, but it is _not_
    /// consulted if any step returns an `Result::Err`, which is always considered terminal.
    pub retry: Ret,
    /// Callback to run before committing a successful publication. This is useful for updating
    /// other tables as part of the same database transaction.
    pub with_commit: C,
}

/// Represents a publication that has just completed.
#[derive(Debug)]
pub struct PublicationResult {
    pub pub_id: models::Id,
    pub user_id: Uuid,
    pub detail: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub draft: tables::DraftCatalog,
    /// The state of any related live_specs, prior to the draft being published
    pub live: tables::LiveCatalog,
    /// The build specifications that were output from a successful publication.
    /// May be empty if the build failed prior to validation. Contains any validation
    /// errors.
    pub built: tables::Validations,
    /// Errors that occurred while running tests.
    pub test_errors: tables::Errors,
    /// The final status of the publication. Note that this is not neccessarily `Success`,
    /// even if there are no `errors`.
    pub status: JobStatus,
    /// The number of retries that have been attempted on this publiclication. This will be 0 for
    /// the initial attempt, and increment by 1 on each subsequent retry.
    pub retry_count: u32,
}

impl PublicationResult {
    pub fn new(
        pub_id: models::Id,
        user_id: Uuid,
        detail: Option<String>,
        start_time: DateTime<Utc>,
        built: build::Output,
        test_errors: tables::Errors,
        status: JobStatus,
        retry_count: u32,
    ) -> Self {
        Self {
            pub_id,
            user_id,
            detail,
            started_at: start_time,
            completed_at: Utc::now(),
            draft: built.draft,
            live: built.live,
            built: built.built,
            test_errors,
            status,
            retry_count,
        }
    }

    pub fn error_for_status(self) -> Result<PublicationResult, anyhow::Error> {
        // TODO(phil): consider returning Ok if status is EmptyDraft?
        if self.status.is_success() {
            Ok(self)
        } else {
            anyhow::bail!("publication failed with status: {:?}", self.status)
        }
    }

    pub fn draft_errors(&self) -> Vec<draft_error::Error> {
        self.draft
            .errors
            .iter()
            .map(tables::Error::to_draft_error)
            .chain(self.live.errors.iter().map(tables::Error::to_draft_error))
            .chain(self.built.errors.iter().map(tables::Error::to_draft_error))
            .chain(self.test_errors.iter().map(tables::Error::to_draft_error))
            .collect()
    }
}

/// A PublishHandler is a Handler which publishes catalog specifications.
#[derive(Debug, Clone)]
pub struct Publisher<MC: MakeConnectors> {
    bindir: String,
    builds_root: url::Url,
    connector_network: String,
    logs_tx: logs::Tx,
    id_gen: std::sync::Arc<std::sync::Mutex<models::IdGenerator>>,
    db: sqlx::PgPool,
    make_connectors: MC,
}

pub struct UncommittedBuild {
    pub(crate) publication_id: models::Id,
    pub(crate) build_id: models::Id,
    pub(crate) user_id: Uuid,
    pub(crate) detail: Option<String>,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) output: build::Output,
    pub(crate) test_errors: tables::Errors,
    pub(crate) incompatible_collections: Vec<IncompatibleCollection>,
    pub(crate) retry_count: u32,
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

    pub fn build_failed(mut self) -> PublicationResult {
        let status = if self.test_errors.is_empty() {
            // get_incompatible_collections returns those that were rejected by materializations,
            // whereas the ones in `incompatible_collections` were rejected due to key or logical
            // parititon changes.
            let mut naughty_collections =
                incompatible_collections::get_incompatible_collections(&self.output.built);
            naughty_collections.extend(self.incompatible_collections.drain(..));
            JobStatus::build_failed(naughty_collections)
        } else {
            JobStatus::TestFailed
        };
        self.into_result(Utc::now(), status)
    }

    pub fn into_result(self, completed_at: DateTime<Utc>, status: JobStatus) -> PublicationResult {
        let UncommittedBuild {
            publication_id,
            user_id,
            detail,
            started_at,
            output,
            test_errors,
            incompatible_collections,
            build_id: _,
            retry_count,
        } = self;
        debug_assert!(
            incompatible_collections.is_empty(),
            "incompatible_collections should always be empty when calling into_result"
        );
        let build::Output { draft, live, built } = output;
        PublicationResult {
            user_id,
            detail,
            pub_id: publication_id,
            started_at,
            completed_at,
            draft,
            live,
            built,
            test_errors,
            status,
            retry_count,
        }
    }
}

impl Into<build::Output> for UncommittedBuild {
    fn into(self) -> build::Output {
        self.output
    }
}

impl<MC: MakeConnectors> Publisher<MC> {
    pub fn new(
        bindir: &str,
        builds_root: &url::Url,
        connector_network: &str,
        logs_tx: &logs::Tx,
        pool: sqlx::PgPool,
        build_id_gen: models::IdGenerator,
        make_connectors: MC,
    ) -> Self {
        Self {
            bindir: bindir.to_string(),
            builds_root: builds_root.clone(),
            connector_network: connector_network.to_string(),
            logs_tx: logs_tx.clone(),
            id_gen: std::sync::Mutex::new(build_id_gen.into()).into(),
            db: pool,
            make_connectors,
        }
    }

    /// Publishs the given `DraftPublication`, using the provided `Initialize`, `FinalizeBuild`,
    /// and `RetryPolicy`.
    #[tracing::instrument(err, skip_all, fields(
        user_id = %publication.user_id,
        logs_token = %publication.logs_token,
    ))]
    pub async fn publish<Ini: Initialize, Fin: FinalizeBuild, Ret: RetryPolicy, C: WithCommit>(
        &self,
        publication: DraftPublication<Ini, Fin, Ret, C>,
    ) -> anyhow::Result<PublicationResult> {
        let mut retry_count = 0u32;
        loop {
            // Generate a new id on each attempt, so that we can retry `PublicationSuperseded`
            // errors with a greater id.
            let publication_id = self.next_id();
            let result = self
                .try_publish(publication_id, retry_count, &publication)
                .await?;

            if result.status.is_success() || result.status.is_empty_draft() {
                return Ok(result);
            }
            if !publication.retry.retry(&result) {
                return Ok(result);
            }
            retry_count += 1;
        }
    }

    #[tracing::instrument(err, skip_all, fields(%publication_id, retry_count))]
    async fn try_publish<Ini: Initialize, Fin: FinalizeBuild, Ret: RetryPolicy, C: WithCommit>(
        &self,
        publication_id: models::Id,
        retry_count: u32,
        DraftPublication {
            user_id,
            logs_token,
            dry_run,
            draft: raw_draft,
            verify_user_authz,
            detail,
            default_data_plane_name,
            initialize,
            finalize,
            retry: _,
            with_commit,
        }: &DraftPublication<Ini, Fin, Ret, C>,
    ) -> anyhow::Result<PublicationResult> {
        let mut draft = raw_draft.clone_specs();
        initialize
            .initialize(&self.db, *user_id, &mut draft)
            .await
            .context("initializing draft")?;
        // It's important that we generate the pub id inside the retry loop so that we can
        // retry `PublicationSuperseded` errors.
        let mut built = self
            .build(
                *user_id,
                publication_id,
                detail.clone(),
                draft,
                *logs_token,
                default_data_plane_name.as_deref().unwrap_or(""),
                *verify_user_authz,
                retry_count,
            )
            .await?;
        finalize.finalize(&mut built).context("finalizing build")?;

        if built.errors().next().is_some() {
            return Ok(built.build_failed());
        } else if is_empty_draft(&built) {
            return Ok(built.into_result(Utc::now(), JobStatus::EmptyDraft));
        } else if *dry_run {
            return Ok(built.into_result(Utc::now(), JobStatus::Success));
        }

        let commit_result = self.commit(built, with_commit).await?;
        Ok(commit_result)
    }

    fn next_id(&self) -> models::Id {
        let mut gen = self.id_gen.lock().unwrap();
        gen.next()
    }

    /// Build and verify the given draft. This is `pub` only because we have existing tests that
    /// use it. If you want to publish something, use the `Publisher::publish` function instead.
    #[tracing::instrument(level = "info", skip(self, draft))]
    pub(crate) async fn build(
        &self,
        user_id: Uuid,
        publication_id: models::Id,
        detail: Option<String>,
        draft: tables::DraftCatalog,
        logs_token: sqlx::types::Uuid,
        default_data_plane_name: &str,
        verify_user_authz: bool,
        retry_count: u32,
    ) -> anyhow::Result<UncommittedBuild> {
        let start_time = Utc::now();
        let build_id = self.id_gen.lock().unwrap().next();

        // Ensure that all the connector images are allowed. It's critical that we do this before
        // calling `build_catalog` in order to prevent the user from running arbitrary images
        // during the build process. Note that this check will need replaced with a more general
        // authorization check once we start supporting user-provided images.
        let forbidden_images = specs::check_connector_images(&draft, &self.db)
            .await
            .context("checking connector images")?;
        let forbidden_source_capture = specs::check_source_capture_annotations(&draft, &self.db)
            .await
            .context("checking source capture")?;
        if !forbidden_images.is_empty() || !forbidden_source_capture.is_empty() {
            let mut built = tables::Validations::default();
            built.errors = forbidden_images;
            built.errors.extend(forbidden_source_capture.into_iter());
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
                test_errors: tables::Errors::default(),
                incompatible_collections: Vec::new(),
                retry_count,
            });
        }

        let live_catalog = specs::resolve_live_specs(
            user_id,
            &draft,
            &self.db,
            default_data_plane_name,
            verify_user_authz,
        )
        .await?;
        if !live_catalog.errors.is_empty() {
            return Ok(UncommittedBuild {
                publication_id,
                build_id,
                user_id,
                detail,
                started_at: start_time,
                output: build::Output {
                    draft,
                    live: live_catalog,
                    built: Default::default(),
                },
                test_errors: tables::Errors::default(),
                incompatible_collections: Vec::new(),
                retry_count,
            });
        }

        let incompatible_collections = validate_collection_transitions(&draft, &live_catalog);
        if !incompatible_collections.is_empty() {
            let errors =  incompatible_collections.iter().map(|ic| tables::Error {
                scope: tables::synthetic_scope(models::CatalogType::Collection, &ic.collection),
                error: anyhow::anyhow!("collection key and logical partitioning may not be changed; a new collection must be created"),
            }).collect::<tables::Errors>();
            let output = build::Output {
                draft,
                live: live_catalog,
                built: tables::Validations {
                    errors,
                    ..Default::default()
                },
            };
            return Ok(UncommittedBuild {
                publication_id,
                build_id,
                user_id,
                detail,
                started_at: start_time,
                output,
                test_errors: tables::Errors::default(),
                incompatible_collections,
                retry_count,
            });
        }

        let live_spec_names = live_catalog.all_spec_names().collect::<Vec<_>>();
        let draft_spec_names = draft.all_spec_names().collect::<Vec<_>>();
        tracing::debug!(
            ?live_spec_names,
            ?draft_spec_names,
            "resolved publication specs"
        );

        let connectors = self.make_connectors.make_connectors(logs_token);

        let tmpdir_handle = tempfile::TempDir::new().context("creating tempdir")?;
        let tmpdir = tmpdir_handle.path();
        let built = builds::build_catalog(
            &self.builds_root,
            draft,
            live_catalog,
            publication_id,
            build_id,
            tmpdir,
            self.logs_tx.clone(),
            logs_token,
            &connectors,
        )
        .await?;

        // If there are any tests, run them now as long as there's no build errors
        let test_errors = if built.built.built_tests.len() > 0
            && !cfg!(test)
            && built.errors().next().is_none()
        {
            tracing::info!(%build_id, %publication_id, tmpdir = %tmpdir.display(), "running tests");
            let data_plane_job = builds::data_plane(
                &self.connector_network,
                &self.bindir,
                logs_token,
                &self.logs_tx,
                tmpdir,
            );
            let test_jobs = builds::test_catalog(
                &self.bindir,
                logs_token,
                &self.logs_tx,
                build_id,
                tmpdir,
                &built,
            );

            // Drive the data-plane and test jobs, until test jobs complete.
            tokio::pin!(test_jobs);
            let errors: tables::Errors = tokio::select! {
                r = data_plane_job => {
                    tracing::error!(?r, "test data-plane exited unexpectedly");
                    test_jobs.await // Wait for test jobs to finish.
                }
                r = &mut test_jobs => r,
            }?;

            tracing::debug!(test_count = %built.live.tests.len(), test_errors = %errors.len(), "finished running tests");
            errors
        } else {
            tables::Errors::default()
        };

        Ok(UncommittedBuild {
            publication_id,
            build_id,
            user_id,
            detail,
            started_at: start_time,
            output: built,
            test_errors,
            incompatible_collections: Vec::new(),
            retry_count,
        })
    }

    /// Commits a successful build. This function is only `pub` because some tests need it.
    /// If you need to publish something, use `Publisher::publish` instead.
    #[tracing::instrument(err, skip_all, fields(
        build_id = %uncommitted.publication_id,
    ))]
    pub(crate) async fn commit<C: WithCommit>(
        &self,
        mut uncommitted: UncommittedBuild,
        with_commit: C,
    ) -> anyhow::Result<PublicationResult> {
        anyhow::ensure!(
            !uncommitted.has_errors(),
            "cannot commit uncommitted build that has errors"
        );

        let completed_at = Utc::now();
        let mut txn = self.db.begin().await?;

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

        let failures = specs::persist_updates(&mut uncommitted, &mut txn).await?;
        if !failures.is_empty() {
            return Ok(
                uncommitted.into_result(completed_at, JobStatus::BuildIdLockFailure { failures })
            );
        }

        let pub_result = uncommitted.into_result(completed_at, JobStatus::Success);
        with_commit
            .before_commit(&mut txn, &pub_result)
            .await
            .context("on publication commit")?;

        txn.commit()
            .await
            .context("committing publication transaction")?;
        tracing::info!("successfully committed publication");
        Ok(pub_result)
    }
}

fn is_empty_draft(build: &UncommittedBuild) -> bool {
    use tables::BuiltRow;

    let built = &build.output.built;
    built.built_captures.iter().all(BuiltRow::is_passthrough)
        && built.built_collections.iter().all(BuiltRow::is_passthrough)
        && built
            .built_materializations
            .iter()
            .all(BuiltRow::is_passthrough)
        && built.built_tests.iter().all(BuiltRow::is_passthrough)
}

pub fn partitions(projections: &BTreeMap<models::Field, models::Projection>) -> Vec<String> {
    projections
        .iter()
        .filter_map(|(field, proj)| {
            if matches!(
                proj,
                models::Projection::Extended {
                    partition: true,
                    ..
                }
            ) {
                Some(field.to_string())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
}

/// Validates that collection keys have not changed. This check is now also performed
/// as part of the `validation` crate, but it's duplicated here so that we can return
/// structured `IncompatibleCollection` errors. We should probably consider moving
/// `IncompatibleCollection` into `tables`, and generating these structured errors
/// as part of `validation`. But not today.
pub fn validate_collection_transitions(
    draft: &tables::DraftCatalog,
    live: &tables::LiveCatalog,
) -> Vec<IncompatibleCollection> {
    draft
        .collections
        .inner_join(
            live.collections.iter().map(|lc| (lc.catalog_name(), lc)),
            |draft_row, _, live_row| {
                let Some(draft_model) = draft_row.model.as_ref() else {
                    return None;
                };
                // Resetting a collection allows the key and partitions to change.
                if draft_model.reset || draft_model.delete {
                    return None;
                }
                let live_model = &live_row.model;

                let mut requires_recreation = Vec::new();
                if draft_model.key != live_model.key {
                    requires_recreation.push(ReCreateReason::KeyChange);
                }
                if partitions(&draft_model.projections) != partitions(&live_model.projections) {
                    requires_recreation.push(ReCreateReason::PartitionChange);
                }
                if requires_recreation.is_empty() {
                    None
                } else {
                    Some(IncompatibleCollection {
                        collection: draft_row.collection.to_string(),
                        requires_recreation,
                        // Don't set affected_materializations because materializations
                        // are not the source of the incompatibility, and all materializations
                        // sourcing from the collection will always be affected.
                        affected_materializations: Vec::new(),
                    })
                }
            },
        )
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_errors_result_in_test_failed_status() {
        let build = UncommittedBuild {
            publication_id: models::Id::zero(),
            build_id: models::Id::zero(),
            user_id: Uuid::new_v4(),
            detail: None,
            started_at: Utc::now(),
            output: Default::default(),
            test_errors: std::iter::once(tables::Error {
                scope: tables::synthetic_scope("test", "test/of/a/test"),
                error: anyhow::anyhow!("test error"),
            })
            .collect(),
            incompatible_collections: Vec::new(),
            retry_count: 0,
        };
        let result = build.build_failed();
        assert_eq!(JobStatus::TestFailed, result.status);
    }
}
