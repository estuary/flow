use std::collections::BTreeMap;

use super::{draft, logs};
use anyhow::Context;
use chrono::{DateTime, Utc};
use sqlx::types::Uuid;
use tables::LiveRow;

pub mod builds;
mod handler;
mod prune;
mod status;

mod quotas;
pub mod specs;

pub use self::status::{
    get_incompatible_collections, AffectedConsumer, IncompatibleCollection, JobStatus, LockFailure,
    ReCreateReason, RejectedField,
};

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
            .chain(self.test_errors.iter().map(draft::Error::from_tables_error))
            .collect()
    }
}

/// A PublishHandler is a Handler which publishes catalog specifications.
#[derive(Debug, Clone)]
pub struct Publisher {
    allow_local: bool,
    bindir: String,
    builds_root: url::Url,
    connector_network: String,
    logs_tx: logs::Tx,
    build_id_gen: models::IdGenerator,
    db: sqlx::PgPool,
    max_concurrent_validations: usize,
}

impl Publisher {
    pub fn new(
        allow_local: bool,
        bindir: &str,
        builds_root: &url::Url,
        connector_network: &str,
        logs_tx: &logs::Tx,
        pool: sqlx::PgPool,
        build_id_gen: models::IdGenerator,
        max_concurrent_validations: usize,
    ) -> Self {
        Self {
            allow_local,
            bindir: bindir.to_string(),
            builds_root: builds_root.clone(),
            connector_network: connector_network.to_string(),
            logs_tx: logs_tx.clone(),
            build_id_gen,
            db: pool,
            max_concurrent_validations,
        }
    }
}

pub struct UncommittedBuild {
    pub(crate) publication_id: models::Id,
    pub(crate) user_id: Uuid,
    pub(crate) detail: Option<String>,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) output: build::Output,
    pub(crate) live_spec_ids: BTreeMap<String, models::Id>,
    pub(crate) test_errors: tables::Errors,
    pub(crate) incompatible_collections: Vec<IncompatibleCollection>,
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
            let mut naughty_collections = status::get_incompatible_collections(&self.output.built);
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
            live_spec_ids: _,
            test_errors,
            incompatible_collections,
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
        }
    }
}

impl Into<build::Output> for UncommittedBuild {
    fn into(self) -> build::Output {
        self.output
    }
}

impl Publisher {
    pub const MAX_OPTIMISTIC_LOCKING_RETRIES: u32 = 10;

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
                user_id,
                detail,
                started_at: start_time,
                output,
                live_spec_ids: BTreeMap::new(),
                test_errors: tables::Errors::default(),
                incompatible_collections: Vec::new(),
            });
        }

        let (live_catalog, live_spec_ids) =
            specs::resolve_live_specs(user_id, &draft, &self.db).await?;
        if !live_catalog.errors.is_empty() {
            return Ok(UncommittedBuild {
                publication_id,
                user_id,
                detail,
                started_at: start_time,
                output: build::Output {
                    draft,
                    live: live_catalog,
                    built: Default::default(),
                },
                live_spec_ids,
                test_errors: tables::Errors::default(),
                incompatible_collections: Vec::new(),
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
                user_id,
                detail,
                started_at: start_time,
                output,
                live_spec_ids,
                test_errors: tables::Errors::default(),
                incompatible_collections,
            });
        }

        let inferred_schemas = live_catalog
            .inferred_schemas
            .iter()
            .map(|s| s.collection_name.as_str())
            .collect::<Vec<_>>();
        let live_spec_names = live_catalog.all_spec_names().collect::<Vec<_>>();
        let draft_spec_names = draft.all_spec_names().collect::<Vec<_>>();
        tracing::debug!(
            ?inferred_schemas,
            ?live_spec_names,
            ?draft_spec_names,
            "resolved publication specs"
        );

        let tmpdir_handle = tempfile::TempDir::new().context("creating tempdir")?;
        let tmpdir = tmpdir_handle.path();
        let built = builds::build_catalog(
            self.allow_local,
            &self.builds_root,
            draft,
            live_catalog,
            self.connector_network.clone(),
            self.max_concurrent_validations,
            publication_id,
            build_id,
            tmpdir,
            self.logs_tx.clone(),
            logs_token,
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
            user_id,
            detail,
            started_at: start_time,
            output: built,
            live_spec_ids,
            test_errors,
            incompatible_collections: Vec::new(),
        })
    }

    #[tracing::instrument(err, skip_all, fields(
        publication_id = %uncommitted.publication_id,
        user_id = %uncommitted.user_id,
        detail = ?uncommitted.detail
    ))]
    pub async fn commit(
        &self,
        mut uncommitted: UncommittedBuild,
    ) -> anyhow::Result<PublicationResult> {
        anyhow::ensure!(
            !uncommitted.has_errors(),
            "cannot commit uncommitted build that has errors"
        );
        let mut txn = self.db.begin().await?;
        let completed_at = Utc::now();

        let pruned_collections = prune::prune_unbound_collections(&mut uncommitted.output.built);
        if !pruned_collections.is_empty() {
            tracing::info!(
                ?pruned_collections,
                remaining_specs = %uncommitted.output.built.spec_count(),
                "pruned unbound collections from built catalog"
            );
        }
        if is_empty_draft(&uncommitted) {
            return Ok(uncommitted.into_result(completed_at, JobStatus::EmptyDraft));
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

        let failures = specs::persist_updates(&mut uncommitted, &mut txn).await?;
        if !failures.is_empty() {
            return Ok(
                uncommitted.into_result(completed_at, JobStatus::ExpectPubIdMismatch { failures })
            );
        }

        txn.commit()
            .await
            .context("committing publication transaction")?;
        tracing::info!("successfully committed publication");
        Ok(uncommitted.into_result(completed_at, JobStatus::Success))
    }
}

fn is_empty_draft(build: &UncommittedBuild) -> bool {
    use tables::BuiltRow;

    let built = &build.output.built;
    built.built_captures.iter().all(BuiltRow::is_unchanged)
        && built.built_collections.iter().all(BuiltRow::is_unchanged)
        && built
            .built_materializations
            .iter()
            .all(BuiltRow::is_unchanged)
        && built.built_tests.iter().all(BuiltRow::is_unchanged)
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

/// Validates that collection keys have not changed. This check lives here and
/// not in `validation` because we're hesitant to commit to it, and may want to
/// allow collection keys to change in the future. So this is easy and lets us
/// continue to return the same structured errors as before.
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
            user_id: Uuid::new_v4(),
            detail: None,
            started_at: Utc::now(),
            output: Default::default(),
            live_spec_ids: Default::default(),
            test_errors: std::iter::once(tables::Error {
                scope: tables::synthetic_scope("test", "test/of/a/test"),
                error: anyhow::anyhow!("test error"),
            })
            .collect(),
            incompatible_collections: Vec::new(),
        };
        let result = build.build_failed();
        assert_eq!(JobStatus::TestFailed, result.status);
    }
}
