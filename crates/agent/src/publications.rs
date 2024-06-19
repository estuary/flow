use std::collections::BTreeMap;

use super::{draft, logs};
use anyhow::Context;
use chrono::{DateTime, Utc};
use sqlx::types::Uuid;

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
    /// Will be empty if the publication was not successful.
    pub built: tables::Validations,
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
            status,
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
    ) -> Self {
        Self {
            allow_local,
            bindir: bindir.to_string(),
            builds_root: builds_root.clone(),
            connector_network: connector_network.to_string(),
            logs_tx: logs_tx.clone(),
            build_id_gen,
            db: pool,
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

    pub fn build_failed(self) -> PublicationResult {
        let naughty_collections = status::get_incompatible_collections(&self.output.built);
        self.into_result(Utc::now(), JobStatus::build_failed(naughty_collections))
    }

    pub fn into_result(self, completed_at: DateTime<Utc>, status: JobStatus) -> PublicationResult {
        let UncommittedBuild {
            publication_id,
            user_id,
            detail,
            started_at,
            output,
            live_spec_ids: _,
        } = self;
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
        let mut built = builds::build_catalog(
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

        // If there are any tests, run them now as long as there's no build errors
        if built.built.built_tests.len() > 0 && !cfg!(test) && built.errors().next().is_none() {
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
            tracing::debug!(test_count = %built.live.tests.len(), test_errors = %errors.len(), "finished running tests");

            // TODO(phil): we don't thread through test failures properly, so we
            // never set the `TestFailed` job status.
            if !errors.is_empty() {
                built.built.errors.extend(errors.into_iter());
            }
        }

        Ok(UncommittedBuild {
            publication_id,
            user_id,
            detail,
            started_at: start_time,
            output: built,
            live_spec_ids,
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
