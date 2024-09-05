mod capture;
mod catalog_test;
mod collection;
mod handler;
mod materialization;
mod publication_status;

use crate::controlplane::ControlPlane;
use anyhow::Context;
use chrono::{DateTime, Utc};
use models::{AnySpec, CatalogType, Id};
use proto_flow::{flow, AnyBuiltSpec};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;
use std::fmt::Debug;

use self::{
    capture::CaptureStatus, catalog_test::TestStatus, collection::CollectionStatus,
    materialization::MaterializationStatus,
};

pub use handler::ControllerHandler;

/// This version is used to determine if the controller state is compatible with the current
/// code. Any controller state having a lower version than this will need to be run in order
/// to "upgrade" it. Any controller state having a higher version than this _must_ be ignored.
///
/// Increment this version whenever we need to ensure that controllers re-visit all live specs.
pub const CONTROLLER_VERSION: i32 = 1;

/// Represents the state of a specific controller and catalog_name.
#[derive(Clone, Debug, Serialize)]
pub struct ControllerState {
    pub catalog_name: String,
    /// The live spec corresponding to this controller, which will be `None` if
    /// the spec has been deleted.
    pub live_spec: Option<AnySpec>,
    /// The built spec that goes along with the live spec, which will be `None`
    /// if the spec has been deleted.
    pub built_spec: Option<AnyBuiltSpec>,
    /// The current `controller_next_run` value. This is useful for knowing
    /// when the controller run was desired, which may have been earlier than
    /// the actual time of the current run.
    pub next_run: Option<DateTime<Utc>>,
    /// The last update time of the controller.
    pub updated_at: DateTime<Utc>,
    /// The number of consecutive failures from previous controller runs. This
    /// gets reset to 0 after any successful controller run.
    pub failures: i32,
    /// The error output from the most recent controller run, or `None` if the
    /// most recent run was successful. If `error` is `Some`, then `failures`
    /// will be > 0.
    pub error: Option<String>,
    /// The `last_pub_id` of the corresponding `live_specs` row. This is used
    /// to determine when the controller needs to re-publish a task, by
    /// comparing this value to the `last_pub_id`s of all its dependencies.
    pub last_pub_id: Id,
    /// The logs token that's used for all operations of this controller. Every
    /// run of a given controller uses the same `logs_token` so that you can
    /// see all the logs in one place.
    pub logs_token: Uuid,
    /// The version of this controller's `current_status`, which will always be
    /// less than or equal to `CONTROLLER_VERSION`.
    pub controller_version: i32,
    /// The current `status` of the controller, which represents the before
    /// state during an update. This is just informational.
    pub current_status: Status,
}

impl ControllerState {
    pub fn parse_db_row(
        job: &agent_sql::controllers::ControllerJob,
    ) -> anyhow::Result<ControllerState> {
        let status: Status = if job.controller_version == 0 {
            Status::Uninitialized
        } else {
            serde_json::from_str(job.status.get()).context("deserializing controller status")?
        };

        // Spec_type may be null for specs last published by a previous version.
        // We now leave the spec_type in place when soft-deleting live_specs.
        let maybe_type = job.spec_type.map(Into::<CatalogType>::into);
        let (live_spec, built_spec) = if let Some(catalog_type) = maybe_type {
            let live_spec = if let Some(live_json) = &job.live_spec {
                let spec = AnySpec::deserialize(catalog_type, live_json.get())
                    .context("deserializing live spec")?;
                Some(spec)
            } else {
                None
            };

            let built_spec = if let Some(built_json) = &job.built_spec {
                let s = match catalog_type {
                    CatalogType::Capture => {
                        AnyBuiltSpec::Capture(serde_json::from_str::<flow::CaptureSpec>(
                            built_json.get(),
                        )?)
                    }
                    CatalogType::Collection => {
                        AnyBuiltSpec::Collection(serde_json::from_str::<flow::CollectionSpec>(
                            built_json.get(),
                        )?)
                    }
                    CatalogType::Materialization => {
                        AnyBuiltSpec::Materialization(serde_json::from_str::<
                            flow::MaterializationSpec,
                        >(built_json.get())?)
                    }
                    CatalogType::Test => AnyBuiltSpec::Test(
                        serde_json::from_str::<flow::TestSpec>(built_json.get())?,
                    ),
                };
                Some(s)
            } else {
                None
            };
            if live_spec.is_some() != built_spec.is_some() {
                anyhow::bail!(
                    "expected live and built specs to both be Some or None, got live: {}, built: {}",
                    live_spec.is_some(),
                    built_spec.is_some()
                );
            }

            (live_spec, built_spec)
        } else {
            (None, None)
        };

        let controller_state = ControllerState {
            next_run: job.controller_next_run,
            updated_at: job.updated_at,
            live_spec,
            built_spec,
            failures: job.failures,
            catalog_name: job.catalog_name.clone(),
            error: job.error.clone(),
            last_pub_id: job.last_pub_id.into(),
            logs_token: job.logs_token,
            controller_version: job.controller_version,
            current_status: status,
        };
        Ok(controller_state)
    }
}

/// A wrapper around an `anyhow::Error` that also contains retry information.
/// Allows marking an error as non-retryable by setting `retry` to `None`.
#[derive(Debug)]
pub struct RetryableError {
    pub inner: anyhow::Error,
    pub retry: Option<NextRun>,
}

impl std::fmt::Display for RetryableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let maybe_retry = if self.retry.is_some() {
            "(will retry)"
        } else {
            "(terminal error)"
        };
        write!(f, "{} {}", self.inner, maybe_retry)
    }
}

impl std::error::Error for RetryableError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

/// A trait to help controllers specify retry behavior for different errors.
/// Allows calling `my_result.with_retry(next_run)?` in order to consider an
/// error retriable.
trait ControllerErrorExt {
    type Success;

    fn with_maybe_retry(
        self,
        maybe_retry: Option<NextRun>,
    ) -> Result<Self::Success, RetryableError>;

    fn with_retry(self, after: NextRun) -> Result<Self::Success, RetryableError>
    where
        Self: Sized,
    {
        self.with_maybe_retry(Some(after))
    }

    fn do_not_retry(self) -> Result<Self::Success, RetryableError>
    where
        Self: Sized,
    {
        self.with_maybe_retry(None)
    }
}

impl<T, E: Into<anyhow::Error>> ControllerErrorExt for Result<T, E> {
    type Success = T;
    fn with_maybe_retry(self, after: Option<NextRun>) -> Result<T, RetryableError> {
        self.map_err(|e| RetryableError {
            inner: e.into(),
            retry: after,
        })
    }
}

/// Represents a desired future run of the controller.
/// This is represented as a simple duration and jitter in order to make
/// testing easier, and to keep controller implementations simple.
#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
pub struct NextRun {
    pub after_seconds: u32,
    pub jitter_percent: u16,
}

impl std::cmp::PartialOrd for NextRun {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for NextRun {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.after_seconds
            .cmp(&other.after_seconds)
            .then(self.jitter_percent.cmp(&other.jitter_percent))
    }
}

impl NextRun {
    const DEFAULT_JITTER: u16 = 20;

    pub fn after_minutes(minutes: u32) -> NextRun {
        NextRun {
            after_seconds: minutes * 60,
            jitter_percent: NextRun::DEFAULT_JITTER,
        }
    }

    pub fn with_jitter_percent(self, jitter_percent: u16) -> Self {
        NextRun {
            after_seconds: self.after_seconds,
            jitter_percent,
        }
    }

    /// Returns an absolute time at which the next run should become due.
    /// Uses only millisecond precision to ensure that the timestamp can be losslessly
    /// round-tripped through postgres.
    pub fn compute_time(&self) -> DateTime<Utc> {
        use rand::Rng;

        let delta_millis = self.after_seconds as i64 * 1000;
        let jitter_mul = self.jitter_percent as f64 / 100.0;
        let jitter_max = (delta_millis as f64 * jitter_mul) as i64;
        let jitter_add = rand::thread_rng().gen_range(0..jitter_max);
        let dur = chrono::TimeDelta::milliseconds(delta_millis + jitter_add);
        Utc::now() + dur
    }
}

/// Represents the internal state of a controller.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(tag = "type")]
pub enum Status {
    Capture(CaptureStatus),
    Collection(CollectionStatus),
    Materialization(MaterializationStatus),
    Test(TestStatus),
    #[schemars(skip)]
    #[serde(other, untagged)]
    Uninitialized,
}

/// Returns a backoff after failing to activate or delete shards/journals in the
/// data-plane. Failures to do so should be re-tried indefinitely.
fn backoff_data_plane_activate(prev_failures: i32) -> NextRun {
    let after_minutes = if prev_failures < 3 {
        prev_failures.max(1) as u32
    } else {
        prev_failures as u32 * 15
    };
    NextRun::after_minutes(after_minutes)
}

impl Status {
    pub fn json_schema() -> schemars::schema::RootSchema {
        let mut settings = schemars::gen::SchemaSettings::draft2019_09();
        //settings.option_add_null_type = false;
        //settings.inline_subschemas = true;
        let generator = schemars::gen::SchemaGenerator::new(settings);
        generator.into_root_schema_for::<Status>()
    }

    fn catalog_type(&self) -> Option<CatalogType> {
        match self {
            Status::Capture(_) => Some(CatalogType::Capture),
            Status::Collection(_) => Some(CatalogType::Collection),
            Status::Materialization(_) => Some(CatalogType::Materialization),
            Status::Test(_) => Some(CatalogType::Test),
            Status::Uninitialized => None,
        }
    }

    /// The main logic of a controller run is performed as an update of the status.
    async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
    ) -> anyhow::Result<Option<NextRun>> {
        let Some(live_spec) = &state.live_spec else {
            // There's no need to delete tests and nothing depends on them.
            if let Some(catalog_type) = self.catalog_type().filter(|ct| *ct != CatalogType::Test) {
                // The live spec has been deleted. Delete the data plane
                // resources, and then notify dependent controllers, to make
                // sure that they can respond. The controller job row will be
                // deleted automatically after we return.
                control_plane
                    .data_plane_delete(state.catalog_name.clone(), catalog_type)
                    .await
                    .context("deleting from data-plane")
                    .with_retry(backoff_data_plane_activate(state.failures))?;
                tracing::info!("deleted from data-plane");
                control_plane
                    .notify_dependents(state.catalog_name.clone())
                    .await
                    .expect("failed to update dependents");
            } else {
                tracing::info!("skipping data-plane deletion because there is no spec_type");
            }
            return Ok(None);
        };

        let next_run = match live_spec {
            AnySpec::Capture(c) => {
                let capture_status = self.as_capture_mut()?;
                capture_status.update(state, control_plane, c).await?
            }
            AnySpec::Collection(c) => {
                let collection_status = self.as_collection_mut()?;
                collection_status.update(state, control_plane, c).await?
            }
            AnySpec::Materialization(m) => {
                let materialization_status = self.as_materialization_mut()?;

                materialization_status
                    .update(state, control_plane, m)
                    .await?
            }
            AnySpec::Test(t) => {
                let test_status = self.as_test_mut()?;
                test_status.update(state, control_plane, t).await?
            }
        };
        tracing::info!(?next_run, "finished controller update");
        Ok(next_run)
    }

    pub fn is_uninitialized(&self) -> bool {
        matches!(self, Status::Uninitialized)
    }

    fn as_capture_mut(&mut self) -> anyhow::Result<&mut CaptureStatus> {
        if self.is_uninitialized() {
            *self = Status::Capture(Default::default());
        }
        match self {
            Status::Capture(c) => Ok(c),
            _ => anyhow::bail!("expected capture status"),
        }
    }

    fn as_collection_mut(&mut self) -> anyhow::Result<&mut CollectionStatus> {
        if self.is_uninitialized() {
            *self = Status::Collection(Default::default());
        }
        match self {
            Status::Collection(c) => Ok(c),
            _ => anyhow::bail!("expected collection status"),
        }
    }

    fn as_materialization_mut(&mut self) -> anyhow::Result<&mut MaterializationStatus> {
        if self.is_uninitialized() {
            *self = Status::Materialization(Default::default());
        }
        match self {
            Status::Materialization(m) => Ok(m),
            _ => anyhow::bail!("expected materialization status"),
        }
    }

    fn as_test_mut(&mut self) -> anyhow::Result<&mut TestStatus> {
        if self.is_uninitialized() {
            *self = Status::Test(Default::default());
        }
        match self {
            Status::Test(t) => Ok(t),
            _ => anyhow::bail!("expected test status"),
        }
    }

    #[cfg(test)]
    pub fn unwrap_capture(&self) -> &CaptureStatus {
        match self {
            Status::Capture(c) => c,
            _ => panic!("expected capture status"),
        }
    }

    #[cfg(test)]
    pub fn unwrap_collection(&self) -> &CollectionStatus {
        match self {
            Status::Collection(c) => c,
            _ => panic!("expected collection status"),
        }
    }

    #[cfg(test)]
    pub fn unwrap_materialization(&self) -> &MaterializationStatus {
        match self {
            Status::Materialization(m) => m,
            _ => panic!("expected materialization status"),
        }
    }

    #[cfg(test)]
    pub fn unwrap_test(&self) -> &TestStatus {
        match self {
            Status::Test(t) => t,
            _ => panic!("expected test status"),
        }
    }
}

/// Selects the smallest next run from among the arguments, returning `None`
/// only if all `next_runs` are `None`.
fn reduce_next_run(next_runs: &[Option<NextRun>]) -> Option<NextRun> {
    let mut min: Option<NextRun> = None;
    for next_run in next_runs {
        match (min, *next_run) {
            (Some(l), Some(r)) => min = Some(l.min(r)),
            (None, Some(r)) => min = Some(r),
            (_, None) => { /* nada */ }
        }
    }
    min
}

fn datetime_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": "string",
        "format": "date-time",
    }))
    .unwrap()
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeSet, VecDeque};

    use chrono::TimeZone;
    use models::{SourceCaptureDef, Capture};

    use super::*;
    use crate::controllers::materialization::SourceCaptureStatus;
    use crate::controllers::publication_status::{
        ActivationStatus, PublicationInfo, PublicationStatus,
    };
    use crate::draft::Error;
    use crate::publications::{AffectedConsumer, IncompatibleCollection, JobStatus, RejectedField};

    #[test]
    fn test_status_round_trip_serde() {
        let mut add_bindings = BTreeSet::new();
        add_bindings.insert(models::Collection::new("snails/shells"));

        let pub_status = PublicationInfo {
            id: Id::new([4, 3, 2, 1, 1, 2, 3, 4]),
            created: Some(Utc.with_ymd_and_hms(2024, 5, 30, 9, 10, 11).unwrap()),
            completed: Some(Utc.with_ymd_and_hms(2024, 5, 30, 9, 10, 11).unwrap()),
            detail: Some("some detail".to_string()),
            result: Some(JobStatus::build_failed(vec![IncompatibleCollection {
                collection: "snails/water".to_string(),
                requires_recreation: Vec::new(),
                affected_materializations: vec![AffectedConsumer {
                    name: "snails/materialize".to_string(),
                    fields: vec![RejectedField {
                        field: "a_field".to_string(),
                        reason: "do not like".to_string(),
                    }],
                }],
            }])),
            errors: vec![Error {
                catalog_name: "snails/shells".to_string(),
                scope: Some("flow://materializations/snails/shells".to_string()),
                detail: "a_field simply cannot be tolerated".to_string(),
            }],
        };
        let mut history = VecDeque::new();
        history.push_front(pub_status);

        let status = Status::Materialization(MaterializationStatus {
            activation: ActivationStatus {
                last_activated: Id::new([1, 2, 3, 4, 4, 3, 2, 1]),
            },
            source_capture: Some(SourceCaptureStatus {
                up_to_date: false,
                add_bindings,
                source_capture: SourceCaptureDef {
                    capture: Capture::new("snails/capture"),
                    ..Default::default()
                }
            }),
            publications: PublicationStatus {
                target_pub_id: Id::new([1, 2, 3, 4, 5, 6, 7, 8]),
                max_observed_pub_id: Id::new([1, 2, 3, 4, 5, 6, 7, 8]),
                history,
            },
        });

        let as_json = serde_json::to_string_pretty(&status).expect("failed to serialize status");
        let round_tripped: Status =
            serde_json::from_str(&as_json).expect("failed to deserialize status");

        #[derive(Debug)]
        #[allow(unused)]
        struct StatusSnapshot {
            starting: Status,
            json: String,
            parsed: Status,
        }

        insta::assert_debug_snapshot!(
            "materialization-status-round-trip",
            StatusSnapshot {
                starting: status,
                json: as_json,
                parsed: round_tripped,
            }
        );
    }

    #[test]
    fn test_status_json_schema() {
        let schema = serde_json::to_value(Status::json_schema()).unwrap();
        insta::assert_json_snapshot!(schema);
    }
}
