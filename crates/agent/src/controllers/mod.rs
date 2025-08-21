pub(crate) mod activation;
pub(crate) mod alerts;
pub(crate) mod capture;
pub(crate) mod catalog_test;
pub(crate) mod collection;
pub(crate) mod config_update;
pub(crate) mod dependencies;
pub(crate) mod executor;
pub(crate) mod materialization;
pub(crate) mod periodic;
pub(crate) mod publication_status;

use crate::controlplane::ControlPlane;
use anyhow::Context;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use models::{status::ControllerStatus, AnySpec, CatalogType, Id};
use proto_flow::{flow, AnyBuiltSpec};
use serde::Serialize;
use sqlx::types::Uuid;
use std::fmt::Debug;

pub use executor::{Event, Inbox, LiveSpecControllerExecutor};

/// This version is used to determine if the controller state is compatible with the current
/// code. Any controller state having a higher version than this will be ignored.
pub const CONTROLLER_VERSION: i32 = 2;

/// Represents the state of a specific controller and catalog_name.
#[derive(Clone, Debug, Serialize)]
pub struct ControllerState {
    pub live_spec_id: Id,
    pub catalog_name: String,
    /// The live spec corresponding to this controller, which will be `None` if
    /// the spec has been deleted.
    pub live_spec: Option<AnySpec>,
    /// The built spec that goes along with the live spec, which will be `None`
    /// if the spec has been deleted.
    pub built_spec: Option<AnyBuiltSpec>,
    /// The last update time of the controller.
    pub controller_updated_at: DateTime<Utc>,
    /// The last update time of the live spec.
    pub live_spec_updated_at: DateTime<Utc>,
    /// The creation time of the live spec
    pub created_at: DateTime<Utc>,
    /// The number of consecutive failures from previous controller runs. This
    /// gets reset to 0 after any successful controller run.
    pub failures: i32,
    /// The error output from the most recent controller run, or `None` if the
    /// most recent run was successful. If `error` is `Some`, then `failures`
    /// will be > 0.
    pub error: Option<String>,
    /// The `last_pub_id` of the corresponding `live_specs` row.
    pub last_pub_id: Id,
    /// The `last_build_id` of the corresponding `live_specs` row.
    pub last_build_id: Id,
    /// The logs token that's used for all operations of this controller. Every
    /// run of a given controller uses the same `logs_token` so that you can
    /// see all the logs in one place.
    pub logs_token: Uuid,
    /// The version of this controller's `current_status`, which will always be
    /// less than or equal to `CONTROLLER_VERSION`.
    pub controller_version: i32,
    /// The current `status` of the controller, which represents the before
    /// state during an update. This is just informational.
    pub current_status: ControllerStatus,
    /// ID of the data plane in which this specification lives. May be zero for tests.
    pub data_plane_id: Id,
    /// Name of the data plane in which this specification lives. May be `None` for tests.
    pub data_plane_name: Option<String>,
    /// The `dependency_hash` of the `live_specs` row, used to determine whether any
    /// dependencies have had their models changed.
    pub live_dependency_hash: Option<String>,
}

/// Returns a struct with all of the initial state that's needed to run a
/// controller.
pub async fn fetch_controller_state(
    controller_task_id: Id,
    db: impl sqlx::PgExecutor<'static>,
) -> anyhow::Result<Option<ControllerState>> {
    let maybe_job = control_plane_api::controllers::fetch_controller_job(controller_task_id, db)
        .await
        .context("fetching controller job")?;

    let Some(job) = maybe_job else {
        return Ok(None);
    };
    let state = ControllerState::parse_db_row(&job)?;
    Ok(Some(state))
}

impl ControllerState {
    pub fn parse_db_row(
        job: &control_plane_api::controllers::ControllerJob,
    ) -> anyhow::Result<ControllerState> {
        let status: ControllerStatus = if job.controller_version == 0 {
            ControllerStatus::Uninitialized
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
            live_spec_id: job.live_spec_id,
            controller_updated_at: job.controller_updated_at,
            live_spec_updated_at: job.live_spec_updated_at,
            created_at: job.created_at,
            live_spec,
            built_spec,
            failures: job.failures,
            catalog_name: job.catalog_name.clone(),
            error: job.error.clone(),
            last_pub_id: job.last_pub_id.into(),
            last_build_id: job.last_build_id.into(),
            logs_token: job.logs_token,
            controller_version: job.controller_version,
            current_status: status,
            data_plane_id: job.data_plane_id.into(),
            data_plane_name: job.data_plane_name.clone(),
            live_dependency_hash: job.live_dependency_hash.clone(),
        };
        Ok(controller_state)
    }
}

/// Returns an `Err` result with a backoff message, and a next retry set to the given `next_attempt`.
fn backoff_err<T>(next_attempt: NextRun, action: &str, fail_count: u32) -> anyhow::Result<T> {
    let plural = if fail_count > 1 { "s" } else { "" };
    anyhow::Result::<T>::Err(anyhow::anyhow!(
        "backing off {action} after {fail_count} failure{plural}"
    ))
    .with_retry(next_attempt)
    .map_err(Into::into)
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

    pub fn immediately() -> NextRun {
        NextRun {
            after_seconds: 0,
            jitter_percent: 0,
        }
    }

    pub fn after_minutes(minutes: u32) -> NextRun {
        NextRun {
            after_seconds: minutes * 60,
            jitter_percent: NextRun::DEFAULT_JITTER,
        }
    }

    pub fn from_duration(dur: chrono::Duration) -> NextRun {
        let after_seconds =
            u32::try_from(dur.num_seconds()).expect("invalid duration for NextRun::from_duration");
        NextRun {
            after_seconds,
            jitter_percent: NextRun::DEFAULT_JITTER,
        }
    }

    pub fn with_jitter_percent(self, jitter_percent: u16) -> Self {
        NextRun {
            after_seconds: self.after_seconds,
            jitter_percent,
        }
    }

    pub fn after(approx_when: DateTime<Utc>) -> NextRun {
        let now = Utc::now();
        let delta = approx_when - now;
        // _Teeechnically_ this could be negative, but we'll just treat that as "run now"
        let after_seconds = delta.max(chrono::TimeDelta::zero()).num_seconds() as u32;
        NextRun {
            after_seconds,
            jitter_percent: NextRun::DEFAULT_JITTER,
        }
    }

    pub fn compute_duration(&self) -> std::time::Duration {
        use rand::Rng;

        let mut dur = std::time::Duration::from_secs(self.after_seconds as u64);
        if self.jitter_percent > 0 && self.after_seconds > 0 {
            let delta_millis = self.after_seconds * 1000;
            let jitter_mul = self.jitter_percent as f64 / 100.0;
            let jitter_max = (delta_millis as f64 * jitter_mul) as u64;
            let jitter_add = rand::thread_rng().gen_range(0..jitter_max);
            dur = dur + std::time::Duration::from_millis(jitter_add);
        }
        dur
    }

    pub fn earliest(runs: impl IntoIterator<Item = Option<NextRun>>) -> Option<NextRun> {
        let mut min = None;
        for run in runs {
            match (min, run) {
                (Some(m), Some(r)) if r < m => min = run,
                (None, _) => min = run,
                _ => { /* pass */ }
            }
        }
        min
    }
}

fn fallback_backoff_next_run(failures: i32) -> NextRun {
    let minutes = match failures.max(1).min(8) as u32 {
        1 => 1,
        2 => 10,
        more => more * 45,
    };
    NextRun::after_minutes(minutes).with_jitter_percent(50)
}

/// Reduces a collection of results into a single result, setting the next run
/// time to the smallest value among all the results, regardless of whether the
/// result is an error or not.
pub fn coalesce_results(
    prev_failures: i32,
    results: impl IntoIterator<Item = anyhow::Result<Option<NextRun>>>,
) -> anyhow::Result<Option<NextRun>> {
    let mut min = None;
    let mut errs = Vec::new();
    for result in results {
        match result {
            Ok(nr) => {
                min = NextRun::earliest([min, nr]);
            }
            Err(e) => match e.downcast::<RetryableError>() {
                Ok(rt) => {
                    min = NextRun::earliest([min, rt.retry]);
                    errs.push(rt.inner);
                }
                Err(reg_err) => {
                    errs.push(reg_err);
                    min = NextRun::earliest([
                        min,
                        Some(fallback_backoff_next_run(prev_failures + 1)),
                    ]);
                }
            },
        }
    }

    // If the controller returned an error and also a desire for an immediate next run,
    // then warn and override the next run to be a few seconds in the future. This is to
    // guard against controllers erroring in a tight loop, which has happened recently.
    // Log at info level because this is not necessarily indicative of any problems,
    // but it definitely _would_ indicate a problem if we see this log consistently
    // repeating for the same spec.
    if !errs.is_empty() && min.as_ref().is_some_and(|n| n.after_seconds == 0) {
        tracing::info!("controller returned error and NextRun::immediately, overriding backoff");
        min = Some(NextRun {
            after_seconds: 3,
            jitter_percent: 50,
        });
    }

    let res = if errs.is_empty() {
        Ok(min)
    } else if errs.len() > 1 {
        Err(anyhow::anyhow!(
            "{} errors:\n- {}",
            errs.len(),
            errs.iter().join("\n- ")
        ))
    } else {
        Err(errs.into_iter().next().unwrap())
    };

    let maybe_next = res.with_maybe_retry(min)?;
    Ok(maybe_next)
}

/// Looks for a failed publication in the history that has a detail message
/// prefixed by the given string, and returns the time of the last attempt, and
/// the total number of failures. This allows the backoff to reset whenever a new version of a
/// dependency is published. Otherwise, you might be waiting a long time after
/// publishing a dependency version that allows this spec to publish
/// successfully.
fn last_pub_failed(
    pub_status: &models::status::publications::PublicationStatus,
    filter_detail_prefix: &str,
) -> Option<(DateTime<Utc>, u32)> {
    pub_status
        .history
        .iter()
        // ignore any history entries that succeeded, OR that came before
        // the last successful publication.
        .take_while(|e| !e.is_success())
        .filter(|e| {
            e.detail
                .as_deref()
                .is_some_and(|d| d.starts_with(filter_detail_prefix))
        })
        // Technically, `completed` should always be Some, except for maybe certain
        // very old/stale controllers. Can probably change this to an `unwrap` soon.
        .flat_map(|e| e.completed.map(|last_attempt| (last_attempt, e.count)))
        .next()
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

/// The main logic of a controller run is performed as an update of the status.
async fn controller_update<C: ControlPlane>(
    status: &mut ControllerStatus,
    state: &ControllerState,
    events: &Inbox,
    control_plane: &C,
) -> anyhow::Result<Option<NextRun>> {
    let Some(live_spec) = &state.live_spec else {
        // There's no need to delete tests and nothing depends on them.
        if let Some(catalog_type) = status.catalog_type().filter(|ct| *ct != CatalogType::Test) {
            // The live spec has been deleted. Delete the data plane
            // resources, and then notify dependent controllers, to make
            // sure that they can respond. The controller job row will be
            // deleted automatically after we return.
            crate::timeout(
                std::time::Duration::from_secs(60),
                control_plane.data_plane_delete(
                    state.catalog_name.clone(),
                    catalog_type,
                    state.data_plane_id,
                ),
                || "Timeout while deleting from data-plane",
            )
            .await
            .context("failed to delete from data-plane")
            .with_retry(backoff_data_plane_activate(state.failures))?;

            control_plane
                .notify_dependents(state.live_spec_id)
                .await
                .expect("failed to update dependents");
            tracing::info!("deleted data-plane resources for deleted live spec");
        } else {
            tracing::info!("skipping data-plane deletion because there is no spec_type");
        }
        return Ok(None);
    };

    let next_run = match live_spec {
        AnySpec::Capture(c) => {
            let capture_status = status.as_capture_mut()?;
            capture::update(capture_status, state, events, control_plane, c).await?
        }
        AnySpec::Collection(c) => {
            let collection_status = status.as_collection_mut()?;
            collection::update(collection_status, state, events, control_plane, c).await?
        }
        AnySpec::Materialization(m) => {
            let materialization_status = status.as_materialization_mut()?;

            materialization::update(materialization_status, state, events, control_plane, m).await?
        }
        AnySpec::Test(t) => {
            let test_status = status.as_test_mut()?;
            catalog_test::update(test_status, state, events, control_plane, t).await?
        }
    };
    tracing::info!(?next_run, "finished controller update");
    Ok(next_run)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_next_run() {
        // Test with zero, to make sure that it doesn't panic.
        let next = NextRun::after_minutes(0)
            .with_jitter_percent(20)
            .compute_duration();
        assert!(next.is_zero());

        let next = NextRun::after(Utc::now());
        assert_eq!(0, next.after_seconds);
        assert!(next.with_jitter_percent(0).compute_duration().is_zero());

        let millis = NextRun::after_minutes(1)
            .with_jitter_percent(20)
            .compute_duration()
            .as_millis();
        assert!(millis >= 60000, "duration too small, got: {millis}ms");
        assert!(millis <= 72000, "duration too big, got: {millis}ms");
    }

    #[test]
    fn test_coalesce_results() {
        // All Ok with a retry
        let res = coalesce_results(
            0,
            [
                Ok(None),
                Ok(Some(NextRun::after_minutes(1))),
                Ok(Some(NextRun::after_minutes(55))),
            ],
        );
        assert_eq!(
            60,
            res.expect("should be Ok")
                .expect("should be Some")
                .after_seconds
        );

        // Multiple errors, with retry set from an Ok result
        let res = coalesce_results(
            999,
            [
                Ok(None),
                Err(RetryableError {
                    inner: anyhow::anyhow!("first error"),
                    retry: None,
                }
                .into()),
                Ok(Some(NextRun::after_minutes(1))),
                Err(RetryableError {
                    inner: anyhow::anyhow!("second error"),
                    retry: Some(NextRun::after_minutes(5)),
                }
                .into()),
            ],
        );
        let err = res.unwrap_err().downcast::<RetryableError>().unwrap();
        assert_eq!(60, err.retry.unwrap().after_seconds);
        assert_eq!(
            "2 errors:\n- first error\n- second error (will retry)",
            &err.to_string()
        );

        // No next run
        let res = coalesce_results(
            0,
            [
                Ok(None),
                Err(RetryableError {
                    inner: anyhow::anyhow!("an error"),
                    retry: None,
                }
                .into()),
            ],
        );
        let err = res.unwrap_err().downcast::<RetryableError>().unwrap();
        assert!(err.retry.is_none());

        // Multiple errors, with retry set from an Err result
        let res = coalesce_results(
            0,
            [
                Ok(None),
                Err(RetryableError {
                    inner: anyhow::anyhow!("first error"),
                    retry: None,
                }
                .into()),
                Ok(Some(NextRun::after_minutes(55))),
                Err(RetryableError {
                    inner: anyhow::anyhow!("second error"),
                    retry: Some(NextRun::after_minutes(5)),
                }
                .into()),
            ],
        );
        let err = res.unwrap_err().downcast::<RetryableError>().unwrap();
        assert_eq!(300, err.retry.unwrap().after_seconds);
        assert_eq!(
            "2 errors:\n- first error\n- second error (will retry)",
            &err.to_string()
        );

        // Single error with default retry
        let res = coalesce_results(
            1,
            [
                Ok(Some(NextRun::after_minutes(999))),
                Err(anyhow::anyhow!("just a plain old test error")),
            ],
        );
        let err = res.unwrap_err().downcast::<RetryableError>().unwrap();
        assert_eq!(600, err.retry.unwrap().after_seconds);

        // Multiple errors, with default retry
        let res = coalesce_results(
            0,
            [
                Ok(None),
                Err(anyhow::anyhow!("not a retryable error")),
                Ok(Some(NextRun::after_minutes(999))),
                Err(RetryableError {
                    inner: anyhow::anyhow!("second error"),
                    retry: Some(NextRun::after_minutes(888)),
                }
                .into()),
            ],
        );
        let err = res.unwrap_err().downcast::<RetryableError>().unwrap();
        assert_eq!(60, err.retry.unwrap().after_seconds);
        assert_eq!(
            "2 errors:\n- not a retryable error\n- second error (will retry)",
            &err.to_string()
        );
    }
}
