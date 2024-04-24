mod capture;
mod collection;
mod handler;
//mod inferred_schema;
mod materialization;
mod publication_status;
//mod source_capture;
mod test;

#[cfg(test)]
pub mod test_util;

use crate::{controlplane::ControlPlane, publications::PublicationResult};
use anyhow::Context;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use models::Id;
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;
use std::fmt::Debug;
use tables::AnySpec;

use self::{
    capture::CaptureStatus,
    collection::CollectionStatus,
    materialization::MaterializationStatus,
    publication_status::{PendingPublication, PublicationInfo, PublicationStatus},
    test::TestStatus,
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
    pub live_spec: Option<AnySpec>,
    pub next_run: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub failures: i32,
    pub errror: Option<String>,
    pub last_pub_id: Id,
    pub logs_token: Uuid,
    pub controller_version: i32,
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
        let catalog_type = job.spec_type.into();

        let live_spec = if let Some(live_json) = &job.live_spec {
            let spec = tables::AnySpec::deserialize(catalog_type, &live_json)
                .context("deserializing live spec")?;
            Some(spec)
        } else {
            None
        };
        let controller_state = ControllerState {
            next_run: job.controller_next_run,
            updated_at: job.updated_at,
            live_spec,
            failures: job.failures,
            catalog_name: job.catalog_name.clone(),
            errror: job.error.clone(),
            last_pub_id: job.last_pub_id.into(),
            logs_token: job.logs_token,
            controller_version: job.controller_version,
            current_status: status,
        };
        Ok(controller_state)
    }
}

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Status {
    Capture(CaptureStatus),
    Collection(CollectionStatus),
    Materialization(MaterializationStatus),
    Test(TestStatus),
    #[serde(other, untagged)]
    Uninitialized,
}

impl Status {
    async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
    ) -> anyhow::Result<Option<NextRun>> {
        let Some(live_spec) = &state.live_spec else {
            tracing::info!("live spec has been deleted, notifying dependents");
            // The live spec has been deleted. Notify dependent controllers, to make sure that they
            // can respond. The controller job row will be deleted automoatically after we return.
            control_plane
                .notify_dependents(state.catalog_name.clone(), state.last_pub_id)
                .await?;
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

#[tracing::instrument(skip_all, err)]
pub async fn finish_pending_publication<C: ControlPlane>(
    pub_status: &mut PublicationStatus,
    pending: PendingPublication,
    state: &ControllerState,
    cp: &mut C,
) -> anyhow::Result<PublicationResult> {
    let detail = pending.details.iter().join(", ");
    let result = cp
        .publish(pending.id, Some(detail), state.logs_token, pending.draft)
        .await?;

    pub_status.record_result(PublicationInfo::observed(&result));
    if result.publication_status.is_success() {
        cp.notify_dependents(state.catalog_name.clone(), result.publication_id)
            .await?;
    }

    Ok(result)
}
