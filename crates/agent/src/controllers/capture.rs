use super::{
    backoff_data_plane_activate,
    publication_status::{ActivationStatus, Dependencies},
    ControlPlane, ControllerErrorExt, ControllerState, NextRun,
};
use crate::controllers::publication_status::PublicationStatus;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// Status of a capture
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct CaptureStatus {
    // TODO: auto discovers are not yet implemented as controllers, but they should be soon.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_auto_discover: Option<DateTime<Utc>>,
    #[serde(default)]
    pub publications: PublicationStatus,
    #[serde(default)]
    pub activation: ActivationStatus,
}

impl CaptureStatus {
    pub async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
        _model: &models::CaptureDef,
    ) -> anyhow::Result<Option<NextRun>> {
        let dependencies = Dependencies::resolve(&state.live_spec, control_plane).await?;
        if dependencies.is_publication_required(state) {
            let draft = self
                .publications
                .start_spec_update(
                    dependencies.next_pub_id(control_plane),
                    state,
                    format!("in response to publication of one or more depencencies"),
                )
                .await;
            if !dependencies.deleted.is_empty() {
                tracing::debug!(deleted_collections = ?dependencies.deleted, "disabling bindings for collections that have been deleted");
                let draft_capture = draft
                    .captures
                    .get_mut_by_key(&models::Capture::new(&state.catalog_name))
                    .expect("draft must contain capture");
                let mut disabled_count = 0;
                for binding in draft_capture.model.as_mut().unwrap().bindings.iter_mut() {
                    if dependencies.deleted.contains(binding.target.as_str()) && !binding.disable {
                        disabled_count += 1;
                        binding.disable = true;
                    }
                }
                let detail = format!(
                    "disabled {disabled_count} binding(s) in response to deleted collections: [{}]",
                    dependencies.deleted.iter().format(", ")
                );
                self.publications
                    .update_pending_draft(detail, control_plane);
            }
        }

        // TODO: implement auto discover here

        if self.publications.has_pending() {
            let _result = self
                .publications
                .finish_pending_publication(state, control_plane)
                .await
                .expect("failed to execute publish")
                .error_for_status()
                .with_maybe_retry(backoff_publication_failure(state.failures))?;
        } else {
            // Not much point in activating if we just published, since we're going to be
            // immediately invoked again.
            self.activation
                .update(state, control_plane)
                .await
                .with_retry(backoff_data_plane_activate(state.failures))?;
            self.publications
                .notify_dependents(state, control_plane)
                .await
                .expect("failed to notify dependents");
        }

        Ok(None)
    }
}

fn backoff_publication_failure(prev_failures: i32) -> Option<NextRun> {
    if prev_failures < 3 {
        Some(NextRun::after_minutes(prev_failures.max(1) as u32))
    } else if prev_failures < 10 {
        Some(NextRun::after_minutes(prev_failures as u32 * 60))
    } else {
        None
    }
}
