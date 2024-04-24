use super::{publication_status::Dependencies, ControlPlane, ControllerState, NextRun};
use crate::controllers::publication_status::PublicationStatus;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// placeholder capture status
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct CaptureStatus {
    // TODO: auto discovers are not yet implemented as controllers, but they should be soon.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_auto_discover: Option<DateTime<Utc>>,
    #[serde(default)]
    pub publications: PublicationStatus,
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
                .await?;
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
                .await?;
            // TODO: check result for incompatible collections and increment backfill counters as necessary
        } else {
            self.publications
                .notify_dependents(state, control_plane)
                .await?;
        }

        Ok(self.publications.next_run(state))
    }
}
