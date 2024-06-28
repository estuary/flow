use super::{publication_status::Dependencies, ControlPlane, ControllerState, NextRun};
use crate::controllers::publication_status::PublicationStatus;
use crate::controllers::ControllerErrorExt;
use anyhow::Context;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct TestStatus {
    pub passing: bool,
    #[serde(default)]
    pub publications: PublicationStatus,
}

impl TestStatus {
    pub async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
        _model: &models::TestDef,
    ) -> anyhow::Result<Option<NextRun>> {
        let dependencies = Dependencies::resolve(&state.live_spec, control_plane).await?;
        if dependencies.is_publication_required(state) {
            self.publications
                .start_spec_update(
                    dependencies.next_pub_id(control_plane),
                    state,
                    format!("in response to publication of one or more depencencies"),
                )
                .await;

            let result = self
                .publications
                .finish_pending_publication(state, control_plane)
                .await
                .context("failed to execute publication")?;
            self.passing = result.status.is_success();
            // return a terminal error if the publication failed
            result.error_for_status().do_not_retry()?;
            // TODO(phil): This would be a great place to trigger an alert if the publication failed
        } else {
            // We're up-to-date with our dependencies, which means the test has been published successfully
            self.passing = true;
        }

        // Don't re-try when tests fail, because fixing them will likely require a change to either
        // the test itself or one of its dependencies.
        Ok(None)
    }
}
