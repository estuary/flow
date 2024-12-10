use std::collections::BTreeSet;

use super::{dependencies::Dependencies, periodic, ControlPlane, ControllerState, NextRun};
use crate::controllers::publication_status::PublicationStatus;
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
        let mut dependencies = Dependencies::resolve(state, control_plane).await?;
        self.passing = false;
        if dependencies
            .update(
                state,
                control_plane,
                &mut self.publications,
                error_on_deleted_dependencies,
            )
            .await?
        {
            // We've successfully published against the latest versions of the dependencies
            self.passing = true;
            return Ok(Some(NextRun::immediately()));
        }

        if periodic::update_periodic_publish(state, &mut self.publications, control_plane).await? {
            // We've successfully published against the latest versions of the dependencies
            self.passing = true;
            return Ok(Some(NextRun::immediately()));
        }

        Ok(periodic::next_periodic_publish(state))
    }
}

fn error_on_deleted_dependencies(
    deleted: &BTreeSet<String>,
) -> anyhow::Result<(String, models::TestDef)> {
    // This will be considered a retryable because technically the
    // collection could spring back into existence.
    anyhow::bail!(
        "test failed because {} of the collection(s) it depends on have been deleted",
        deleted.len()
    )
}
