use std::collections::BTreeSet;

use super::{dependencies::Dependencies, periodic, ControlPlane, ControllerState, Inbox, NextRun};
use models::status::catalog_test::TestStatus;

pub async fn update<C: ControlPlane>(
    status: &mut TestStatus,
    state: &ControllerState,
    _events: &Inbox,
    control_plane: &C,
    _model: &models::TestDef,
) -> anyhow::Result<Option<NextRun>> {
    let mut dependencies = Dependencies::resolve(state, control_plane).await?;
    status.passing = false;
    if dependencies
        .update(
            state,
            control_plane,
            &mut status.publications,
            error_on_deleted_dependencies,
        )
        .await?
    {
        // We've successfully published against the latest versions of the dependencies
        status.passing = true;
        return Ok(Some(NextRun::immediately()));
    }

    if periodic::update_periodic_publish(state, &mut status.publications, control_plane).await? {
        // We've successfully published against the latest versions of the dependencies
        status.passing = true;
        return Ok(Some(NextRun::immediately()));
    }

    // If dependencies are up to date, then the test is passing.
    status.passing = true;
    Ok(periodic::next_periodic_publish(state))
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
