use std::collections::BTreeMap;

use crate::{Drivers, NoOpDrivers};
use futures::FutureExt;

/// Inspects each of the docker images that's used in the build, returning an `ImageInspections` table.
/// This is effectively an up-front check that each image exists and can be pulled, and the image inspection
/// outputs are used to derive a set of shard labels that determine how network traffic should be forwarded
/// from the outside world into the container.
/// Each image is inspected at most once, and images that are _only_ used for disabled tasks are no-op'd,
/// since you might well want to disable a task _because_ of a problem with the image registry.
/// Any errors are returned along with each image in the table. This is because image pull/inspection
/// errors don't really have a single `scope` after we de-duplicate them. Many tasks could use the
/// same image, and they will _each_ produce an error that's scoped to the task definition, based on
/// the errors in this table.
pub async fn walk_all_images<D: Drivers>(
    drivers: &D,
    captures: &[tables::Capture],
    materializations: &[tables::Materialization],
) -> tables::ImageInspections {
    // map each image to the scope of the first enabled task that uses it. If no tasks are enabled for the image,
    // then we'll use the None as a sentinel to no-op the inspection.
    let mut used_images = BTreeMap::<String, Option<url::Url>>::new();

    for capture in captures {
        if let models::CaptureEndpoint::Connector(config) = &capture.spec.endpoint {
            let is_enabled = !capture.spec.shards.disable;
            let maybe_scope = used_images.entry(config.image.clone()).or_default();
            if maybe_scope.is_none() && is_enabled {
                *maybe_scope = Some(capture.scope.clone());
            }
        }
    }
    for materialization in materializations {
        if let models::MaterializationEndpoint::Connector(config) = &materialization.spec.endpoint {
            let is_enabled = !materialization.spec.shards.disable;
            let maybe_scope = used_images.entry(config.image.clone()).or_default();
            if maybe_scope.is_none() && is_enabled {
                *maybe_scope = Some(materialization.scope.clone());
            }
        }
    }

    // Run all image inspections concurrently

    let mut inspections = tables::ImageInspections::new();
    let inspect_results = used_images.iter().map(|(image, maybe_scope)| async move {
        if maybe_scope.is_some() {
            drivers.inspect_image(image.clone())
        } else {
            NoOpDrivers {}.inspect_image(image.clone())
        }
        .map(|result| (image.clone(), result))
        .await
    });

    let inspect_results: Vec<(String, anyhow::Result<Vec<u8>>)> =
        futures::future::join_all(inspect_results).await;

    for (image, result) in inspect_results {
        match result {
            Ok(json) => {
                inspections.insert_row(image, bytes::Bytes::from(json), None);
            }
            Err(err) => {
                inspections.insert_row(image, bytes::Bytes::new(), Some(err.to_string()));
            }
        }
    }

    inspections
}
