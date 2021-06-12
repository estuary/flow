use super::{indexed, reference, Drivers, Error};
use futures::FutureExt;
use models::{build, tables};
use protocol::{capture, flow};

pub async fn walk_all_captures<D: Drivers>(
    drivers: &D,
    built_collections: &[tables::BuiltCollection],
    captures: &[tables::Capture],
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    errors: &mut tables::Errors,
) -> tables::BuiltCaptures {
    let mut validations = Vec::new();

    for capture in captures {
        validations.extend(
            walk_capture_request(
                built_collections,
                capture,
                collections,
                derivations,
                endpoints,
                imports,
                errors,
            )
            .into_iter(),
        );
    }

    // Run all validations concurrently.
    let validations =
        validations
            .into_iter()
            .map(|(built_collection, capture, request)| async move {
                drivers
                    .validate_capture(request.clone())
                    // Pass-through the capture & CollectionSpec for future verification.
                    .map(|response| (built_collection, capture, request, response))
                    .await
            });
    let validations = futures::future::join_all(validations).await;

    let mut built_captures = tables::BuiltCaptures::new();

    for (built_collection, capture, request, response) in validations {
        match response {
            Ok(response) => {
                let capture::ValidateRequest {
                    endpoint_type,
                    endpoint_name,
                    endpoint_spec_json,
                    ..
                } = request;

                // Safe to unwrap because walk_capture_request previously
                // cast to i32 from EndpointType.
                let endpoint_type = flow::EndpointType::from_i32(endpoint_type).unwrap();

                let capture::ValidateResponse { resource_path } = response;

                // Capture tasks are named by their captured collection,
                // followed by the fully qualified endpoint resource path from
                // which they capture. This means that at most one capture task
                // may exist for a given pair of (collection, endpoint resource).
                // However, an endpoint resource may be captured into multiple
                // collections, and a collection may have multiple captures.
                let resolved_name = format!(
                    "{}__{}",
                    capture.collection.as_str(),
                    build::encode_endpoint_path(&endpoint_name, &resource_path)
                );

                let spec = build::capture_spec(
                    capture,
                    built_collection,
                    &resolved_name,
                    endpoint_type,
                    endpoint_spec_json,
                    resource_path,
                );
                built_captures.push_row(&capture.scope, resolved_name, spec);
            }
            Err(err) => {
                Error::CaptureDriver {
                    name: request.endpoint_name,
                    detail: err,
                }
                .push(&capture.scope, errors);
            }
        }
    }

    indexed::walk_duplicates(
        "capture",
        built_captures.iter().map(|m| (&m.capture, &m.scope)),
        errors,
    );

    built_captures
}

fn walk_capture_request<'a>(
    built_collections: &'a [tables::BuiltCollection],
    capture: &'a tables::Capture,
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    errors: &mut tables::Errors,
) -> Option<(
    &'a tables::BuiltCollection,
    &'a tables::Capture,
    capture::ValidateRequest,
)> {
    let tables::Capture {
        scope,
        collection: target,
        endpoint,
        endpoint_patch_spec,
    } = capture;

    let target = reference::walk_reference(
        scope,
        "capture",
        "collection",
        target,
        collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    );

    let endpoint = reference::walk_reference(
        scope,
        "capture",
        "endpoint",
        endpoint,
        endpoints,
        |e| (&e.endpoint, &e.scope),
        imports,
        errors,
    );

    // We must resolve both |target| and |endpoint| to continue.
    let (target, endpoint) = match (target, endpoint) {
        (Some(s), Some(e)) => (s, e),
        _ => return None,
    };

    // Collection must be an ingestion, and not a derivation.
    if let Some(_) = derivations
        .iter()
        .find(|d| d.derivation == target.collection)
    {
        Error::CaptureOfDerivation {
            derivation: target.collection.to_string(),
        }
        .push(scope, errors);
    }

    let built_collection = built_collections
        .iter()
        .find(|c| c.collection == target.collection)
        .unwrap();

    let mut endpoint_spec = endpoint.base_spec.clone();
    json_patch::merge(&mut endpoint_spec, &endpoint_patch_spec);

    let request = capture::ValidateRequest {
        endpoint_name: endpoint.endpoint.to_string(),
        endpoint_type: endpoint.endpoint_type as i32,
        endpoint_spec_json: endpoint_spec.to_string(),
        collection: Some(built_collection.spec.clone()),
    };

    Some((built_collection, capture, request))
}
