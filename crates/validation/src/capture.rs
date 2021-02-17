use super::{indexed, reference, Error};
use itertools::Itertools;
use models::tables;

pub fn walk_all_captures(
    captures: &[tables::Capture],
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    errors: &mut tables::Errors,
) {
    for capture in captures {
        walk_capture(
            capture,
            collections,
            derivations,
            endpoints,
            imports,
            errors,
        );
    }
    indexed::walk_duplicates(
        "capture",
        captures.iter().map(|c| (&c.capture, &c.scope)),
        errors,
    );

    // TODO: derive a built_captures table, in line with built_materializations.
    // It should represent the collection, Option<EndpointType> (where None means "push"),
    // and EndpointConfig.

    // Require that tuples of (collection, endpoint, patch_config) are globally unique.
    // TODO: this de-dup should be in terms of built_captures. See walk_all_materializations.
    let cmp = |lhs: &&tables::Capture, rhs: &&tables::Capture| {
        (&lhs.collection, &lhs.endpoint)
            .cmp(&(&rhs.collection, &rhs.endpoint))
            .then_with(|| json::json_cmp(&lhs.patch_config, &rhs.patch_config))
    };
    for (lhs, rhs) in captures.iter().sorted_by(cmp).tuple_windows() {
        if cmp(&lhs, &rhs) == std::cmp::Ordering::Equal {
            Error::CaptureMultiplePulls {
                lhs_name: lhs.capture.to_string(),
                rhs_name: rhs.capture.to_string(),
                rhs_scope: rhs.scope.clone(),
                target: lhs.collection.to_string(),
            }
            .push(&lhs.scope, errors);
        }
    }
}

fn walk_capture(
    capture: &tables::Capture,
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    errors: &mut tables::Errors,
) {
    let tables::Capture {
        scope,
        capture: name,
        collection: target,
        endpoint,
        allow_push: _,
        patch_config: _,
    } = capture;

    indexed::walk_name(
        scope,
        "capture",
        name.as_ref(),
        &indexed::CAPTURE_RE,
        errors,
    );

    // Ensure we can dereference the capture's target.
    let _ = reference::walk_reference(
        scope,
        "capture",
        name.as_ref(),
        "collection",
        target,
        collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    );

    // But it must not be a derivation.
    if let Some(_) = derivations.iter().find(|d| d.derivation == *target) {
        Error::CaptureOfDerivation {
            capture: name.to_string(),
            derivation: target.to_string(),
        }
        .push(scope, errors);
    }

    if let Some(endpoint) = endpoint {
        // Dereference the captures's endpoint.
        if let Some(endpoint) = reference::walk_reference(
            scope,
            "capture",
            name.as_ref(),
            "endpoint",
            endpoint,
            endpoints,
            |e| (&e.endpoint, &e.scope),
            imports,
            errors,
        ) {
            // Ensure it's of a compatible endpoint type.
            if !matches!(endpoint.endpoint_type, protocol::flow::EndpointType::S3) {
                Error::CaptureEndpointType {
                    type_: endpoint.endpoint_type,
                }
                .push(scope, errors);
            }
        }
    }
}
