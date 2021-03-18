use super::{indexed, reference, Error};
use models::{build, tables};
use protocol::flow;

pub fn walk_all_captures(
    built_collections: &[tables::BuiltCollection],
    captures: &[tables::Capture],
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    errors: &mut tables::Errors,
) -> tables::BuiltCaptures {
    let mut built_captures = tables::BuiltCaptures::new();

    for capture in captures {
        if let Some(spec) = walk_capture(
            built_collections,
            capture,
            collections,
            derivations,
            endpoints,
            imports,
            errors,
        ) {
            let name = spec.capture.to_string();
            built_captures.push_row(&capture.scope, name, spec);
        }
    }

    indexed::walk_duplicates(
        "capture",
        built_captures.iter().map(|m| (&m.capture, &m.scope)),
        errors,
    );

    built_captures
}

fn walk_capture(
    built_collections: &[tables::BuiltCollection],
    capture: &tables::Capture,
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    errors: &mut tables::Errors,
) -> Option<flow::CaptureSpec> {
    let tables::Capture {
        scope,
        collection: target,
        endpoint,
        patch_config,
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

    let mut endpoint_config = endpoint.base_config.clone();
    json_patch::merge(&mut endpoint_config, &patch_config);

    // TODO - this should use the endpoint-provided resource path,
    // rather than the ingested collection name as a placeholder.
    let resource_path = vec!["placeholder".to_owned(), target.collection.to_string()];
    let resolved_name = build::materialization_name(&endpoint.endpoint, &resource_path);

    Some(build::capture_spec(
        capture,
        built_collection,
        &resolved_name,
        endpoint.endpoint_type,
        endpoint_config.to_string(),
        resource_path,
    ))
}
