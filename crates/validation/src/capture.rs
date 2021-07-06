use super::{indexed, reference, Drivers, Error};
use futures::FutureExt;
use itertools::{EitherOrBoth, Itertools};
use models::tables;
use protocol::{capture, flow};

pub async fn walk_all_captures<D: Drivers>(
    drivers: &D,
    built_collections: &[tables::BuiltCollection],
    capture_bindings: &[tables::CaptureBinding],
    captures: &[tables::Capture],
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    imports: &[&tables::Import],
    errors: &mut tables::Errors,
) -> tables::BuiltCaptures {
    let mut validations = Vec::new();

    // Index |capture_bindings| on (capture, index),
    // then group bindings having the same capture.
    let capture_bindings = capture_bindings
        .iter()
        .sorted_by_key(|c| (&c.capture, c.capture_index))
        .group_by(|c| &c.capture);

    // Walk ordered captures, left-joined by their bindings.
    for (capture, bindings) in captures
        .iter()
        .sorted_by_key(|c| &c.capture)
        .merge_join_by(capture_bindings.into_iter(), |l, (r, _)| l.capture.cmp(r))
        .filter_map(|eob| match eob {
            EitherOrBoth::Both(capture, (_, bindings)) => Some((capture, Some(bindings))),
            EitherOrBoth::Left(capture) => Some((capture, None)),
            EitherOrBoth::Right(_) => None,
        })
    {
        let mut capture_errors = tables::Errors::new();

        // Require the capture name is valid.
        indexed::walk_name(
            &capture.scope,
            "capture",
            &capture.capture,
            &indexed::CAPTURE_RE,
            &mut capture_errors,
        );

        let validation = walk_capture_request(
            built_collections,
            capture,
            bindings.into_iter().flatten().collect_vec(),
            collections,
            derivations,
            imports,
            &mut capture_errors,
        );

        // Skip validation if errors were encountered building the request.
        if capture_errors.is_empty() {
            validations.extend(validation.into_iter());
        } else {
            errors.extend(capture_errors.into_iter());
        }
    }

    // Run all validations concurrently.
    let validations =
        validations
            .into_iter()
            .map(|(capture, binding_models, request)| async move {
                drivers
                    .validate_capture(request.clone())
                    .map(|response| (capture, binding_models, request, response))
                    .await
            });
    let validations = futures::future::join_all(validations).await;

    let mut built_captures = tables::BuiltCaptures::new();

    for (capture, binding_models, request, response) in validations {
        match response {
            Ok(response) => {
                let tables::Capture {
                    scope,
                    interval_seconds,
                    ..
                } = capture;

                let capture::ValidateRequest {
                    endpoint_type,
                    endpoint_spec_json,
                    bindings: binding_requests,
                    capture: name,
                } = request;

                let capture::ValidateResponse {
                    bindings: binding_responses,
                } = response;

                // We constructed |binding_requests| while processing binding models.
                assert!(binding_requests.len() == binding_models.len());

                if binding_requests.len() != binding_responses.len() {
                    Error::CaptureDriver {
                        name: name.to_string(),
                        detail: anyhow::anyhow!(
                            "driver returned wrong number of bindings (expected {}, got {})",
                            binding_requests.len(),
                            binding_responses.len()
                        ),
                    }
                    .push(scope, errors);
                }

                // Join requests, responses and models to produce tuples
                // of (binding scope, built binding).
                let bindings: Vec<_> = binding_requests
                    .into_iter()
                    .zip(binding_responses.into_iter())
                    .zip(binding_models.into_iter())
                    .map(|((binding_request, binding_response), binding_model)| {
                        let capture::validate_request::Binding {
                            collection,
                            resource_spec_json,
                        } = binding_request;

                        let capture::validate_response::Binding { resource_path } =
                            binding_response;

                        (
                            &binding_model.scope,
                            flow::capture_spec::Binding {
                                resource_spec_json,
                                resource_path,
                                collection,
                            },
                        )
                    })
                    .collect();

                // Look for (and error on) duplicated resource paths within the bindings.
                for ((l_scope, _), (r_scope, binding)) in bindings
                    .iter()
                    .sorted_by(|(_, l), (_, r)| l.resource_path.cmp(&r.resource_path))
                    .tuple_windows()
                    .filter(|((_, l), (_, r))| l.resource_path == r.resource_path)
                {
                    Error::BindingDuplicatesResource {
                        entity: "capture",
                        name: name.to_string(),
                        resource: binding.resource_path.iter().join("."),
                        rhs_scope: (*r_scope).clone(),
                    }
                    .push(l_scope, errors);
                }

                // Unzip to strip scopes, leaving built bindings.
                let (_, bindings): (Vec<_>, Vec<_>) = bindings.into_iter().unzip();

                let spec = flow::CaptureSpec {
                    capture: name.clone(),
                    endpoint_type,
                    endpoint_spec_json,
                    bindings,
                    interval_seconds: *interval_seconds,
                };
                built_captures.push_row(scope, name, spec);
            }
            Err(err) => {
                Error::CaptureDriver {
                    name: request.capture,
                    detail: err,
                }
                .push(&capture.scope, errors);
            }
        }
    }

    built_captures
}

fn walk_capture_request<'a>(
    built_collections: &'a [tables::BuiltCollection],
    capture: &'a tables::Capture,
    capture_bindings: Vec<&'a tables::CaptureBinding>,
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    imports: &[&tables::Import],
    errors: &mut tables::Errors,
) -> Option<(
    &'a tables::Capture,
    Vec<&'a tables::CaptureBinding>,
    capture::ValidateRequest,
)> {
    let tables::Capture {
        scope: _,
        capture: name,
        endpoint_type,
        endpoint_spec,
        interval_seconds: _,
    } = capture;

    let (binding_models, binding_requests): (Vec<_>, Vec<_>) = capture_bindings
        .iter()
        .filter_map(|capture_binding| {
            walk_capture_binding(
                built_collections,
                capture_binding,
                collections,
                derivations,
                imports,
                errors,
            )
            .map(|binding_request| (*capture_binding, binding_request))
        })
        .unzip();

    let request = capture::ValidateRequest {
        capture: name.to_string(),
        bindings: binding_requests,
        endpoint_type: *endpoint_type as i32,
        endpoint_spec_json: endpoint_spec.to_string(),
    };

    Some((capture, binding_models, request))
}

fn walk_capture_binding<'a>(
    built_collections: &'a [tables::BuiltCollection],
    capture_binding: &tables::CaptureBinding,
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    imports: &[&tables::Import],
    errors: &mut tables::Errors,
) -> Option<capture::validate_request::Binding> {
    let tables::CaptureBinding {
        scope,
        capture: _,
        capture_index: _,
        resource_spec,
        collection,
    } = capture_binding;

    // We must resolve the target collection to continue.
    let target = reference::walk_reference(
        scope,
        "capture",
        "collection",
        collection,
        collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    )?;

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

    let request = capture::validate_request::Binding {
        resource_spec_json: resource_spec.to_string(),
        collection: Some(built_collection.spec.clone()),
    };

    Some(request)
}
