use super::{indexed, reference, storage_mapping, Drivers, Error, NoOpDrivers};
use futures::FutureExt;
use itertools::{EitherOrBoth, Itertools};
use proto_flow::{capture, flow};

pub async fn walk_all_captures<D: Drivers>(
    build_config: &flow::build_api::Config,
    drivers: &D,
    built_collections: &[tables::BuiltCollection],
    capture_bindings: &[tables::CaptureBinding],
    captures: &[tables::Capture],
    resources: &[tables::Resource],
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) -> tables::BuiltCaptures {
    let mut validations = Vec::new();

    // Group |capture_bindings| on bindings having the same capture.
    let capture_bindings = capture_bindings.iter().group_by(|c| &c.capture);

    // Walk ordered captures, left-joined by their bindings.
    for (capture, bindings) in captures
        .iter()
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
            models::Capture::regex(),
            &mut capture_errors,
        );

        let validation = walk_capture_request(
            built_collections,
            capture,
            bindings.into_iter().flatten().collect_vec(),
            resources,
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
                // If shards are disabled, then don't ask the connector to validate. Users may
                // disable captures in response to the source system being unreachable, and we
                // wouldn't want a validation error for a disabled task to terminate the build.
                if capture.spec.shards.disable {
                    NoOpDrivers {}.validate_capture(request.clone())
                } else {
                    drivers.validate_capture(request.clone())
                }
                .map(|response| (capture, binding_models, request, response))
                .await
            });

    let validations: Vec<(
        &tables::Capture,
        Vec<&tables::CaptureBinding>,
        proto_flow::capture::ValidateRequest,
        anyhow::Result<proto_flow::capture::ValidateResponse>,
    )> = futures::future::join_all(validations).await;

    let mut built_captures = tables::BuiltCaptures::new();

    for (capture, binding_models, request, response) in validations {
        // Unwrap |response| and continue if an Err.
        let response = match response {
            Err(err) => {
                Error::CaptureDriver {
                    name: request.capture,
                    detail: err,
                }
                .push(&capture.scope, errors);

                continue;
            }
            Ok(response) => response,
        };

        let tables::Capture {
            scope,
            spec: models::CaptureDef {
                interval, shards, ..
            },
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
            .push(&scope, errors);
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

                let capture::validate_response::Binding { resource_path } = binding_response;

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

        let recovery_stores = storage_mapping::mapped_stores(
            scope,
            "capture",
            &format!("recovery/{}", name.as_str()),
            storage_mappings,
            errors,
        );

        let spec = flow::CaptureSpec {
            capture: name.clone(),
            endpoint_type,
            endpoint_spec_json,
            bindings,
            interval_seconds: interval.as_secs() as u32,
            recovery_log_template: Some(assemble::recovery_log_template(
                build_config,
                &name,
                labels::TASK_TYPE_CAPTURE,
                recovery_stores,
            )),
            shard_template: Some(assemble::shard_template(
                build_config,
                &name,
                labels::TASK_TYPE_CAPTURE,
                &shards,
                false, // Don't disable wait_for_ack.
            )),
        };
        built_captures.insert_row(scope, name, spec);
    }

    built_captures
}

fn walk_capture_request<'a>(
    built_collections: &'a [tables::BuiltCollection],
    capture: &'a tables::Capture,
    capture_bindings: Vec<&'a tables::CaptureBinding>,
    resources: &[tables::Resource],
    errors: &mut tables::Errors,
) -> Option<(
    &'a tables::Capture,
    Vec<&'a tables::CaptureBinding>,
    capture::ValidateRequest,
)> {
    let tables::Capture {
        scope: _,
        capture: name,
        spec: models::CaptureDef { endpoint, .. },
        endpoint_config,
    } = capture;

    let (binding_models, binding_requests): (Vec<_>, Vec<_>) = capture_bindings
        .iter()
        .filter_map(|capture_binding| {
            walk_capture_binding(built_collections, capture_binding, errors)
                .map(|binding_request| (*capture_binding, binding_request))
        })
        .unzip();

    let endpoint_spec_json = match endpoint {
        models::CaptureEndpoint::Connector(models::ConnectorConfig { image, config }) => {
            let config = match endpoint_config
                .as_ref()
                .and_then(|url| tables::Resource::fetch_content_dom(resources, url))
            {
                Some(external) => external.to_owned(),
                None => config.to_owned(),
            };

            serde_json::to_string(&models::ConnectorConfig {
                image: image.to_owned(),
                config,
            })
            .unwrap()
        }
        models::CaptureEndpoint::Ingest(ingest) => serde_json::to_string(ingest).unwrap(),
    };

    let request = capture::ValidateRequest {
        capture: name.to_string(),
        bindings: binding_requests,
        endpoint_type: assemble::capture_endpoint_type(endpoint) as i32,
        endpoint_spec_json,
    };

    Some((capture, binding_models, request))
}

fn walk_capture_binding<'a>(
    built_collections: &'a [tables::BuiltCollection],
    capture_binding: &tables::CaptureBinding,
    errors: &mut tables::Errors,
) -> Option<capture::validate_request::Binding> {
    let tables::CaptureBinding {
        scope,
        capture: _,
        capture_index: _,
        spec:
            models::CaptureBinding {
                resource,
                target: collection,
            },
    } = capture_binding;

    // We must resolve the target collection to continue.
    let built_collection = reference::walk_reference(
        scope,
        "capture",
        "collection",
        collection,
        built_collections,
        |c| (&c.collection, &c.scope),
        errors,
    )?;

    let request = capture::validate_request::Binding {
        resource_spec_json: serde_json::to_string(&resource).unwrap(),
        collection: Some(built_collection.spec.clone()),
    };

    Some(request)
}
