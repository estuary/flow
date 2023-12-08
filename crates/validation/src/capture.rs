use super::{indexed, reference, storage_mapping, Connectors, Error, NoOpConnectors, Scope};
use itertools::Itertools;
use proto_flow::{capture, flow, ops::log::Level as LogLevel};

pub async fn walk_all_captures(
    build_id: &str,
    built_collections: &[tables::BuiltCollection],
    captures: &[tables::Capture],
    connectors: &dyn Connectors,
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) -> tables::BuiltCaptures {
    let mut validations = Vec::new();

    for capture in captures {
        let mut capture_errors = tables::Errors::new();
        let validation = walk_capture_request(built_collections, capture, &mut capture_errors);

        // Skip validation if errors were encountered while building the request.
        if !capture_errors.is_empty() {
            errors.extend(capture_errors.into_iter());
        } else if let Some(validation) = validation {
            validations.push(validation);
        }
    }

    // Run all validations concurrently.
    let validations = validations
        .into_iter()
        .map(|(capture, request)| async move {
            let mut wrapped = capture::Request {
                validate: Some(request.clone()),
                ..Default::default()
            };

            if let Some(log_level) = capture
                .spec
                .shards
                .log_level
                .as_ref()
                .and_then(|s| LogLevel::from_str_name(s))
            {
                wrapped.set_internal_log_level(log_level);
            }

            // If shards are disabled, then don't ask the connector to validate.
            // A broken but disabled endpoint should not cause a build to fail.
            let response = if capture.spec.shards.disable {
                NoOpConnectors.validate_capture(wrapped)
            } else {
                connectors.validate_capture(wrapped)
            };
            (capture, request, response.await)
        });

    let validations: Vec<(
        &tables::Capture,
        capture::request::Validate,
        anyhow::Result<capture::Response>,
    )> = futures::future::join_all(validations).await;

    let mut built_captures = tables::BuiltCaptures::new();

    for (capture, mut request, response) in validations {
        let tables::Capture {
            scope,
            capture: _,
            spec: models::CaptureDef {
                interval, shards, ..
            },
        } = capture;
        let scope = Scope::new(scope);

        // Unwrap `response` and bail out if it failed.
        let (validated, network_ports) = match extract_validated(response) {
            Err(err) => {
                err.push(scope, errors);
                continue;
            }
            Ok(ok) => ok,
        };

        let capture::request::Validate {
            connector_type,
            config_json,
            bindings: binding_requests,
            name,
            last_capture: _,
            last_version: _,
        } = &mut request;

        let capture::response::Validated {
            bindings: binding_responses,
        } = &validated;

        if binding_requests.len() != binding_responses.len() {
            Error::WrongConnectorBindings {
                expect: binding_requests.len(),
                got: binding_responses.len(),
            }
            .push(scope, errors);
        }

        // Join requests and responses to produce tuples
        // of (binding index, built binding).
        let built_bindings: Vec<_> = std::mem::take(binding_requests)
            .into_iter()
            .zip(binding_responses.into_iter())
            .enumerate()
            .map(|(binding_index, (binding_request, binding_response))| {
                let capture::request::validate::Binding {
                    resource_config_json,
                    collection,
                    backfill,
                } = binding_request;

                let capture::response::validated::Binding { resource_path } = binding_response;

                let state_key = assemble::encode_state_key(resource_path, backfill);

                (
                    binding_index,
                    flow::capture_spec::Binding {
                        resource_config_json,
                        resource_path: resource_path.clone(),
                        collection,
                        backfill,
                        state_key,
                    },
                )
            })
            .collect();

        // Look for (and error on) duplicated resource paths within the bindings.
        for ((l_index, _), (r_index, binding)) in built_bindings
            .iter()
            .sorted_by(|(_, l), (_, r)| l.resource_path.cmp(&r.resource_path))
            .tuple_windows()
            .filter(|((_, l), (_, r))| l.resource_path == r.resource_path)
        {
            let scope = scope.push_prop("bindings");
            let lhs_scope = scope.push_item(*l_index);
            let rhs_scope = scope.push_item(*r_index).flatten();

            Error::BindingDuplicatesResource {
                entity: "capture",
                name: name.to_string(),
                resource: binding.resource_path.iter().join("."),
                rhs_scope,
            }
            .push(lhs_scope, errors);
        }

        // Unzip to strip binding indices, leaving built bindings.
        let (_, built_bindings): (Vec<_>, Vec<_>) = built_bindings.into_iter().unzip();

        let recovery_stores = storage_mapping::mapped_stores(
            scope,
            "capture",
            &format!("recovery/{}", name.as_str()),
            storage_mappings,
            errors,
        );

        let spec = flow::CaptureSpec {
            name: name.clone(),
            connector_type: *connector_type,
            config_json: std::mem::take(config_json),
            bindings: built_bindings,
            interval_seconds: interval.as_secs() as u32,
            recovery_log_template: Some(assemble::recovery_log_template(
                build_id,
                &name,
                labels::TASK_TYPE_CAPTURE,
                recovery_stores,
            )),
            shard_template: Some(assemble::shard_template(
                build_id,
                &name,
                labels::TASK_TYPE_CAPTURE,
                &shards,
                false, // Don't disable wait_for_ack.
                &network_ports,
            )),
            network_ports,
        };
        built_captures.insert_row(scope.flatten(), std::mem::take(name), validated, spec);
    }

    built_captures
}

fn walk_capture_request<'a>(
    built_collections: &'a [tables::BuiltCollection],
    capture: &'a tables::Capture,
    errors: &mut tables::Errors,
) -> Option<(&'a tables::Capture, capture::request::Validate)> {
    let tables::Capture {
        scope,
        capture: name,
        spec: models::CaptureDef {
            endpoint, bindings, ..
        },
    } = capture;
    let scope = Scope::new(scope);

    // Require the capture name is valid.
    indexed::walk_name(
        scope,
        "capture",
        &capture.capture,
        models::Capture::regex(),
        errors,
    );

    let (connector_type, config_json) = match endpoint {
        models::CaptureEndpoint::Connector(config) => (
            flow::capture_spec::ConnectorType::Image as i32,
            serde_json::to_string(config).unwrap(),
        ),
        models::CaptureEndpoint::Local(config) => (
            flow::capture_spec::ConnectorType::Local as i32,
            serde_json::to_string(config).unwrap(),
        ),
    };

    let bindings = bindings
        .iter()
        .enumerate()
        // Disabled bindings get skipped. We don't send them in the Validate request, and we also
        // don't do any validation of the target name. By implication, the target of a disabled binding
        // can be any catalog name, even if it doesn't exist or is otherwise "wrong".
        .filter(|(_, b)| !b.disable)
        .map(|(binding_index, binding)| {
            walk_capture_binding(
                scope.push_prop("bindings").push_item(binding_index),
                binding,
                built_collections,
                errors,
            )
        })
        // Force eager evaluation of all results.
        .collect::<Vec<Option<_>>>()
        .into_iter()
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .collect();

    let request = capture::request::Validate {
        name: name.to_string(),
        connector_type,
        config_json,
        bindings,
        // TODO(johnny): Thread these through.
        last_capture: None,
        last_version: String::new(),
    };

    Some((capture, request))
}

fn walk_capture_binding<'a>(
    scope: Scope,
    binding: &'a models::CaptureBinding,
    built_collections: &'a [tables::BuiltCollection],
    errors: &mut tables::Errors,
) -> Option<capture::request::validate::Binding> {
    let models::CaptureBinding {
        resource,
        target,
        disable: _,
        backfill,
    } = binding;

    // We must resolve the target collection to continue.
    let built_collection = reference::walk_reference(
        scope,
        "this capture binding",
        "collection",
        target,
        built_collections,
        |c| (&c.collection, Scope::new(&c.scope)),
        errors,
    )?;

    let request = capture::request::validate::Binding {
        resource_config_json: resource.to_string(),
        collection: Some(built_collection.spec.clone()),
        backfill: *backfill,
    };

    Some(request)
}

fn extract_validated(
    response: anyhow::Result<capture::Response>,
) -> Result<(capture::response::Validated, Vec<flow::NetworkPort>), Error> {
    let response = match response {
        Ok(response) => response,
        Err(err) => return Err(Error::Connector { detail: err }),
    };

    let internal = match response.get_internal() {
        Ok(internal) => internal,
        Err(err) => {
            return Err(Error::Connector {
                detail: anyhow::anyhow!("parsing internal: {err}"),
            });
        }
    };

    let Some(validated) = response.validated else {
        return Err(Error::Connector {
            detail: anyhow::anyhow!(
                "expected Validated but got {}",
                serde_json::to_string(&response).unwrap()
            ),
        });
    };
    let network_ports = internal.container.unwrap_or_default().network_ports;

    Ok((validated, network_ports))
}
