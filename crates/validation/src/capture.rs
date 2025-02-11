use super::{
    indexed, reference, storage_mapping, walk_transition, Connectors, Error, NoOpConnectors, Scope,
};
use proto_flow::{capture, flow, ops::log::Level as LogLevel};
use tables::EitherOrBoth as EOB;

pub async fn walk_all_captures(
    pub_id: models::Id,
    build_id: models::Id,
    draft_captures: &tables::DraftCaptures,
    live_captures: &tables::LiveCaptures,
    built_collections: &tables::BuiltCollections,
    connectors: &dyn Connectors,
    data_planes: &tables::DataPlanes,
    default_plane_id: Option<models::Id>,
    storage_mappings: &tables::StorageMappings,
    dependencies: &tables::Dependencies<'_>,
    errors: &mut tables::Errors,
) -> tables::BuiltCaptures {
    // Outer join of live and draft captures.
    let it =
        live_captures.outer_join(
            draft_captures.iter().map(|r| (&r.capture, r)),
            |eob| match eob {
                EOB::Left(live) => Some(EOB::Left(live)),
                EOB::Right((_capture, draft)) => Some(EOB::Right(draft)),
                EOB::Both(live, (_capture, draft)) => Some(EOB::Both(live, draft)),
            },
        );

    let futures: Vec<_> = it
        .map(|eob| async {
            let mut local_errors = tables::Errors::new();

            let built_capture = walk_capture(
                pub_id,
                build_id,
                eob,
                built_collections,
                connectors,
                data_planes,
                default_plane_id,
                storage_mappings,
                dependencies,
                &mut local_errors,
            )
            .await;

            (built_capture, local_errors)
        })
        .collect();

    // Evaluate all validations concurrently.
    let outcomes = futures::future::join_all(futures).await;

    outcomes
        .into_iter()
        .filter_map(|(built, local_errors)| {
            errors.extend(local_errors.into_iter());
            built
        })
        .collect()
}

async fn walk_capture(
    pub_id: models::Id,
    build_id: models::Id,
    eob: EOB<&tables::LiveCapture, &tables::DraftCapture>,
    built_collections: &tables::BuiltCollections,
    connectors: &dyn Connectors,
    data_planes: &tables::DataPlanes,
    default_plane_id: Option<models::Id>,
    storage_mappings: &tables::StorageMappings,
    dependencies: &tables::Dependencies<'_>,
    errors: &mut tables::Errors,
) -> Option<tables::BuiltCapture> {
    let (
        capture,
        scope,
        model,
        control_id,
        data_plane_id,
        expect_pub_id,
        expect_build_id,
        _live_model,
        live_spec,
        is_touch,
    ) = match walk_transition(pub_id, build_id, default_plane_id, eob, errors) {
        Ok(ok) => ok,
        Err(built) => return Some(built),
    };
    let scope = Scope::new(scope);

    let dependency_hash = dependencies.compute_hash(&model);
    let models::CaptureDef {
        auto_discover,
        endpoint,
        bindings: bindings_model,
        interval,
        shards,
        expect_pub_id: _,
        delete: _,
    } = model;

    indexed::walk_name(scope, "capture", capture, models::Capture::regex(), errors);

    // Unwrap `endpoint` into a connector type and configuration.
    let (connector_type, config_json) = match &endpoint {
        models::CaptureEndpoint::Connector(config) => (
            flow::capture_spec::ConnectorType::Image as i32,
            serde_json::to_string(config).unwrap(),
        ),
        models::CaptureEndpoint::Local(config) => (
            flow::capture_spec::ConnectorType::Local as i32,
            serde_json::to_string(config).unwrap(),
        ),
    };

    // Map enumerated binding models into paired validation requests.
    let bindings_model_len = bindings_model.len();
    let bindings: Vec<(
        usize,
        models::CaptureBinding,
        Option<capture::request::validate::Binding>,
    )> = bindings_model
        .into_iter()
        .enumerate()
        .filter_map(|(index, model)| {
            let (model, validate) = walk_capture_binding(
                scope.push_prop("bindings").push_item(index),
                model,
                built_collections,
                errors,
            )?;
            Some((index, model, validate))
        })
        .collect();

    // Determine storage mappings for task recovery logs.
    let recovery_stores = storage_mapping::mapped_stores(
        scope,
        "capture",
        &format!("recovery/{capture}"),
        storage_mappings,
        errors,
    );

    // Resolve the data-plane for this task. We cannot continue without it.
    let data_plane =
        reference::walk_data_plane(scope, capture, data_plane_id, data_planes, errors)?;

    // We've completed all cheap validation checks.
    // If we've already encountered errors then stop now.
    if !errors.is_empty() {
        return None;
    }

    let bindings_validate: Vec<_> = bindings
        .iter()
        .filter_map(|(_index, _model, validate)| validate.clone())
        .collect();
    let bindings_validate_len = bindings_validate.len();

    let validate_request = capture::request::Validate {
        name: capture.to_string(),
        connector_type,
        config_json: config_json.clone(),
        bindings: bindings_validate,
        last_capture: live_spec.cloned(),
        last_version: if expect_pub_id.is_zero() {
            String::new()
        } else {
            expect_pub_id.to_string()
        },
    };
    let wrapped_request = capture::Request {
        validate: Some(validate_request),
        ..Default::default()
    }
    .with_internal(|internal| {
        if let Some(s) = &shards.log_level {
            internal.set_log_level(LogLevel::from_str_name(s).unwrap_or_default());
        }
    });

    // If shards are disabled, then don't ask the connector to validate.
    let response = if shards.disable {
        NoOpConnectors.validate_capture(wrapped_request, data_plane)
    } else {
        connectors.validate_capture(wrapped_request, data_plane)
    }
    .await;

    // Unwrap `response` and bail out if it failed.
    let (validated_response, network_ports) = match extract_validated(response) {
        Err(err) => {
            err.push(scope, errors);
            return None;
        }
        Ok(ok) => ok,
    };

    let capture::response::Validated {
        bindings: bindings_validated,
    } = &validated_response;

    if bindings_validate_len != bindings_validated.len() {
        Error::WrongConnectorBindings {
            expect: bindings_validate_len,
            got: bindings_validated.len(),
        }
        .push(scope, errors);
    }

    // Join binding models and their Validate requests with their Validated responses.
    let bindings = bindings.into_iter().scan(
        bindings_validated.into_iter(),
        |validated, (index, model, validate)| {
            if let Some(validate) = validate {
                validated
                    .next()
                    .map(|validated| (index, model, Some((validate, validated))))
            } else {
                Some((index, model, None))
            }
        },
    );

    let mut bindings_index = Vec::<(usize, usize)>::with_capacity(bindings_validate_len);
    let mut bindings_model = Vec::with_capacity(bindings_model_len);
    let mut bindings_spec = Vec::with_capacity(bindings_validate_len);

    for (index, model, validate_validated) in bindings {
        let Some((validate, validated)) = validate_validated else {
            bindings_model.push(model);
            continue;
        };
        let capture::request::validate::Binding {
            resource_config_json,
            collection,
            backfill,
        } = validate;

        let capture::response::validated::Binding { resource_path } = validated;

        let state_key = assemble::encode_state_key(resource_path, backfill);

        let spec = flow::capture_spec::Binding {
            resource_config_json,
            resource_path: resource_path.clone(),
            collection,
            backfill,
            state_key,
        };

        bindings_index.push((bindings_spec.len(), index));
        bindings_model.push(model);
        bindings_spec.push(spec);
    }

    super::validate_resource_paths(
        scope,
        "capture",
        &capture,
        bindings_index,
        |index| bindings_spec[index].resource_path.as_slice(),
        errors,
    );

    // Pluck out the current shard ID prefix, or create a unique one if it doesn't exist.
    let shard_id_prefix = if let Some(flow::CaptureSpec {
        shard_template: Some(shard_template),
        ..
    }) = live_spec
    {
        shard_template.id.clone()
    } else {
        assemble::shard_id_prefix(pub_id, &capture, labels::TASK_TYPE_CAPTURE)
    };

    let recovery_log_template = assemble::recovery_log_template(
        build_id,
        &capture,
        labels::TASK_TYPE_CAPTURE,
        &shard_id_prefix,
        recovery_stores,
    );
    let shard_template = assemble::shard_template(
        build_id,
        &capture,
        labels::TASK_TYPE_CAPTURE,
        &shards,
        &shard_id_prefix,
        false, // Don't disable wait_for_ack.
        &network_ports,
    );
    let spec = flow::CaptureSpec {
        name: capture.to_string(),
        connector_type,
        config_json,
        bindings: bindings_spec,
        interval_seconds: interval.as_secs() as u32,
        recovery_log_template: Some(recovery_log_template),
        shard_template: Some(shard_template),
        network_ports,
    };
    let model = models::CaptureDef {
        auto_discover,
        endpoint,
        bindings: bindings_model,
        interval,
        shards,
        expect_pub_id: None,
        delete: false,
    };

    Some(tables::BuiltCapture {
        capture: capture.clone(),
        scope: scope.flatten(),
        control_id,
        data_plane_id,
        expect_pub_id,
        expect_build_id,
        model: Some(model),
        validated: Some(validated_response),
        spec: Some(spec),
        previous_spec: live_spec.cloned(),
        is_touch,
        dependency_hash,
    })
}

fn walk_capture_binding<'a>(
    scope: Scope<'a>,
    model: models::CaptureBinding,
    built_collections: &'a tables::BuiltCollections,
    errors: &mut tables::Errors,
) -> Option<(
    models::CaptureBinding,
    Option<capture::request::validate::Binding>,
)> {
    if model.disable {
        return Some((model, None)); // Retain but perform no further validation.
    }

    // We must resolve the target collection to continue.
    let (spec, _) = reference::walk_reference(
        scope,
        "this capture binding",
        &model.target,
        built_collections,
        errors,
    )?;

    let request = capture::request::validate::Binding {
        resource_config_json: model.resource.to_string(),
        collection: Some(spec),
        backfill: model.backfill,
    };

    Some((model, Some(request)))
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
