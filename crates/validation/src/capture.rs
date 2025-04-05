use super::{
    indexed, reference, storage_mapping, walk_transition, Connectors, Error, NoOpConnectors, Scope,
};
use futures::SinkExt;
use proto_flow::{capture, flow, ops::log::Level as LogLevel};
use std::collections::BTreeMap;
use tables::EitherOrBoth as EOB;

pub async fn walk_all_captures<C: Connectors>(
    pub_id: models::Id,
    build_id: models::Id,
    draft_captures: &tables::DraftCaptures,
    live_captures: &tables::LiveCaptures,
    built_collections: &tables::BuiltCollections,
    connectors: &C,
    data_planes: &tables::DataPlanes,
    default_plane_id: Option<models::Id>,
    dependencies: &tables::Dependencies<'_>,
    noop_captures: bool,
    storage_mappings: &tables::StorageMappings,
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
                dependencies,
                noop_captures,
                storage_mappings,
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

async fn walk_capture<C: Connectors>(
    pub_id: models::Id,
    build_id: models::Id,
    eob: EOB<&tables::LiveCapture, &tables::DraftCapture>,
    built_collections: &tables::BuiltCollections,
    connectors: &C,
    data_planes: &tables::DataPlanes,
    default_plane_id: Option<models::Id>,
    dependencies: &tables::Dependencies<'_>,
    noop_captures: bool,
    storage_mappings: &tables::StorageMappings,
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
        live_model,
        live_spec,
        is_touch,
    ) = match walk_transition(pub_id, build_id, default_plane_id, eob, errors) {
        Ok(ok) => ok,
        Err(built) => return Some(built),
    };
    let scope = Scope::new(scope);
    let mut model_fixes = Vec::new();

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
    // Resolve the data-plane for this task. We cannot continue without it.
    let data_plane =
        reference::walk_data_plane(scope, capture, data_plane_id, data_planes, errors)?;

    // Start an RPC with the task's connector.
    let (mut request_tx, request_rx) = futures::channel::mpsc::channel(1);
    let response_rx = if noop_captures || shards.disable {
        futures::future::Either::Left(NoOpConnectors.capture(data_plane, capture, request_rx))
    } else {
        futures::future::Either::Right(connectors.capture(data_plane, capture, request_rx))
    };
    futures::pin_mut!(response_rx);

    // Send Request.Spec and receive Response.Spec.
    _ = request_tx
        .send(
            capture::Request {
                spec: Some(capture::request::Spec {
                    connector_type,
                    config_json: config_json.clone(),
                }),
                ..Default::default()
            }
            .with_internal(|internal| {
                if let Some(s) = &shards.log_level {
                    internal.set_log_level(LogLevel::from_str_name(s).unwrap_or_default());
                }
            }),
        )
        .await;

    let capture::response::Spec {
        documentation_url: _,
        config_schema_json: _,
        resource_config_schema_json: _,
        resource_path_pointers,
        ..
    } = super::expect_response(
        scope,
        &mut response_rx,
        |response| Ok(response.spec.take()),
        errors,
    )
    .await?;

    let resource_path_pointers: Vec<doc::Pointer> = resource_path_pointers
        .into_iter()
        .map(|ptr| doc::Pointer::from_str(&ptr))
        .collect();

    // Index live binding models on their extracted resource paths.
    let live_bindings_model: BTreeMap<Vec<String>, &models::CaptureBinding> = live_model
        .iter()
        .flat_map(|model| model.bindings.iter())
        .filter_map(|model| {
            super::extract_resource_path(&resource_path_pointers, &model.resource)
                .ok() // Best effort.
                .map(|path| (path, model))
        })
        .collect();

    // Index live binding specs, both active and inactive, on their declared resource paths.
    let mut live_bindings_spec: BTreeMap<&[String], &flow::capture_spec::Binding> = live_spec
        .iter()
        .flat_map(|spec| spec.inactive_bindings.iter().chain(spec.bindings.iter()))
        .map(|binding| (binding.resource_path.as_slice(), binding))
        .collect();

    let scope_bindings = scope.push_prop("bindings");

    // Map enumerated binding models into paired validation requests.
    let bindings_model_len = bindings_model.len();
    let bindings: Vec<(
        models::CaptureBinding,
        models::ResourcePath,
        Option<capture::request::validate::Binding>,
    )> = bindings_model
        .into_iter()
        .enumerate()
        .map(|(index, model)| {
            walk_capture_binding(
                scope_bindings.push_item(index),
                model,
                built_collections,
                capture,
                noop_captures || shards.disable,
                &live_bindings_model,
                &mut live_bindings_spec,
                &mut model_fixes,
                &resource_path_pointers,
                errors,
            )
        })
        .collect();

    super::validate_resource_paths(
        scope,
        "capture",
        &capture,
        bindings.len(),
        |index| bindings[index].1.as_slice(),
        errors,
    );

    // Determine storage mappings for task recovery logs.
    let recovery_stores = storage_mapping::mapped_stores(
        scope,
        "capture",
        &format!("recovery/{capture}"),
        storage_mappings,
        errors,
    );

    // We've completed all cheap validation checks.
    // If we've already encountered errors then stop now.
    if !errors.is_empty() {
        return None;
    }

    // Filter to validation requests of active bindings.
    let bindings_validate: Vec<capture::request::validate::Binding> = bindings
        .iter()
        .filter_map(|(_model, _resource_path, validate)| validate.clone())
        .collect();
    let bindings_validate_len = bindings_validate.len();

    let validate_request = capture::request::Validate {
        name: capture.to_string(),
        connector_type,
        config_json: config_json.clone(),
        bindings: bindings_validate,
        last_capture: live_spec.cloned(),
        last_version: if expect_build_id.is_zero() {
            String::new()
        } else {
            expect_build_id.to_string()
        },
    };

    // Send Request.Validate and receive Response.Validated.
    _ = request_tx
        .send(
            capture::Request {
                validate: Some(validate_request.clone()),
                ..Default::default()
            }
            .with_internal(|internal| {
                if let Some(s) = &shards.log_level {
                    internal.set_log_level(LogLevel::from_str_name(s).unwrap_or_default());
                }
            }),
        )
        .await;

    let (validated_response, network_ports) = super::expect_response(
        scope,
        &mut response_rx,
        |response| {
            let network_ports = match response.get_internal() {
                Ok(internal) => internal.container.unwrap_or_default().network_ports,
                Err(err) => return Err(anyhow::anyhow!("parsing internal: {err}")),
            };
            Ok(response.validated.take().map(|v| (v, network_ports)))
        },
        errors,
    )
    .await?;

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
        |validated, (model, resource_path, validate)| {
            if let Some(validate) = validate {
                validated
                    .next()
                    .map(|validated| (model, resource_path, Some((validate, validated))))
            } else {
                Some((model, resource_path, None))
            }
        },
    );

    let mut bindings_model = Vec::with_capacity(bindings_model_len);
    let mut bindings_spec = Vec::with_capacity(bindings_validate_len);

    // Map Validate / Validated pairs into CaptureSpec::Bindings.
    for (index, (model, resource_path, validate_validated)) in bindings.into_iter().enumerate() {
        let Some((validate, validated)) = validate_validated else {
            bindings_model.push(model);
            continue;
        };
        let capture::request::validate::Binding {
            resource_config_json,
            collection,
            backfill,
        } = validate;

        let capture::response::validated::Binding {
            resource_path: validated_resource_path,
        } = validated;

        if resource_path != *validated_resource_path {
            Error::BindingWrongResourcePath {
                entity: "capture",
                computed: resource_path.clone(),
                validated: validated_resource_path.clone(),
            }
            .push(scope_bindings.push_item(index), errors);
        }

        let state_key = assemble::encode_state_key(&resource_path, backfill);

        let spec = flow::capture_spec::Binding {
            resource_config_json,
            resource_path,
            collection,
            backfill,
            state_key,
        };

        bindings_model.push(model);
        bindings_spec.push(spec);
    }

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

    // Any remaining live binding specs were not removed while walking bindings, and must be inactive.
    let inactive_bindings = live_bindings_spec.values().map(|v| (*v).clone()).collect();

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
        inactive_bindings,
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
        dependency_hash,
        expect_build_id,
        expect_pub_id,
        is_touch: is_touch && model_fixes.is_empty(),
        model: Some(model),
        model_fixes,
        previous_spec: live_spec.cloned(),
        spec: Some(spec),
        validated: Some(validated_response),
    })
}

fn walk_capture_binding<'a>(
    scope: Scope<'a>,
    mut model: models::CaptureBinding,
    built_collections: &'a tables::BuiltCollections,
    _catalog_name: &models::Capture,
    disable: bool,
    live_bindings_model: &BTreeMap<Vec<String>, &models::CaptureBinding>,
    live_bindings_spec: &mut BTreeMap<&[String], &flow::capture_spec::Binding>,
    model_fixes: &mut Vec<String>,
    resource_path_pointers: &[doc::Pointer],
    errors: &mut tables::Errors,
) -> (
    models::CaptureBinding,
    models::ResourcePath,
    Option<capture::request::validate::Binding>,
) {
    let resource_path = match super::extract_resource_path(resource_path_pointers, &model.resource)
    {
        Err(err) => {
            err.push(scope, errors);
            Vec::new()
        }
        Ok(path) => path,
    };

    if disable || model.disable {
        return (model, resource_path, None);
    }
    let live_model = live_bindings_model.get(&resource_path);
    let modified = Some(&&model) != live_model;

    let target = &model.target;

    // We must resolve the target collection to continue.
    let Some((target_spec, _built_collection)) = reference::walk_reference(
        scope,
        "this capture binding",
        target,
        built_collections,
        modified.then_some(errors),
    ) else {
        model_fixes.push(format!("disabled binding of deleted collection {target}"));
        model.disable = true;
        return (model, resource_path, None);
    };

    // Removal from `live_bindings_spec` is how we know to not include it in `inactive_bindings`.
    if let Some(live_spec) = live_bindings_spec.remove(resource_path.as_slice()) {
        if model.backfill == live_spec.backfill
            && super::collection_was_reset(&target_spec, &live_spec.collection)
        {
            model_fixes.push(format!("backfilled binding of reset collection {target}"));
            model.backfill += 1;
        }
    }

    let validate = capture::request::validate::Binding {
        resource_config_json: model.resource.to_string(),
        collection: Some(target_spec),
        backfill: model.backfill,
    };

    (model, resource_path, Some(validate))
}
