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

    let models::CaptureDef {
        auto_discover,
        endpoint,
        bindings: bindings_model,
        interval,
        redact_salt: model_redact_salt,
        shards,
        expect_pub_id: _,
        delete: _,
    } = model;

    indexed::walk_name(scope, "capture", capture, models::Capture::regex(), errors);

    // Unwrap `endpoint` into a connector type and configuration.
    let (connector_type, config_json): (i32, bytes::Bytes) = match &endpoint {
        models::CaptureEndpoint::Connector(config) => (
            flow::capture_spec::ConnectorType::Image as i32,
            serde_json::to_string(config).unwrap().into(),
        ),
        models::CaptureEndpoint::Local(config) => (
            flow::capture_spec::ConnectorType::Local as i32,
            serde_json::to_string(config).unwrap().into(),
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
        resource_path_pointers: _,
        ..
    } = super::expect_response(
        scope,
        &mut response_rx,
        |response| Ok(response.spec.take()),
        errors,
    )
    .await?;

    // Index live binding models having a non-empty resource /_meta/path .
    let live_bindings_model: BTreeMap<Vec<String>, &models::CaptureBinding> = live_model
        .iter()
        .flat_map(|model| model.bindings.iter())
        .filter_map(|model| {
            let model_path = super::load_resource_meta_path(model.resource.get().as_bytes());
            (!model_path.is_empty()).then_some((model_path, model))
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
        models::ResourcePath,
        models::CaptureBinding,
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
                &live_bindings_spec,
                &mut model_fixes,
                errors,
            )
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

    // We've completed all cheap validation checks.
    // If we've already encountered errors then stop now.
    if !errors.is_empty() {
        return None;
    }

    // Filter to validation requests of active bindings.
    let bindings_validate: Vec<capture::request::validate::Binding> = bindings
        .iter()
        .filter_map(|(_path, _model, validate)| validate.clone())
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
        |validated, (path, model, validate)| {
            if let Some(validate) = validate {
                validated
                    .next()
                    .map(|validated| (path, model, Some((validate, validated))))
            } else {
                Some((path, model, None))
            }
        },
    );

    let mut bindings_path = Vec::with_capacity(bindings_model_len);
    let mut bindings_model = Vec::with_capacity(bindings_model_len);
    let mut bindings_spec = Vec::with_capacity(bindings_validate_len);
    let mut n_meta_updated = 0;

    // Map `bindings` into destructured binding models and built specs.
    for (index, (mut path, mut model, validate_validated)) in bindings.into_iter().enumerate() {
        let Some((validate, validated)) = validate_validated else {
            bindings_path.push(path);
            bindings_model.push(model);
            continue;
        };
        let scope = scope_bindings.push_item(index);

        let capture::request::validate::Binding {
            resource_config_json,
            collection,
            backfill: _, // Same as `model.backfill`.
        } = validate;

        let capture::response::validated::Binding {
            resource_path: validated_path,
        } = validated;

        if validated_path.is_empty() {
            Error::BindingMissingResourcePath { entity: "capture" }.push(scope, errors);
        } else if path != *validated_path {
            path = validated_path.clone();
            model.resource = super::store_resource_meta(&model.resource, &path);
            n_meta_updated += 1;
        }

        // Map to the live binding now that we have a validated resource path.
        let live_spec: Option<&flow::capture_spec::Binding> =
            live_bindings_spec.get(path.as_slice()).cloned();

        if let Some(live_spec) = live_spec {
            if model.backfill < live_spec.backfill {
                model_fixes.push(format!("restored `backfill` of resource {path:?}"));
                model.backfill = live_spec.backfill;
            }
        }

        // Build a state key using the validated resource path.
        let state_key = assemble::encode_state_key(&path, model.backfill);

        let spec = flow::capture_spec::Binding {
            resource_config_json,
            resource_path: path.clone(),
            collection,
            backfill: model.backfill,
            state_key,
        };

        bindings_path.push(path);
        bindings_model.push(model);
        bindings_spec.push(spec);
    }

    if n_meta_updated != 0 {
        model_fixes.push(format!(
            "updated resource /_meta of {n_meta_updated} bindings"
        ));
    }

    super::validate_resource_paths(
        scope,
        "capture",
        bindings_path.len(),
        |index| &bindings_path[index],
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

    // Remove built bindings from `live_bindings_spec`. The remainder must be inactive.
    for binding in &bindings_spec {
        live_bindings_spec.remove(binding.resource_path.as_slice());
    }
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
        redact_salt: bytes::Bytes::new(),
    };
    let model = models::CaptureDef {
        auto_discover,
        endpoint,
        bindings: bindings_model,
        interval,
        redact_salt: model_redact_salt,
        shards,
        expect_pub_id: None,
        delete: false,
    };

    std::mem::drop(request_tx);
    () = super::expect_eof(scope, response_rx, errors).await;

    // Compute the dependency hash, now that we're done with any modifications of the model
    let dependency_hash = dependencies.compute_hash(&model);
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
    live_bindings_spec: &BTreeMap<&[String], &flow::capture_spec::Binding>,
    model_fixes: &mut Vec<String>,
    errors: &mut tables::Errors,
) -> (
    models::ResourcePath,
    models::CaptureBinding,
    Option<capture::request::validate::Binding>,
) {
    let model_path = super::load_resource_meta_path(model.resource.get().as_bytes());

    if model.disable {
        // A disabled binding may reference a non-extant collection.
        return (model_path, model, None);
    }

    let live_model = live_bindings_model.get(&model_path);
    let modified_target = Some(&model.target) != live_model.map(|l| &l.target);
    let target = &model.target;

    // We must resolve the target collection to continue.
    let Some((target_spec, _built_collection)) = reference::walk_reference(
        scope,
        "this capture binding",
        target,
        built_collections,
        modified_target.then_some(errors),
    ) else {
        model_fixes.push(format!("disabled binding of deleted collection {target}"));
        model.disable = true;
        return (model_path, model, None);
    };

    if disable {
        // Perform no further validations if the task is disabled.
        return (model_path, model, None);
    }

    // Was this binding's target collection reset under its current backfill count?
    let live_spec = live_bindings_spec.get(model_path.as_slice());
    let was_reset = live_spec.is_some_and(|live_spec| {
        live_spec.backfill == model.backfill
            && super::collection_was_reset(&target_spec, &live_spec.collection)
    });

    if was_reset {
        model_fixes.push(format!("backfilled binding of reset collection {target}"));
        model.backfill += 1;
    }

    let validate = capture::request::validate::Binding {
        resource_config_json: super::strip_resource_meta(&model.resource),
        collection: Some(target_spec),
        backfill: model.backfill,
    };

    (model_path, model, Some(validate))
}
