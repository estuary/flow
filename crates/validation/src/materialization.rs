use super::{
    Connectors, Error, NoOpConnectors, Scope, collection, field_selection, indexed, reference,
    storage_mapping, walk_transition,
};
use futures::SinkExt;
use itertools::Itertools;
use json::schema::types;
use proto_flow::{flow, materialize, ops::log::Level as LogLevel};
use std::collections::BTreeMap;
use tables::EitherOrBoth as EOB;

pub async fn walk_all_materializations<C: Connectors>(
    pub_id: models::Id,
    build_id: models::Id,
    draft_materializations: &tables::DraftMaterializations,
    live_materializations: &tables::LiveMaterializations,
    built_collections: &tables::BuiltCollections,
    connectors: &C,
    data_planes: &tables::DataPlanes,
    default_plane_id: Option<models::Id>,
    dependencies: &tables::Dependencies<'_>,
    noop_materializations: bool,
    storage_mappings: &tables::StorageMappings,
    errors: &mut tables::Errors,
) -> tables::BuiltMaterializations {
    // Outer join of live and draft materializations.
    let it = live_materializations.outer_join(
        draft_materializations
            .iter()
            .map(|r| (&r.materialization, r)),
        |eob| match eob {
            EOB::Left(live) => Some(EOB::Left(live)),
            EOB::Right((_materialization, draft)) => Some(EOB::Right(draft)),
            EOB::Both(live, (_materialization, draft)) => Some(EOB::Both(live, draft)),
        },
    );

    let futures: Vec<_> = it
        .map(|eob| async {
            let mut local_errors = tables::Errors::new();

            let built_materialization = walk_materialization(
                pub_id,
                build_id,
                eob,
                built_collections,
                connectors,
                data_planes,
                default_plane_id,
                dependencies,
                noop_materializations,
                storage_mappings,
                &mut local_errors,
            )
            .await;

            (built_materialization, local_errors)
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

async fn walk_materialization<C: Connectors>(
    pub_id: models::Id,
    build_id: models::Id,
    eob: EOB<&tables::LiveMaterialization, &tables::DraftMaterialization>,
    built_collections: &tables::BuiltCollections,
    connectors: &C,
    data_planes: &tables::DataPlanes,
    default_plane_id: Option<models::Id>,
    dependencies: &tables::Dependencies<'_>,
    noop_materializations: bool,
    storage_mappings: &tables::StorageMappings,
    errors: &mut tables::Errors,
) -> Option<tables::BuiltMaterialization> {
    let (
        materialization,
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

    let models::MaterializationDef {
        on_incompatible_schema_change,
        source: sources,
        endpoint,
        bindings: bindings_model,
        mut shards,
        expect_pub_id: _,
        delete: _,
    } = model;

    indexed::walk_name(
        scope,
        "materialization",
        materialization,
        models::Materialization::regex(),
        errors,
    );

    // Unwrap `endpoint` into a connector type and configuration.
    let (connector_type, config_json) = match &endpoint {
        models::MaterializationEndpoint::Connector(config) => (
            flow::materialization_spec::ConnectorType::Image as i32,
            serde_json::to_string(config).unwrap(),
        ),
        models::MaterializationEndpoint::Local(config) => (
            flow::materialization_spec::ConnectorType::Local as i32,
            serde_json::to_string(config).unwrap(),
        ),
        models::MaterializationEndpoint::Dekaf(config) => (
            flow::materialization_spec::ConnectorType::Dekaf as i32,
            serde_json::to_string(config).unwrap(),
        ),
    };
    // Resolve the data-plane for this task. We cannot continue without it.
    let data_plane =
        reference::walk_data_plane(scope, materialization, data_plane_id, data_planes, errors)?;

    // Start an RPC with the task's connector.
    let (mut request_tx, request_rx) = futures::channel::mpsc::channel(1);
    let response_rx = if noop_materializations || shards.disable {
        futures::future::Either::Left(NoOpConnectors.materialize(
            data_plane,
            materialization,
            request_rx,
        ))
    } else {
        futures::future::Either::Right(connectors.materialize(
            data_plane,
            materialization,
            request_rx,
        ))
    };
    futures::pin_mut!(response_rx);

    // Send Request.Spec and receive Response.Spec.
    _ = request_tx
        .send(
            materialize::Request {
                spec: Some(materialize::request::Spec {
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

    let materialize::response::Spec {
        documentation_url: _,
        config_schema_json: _,
        resource_config_schema_json: _,
        ..
    } = super::expect_response(
        scope,
        &mut response_rx,
        |response| Ok(response.spec.take()),
        errors,
    )
    .await?;

    // Index live binding models on their (non-empty) resource /_meta/path .
    let live_bindings_model: BTreeMap<Vec<String>, &models::MaterializationBinding> = live_model
        .iter()
        .flat_map(|model| model.bindings.iter())
        .filter_map(|model| {
            let model_path = super::load_resource_meta_path(model.resource.get());
            (!model_path.is_empty()).then_some((model_path, model))
        })
        .collect();

    // Index live binding specs, both active and inactive, on their declared resource paths.
    let mut live_bindings_spec: BTreeMap<&[String], &flow::materialization_spec::Binding> =
        live_spec
            .iter()
            .flat_map(|spec| spec.inactive_bindings.iter().chain(spec.bindings.iter()))
            .map(|binding| (binding.resource_path.as_slice(), binding))
            .collect();

    let scope_bindings = scope.push_prop("bindings");

    // Map enumerated binding models into paired validation requests.
    let bindings_model_len = bindings_model.len();
    let bindings: Vec<(
        models::ResourcePath,
        models::MaterializationBinding,
        bool,
        Option<materialize::request::validate::Binding>,
    )> = bindings_model
        .into_iter()
        .enumerate()
        .map(|(index, model)| {
            walk_materialization_binding(
                scope_bindings.push_item(index),
                on_incompatible_schema_change,
                model,
                built_collections,
                materialization,
                data_plane_id,
                noop_materializations || shards.disable,
                &live_bindings_model,
                &live_bindings_spec,
                &mut model_fixes,
                errors,
            )
        })
        .collect();

    // Do we need to disable the whole task due to a reset source collection?
    if let Some((_, model_binding, _, _)) = bindings
        .iter()
        .find(|(_, _, disable_task, _)| *disable_task)
    {
        model_fixes.push(format!(
            "disabling materialization due to reset of collection {} (onIncompatibleSchemaChange: disableTask)",
            model_binding.source.collection(),
        ));
        shards.disable = true;
    }

    // Determine storage mappings for task recovery logs.
    let recovery_stores = storage_mapping::mapped_stores(
        scope,
        "materialization",
        &format!("recovery/{materialization}"),
        storage_mappings,
        errors,
    );

    // We've completed all cheap validation checks.
    // If we've already encountered errors then stop now.
    if !errors.is_empty() {
        return None;
    }

    // Filter to validation requests of active bindings.
    let bindings_validate: Vec<materialize::request::validate::Binding> = bindings
        .iter()
        .filter_map(|(_path, _model, _disable_task, validate)| {
            // TODO(johnny): Switch back to `validate.clone()` once connectors expect `Validate.group_by`.
            if let Some(mut validate) = validate.clone() {
                validate.group_by.clear();
                Some(validate)
            } else {
                None
            }
        })
        .collect();
    let bindings_validate_len = bindings_validate.len();

    let validate_request = materialize::request::Validate {
        name: materialization.to_string(),
        connector_type,
        config_json: config_json.clone(),
        bindings: bindings_validate,
        last_materialization: live_spec.cloned(),
        last_version: if expect_build_id.is_zero() {
            String::new()
        } else {
            expect_build_id.to_string()
        },
    };

    // Send Request.Validate and receive Response.Validated.
    _ = request_tx
        .send(
            materialize::Request {
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

    let materialize::response::Validated {
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
        |validated, (path, model, _disable_task, validate)| {
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

        let materialize::request::validate::Binding {
            resource_config_json,
            collection,
            field_config_json_map: _,
            backfill: _, // Same as `model.backfill`.
            group_by,
        } = validate;
        let collection = collection.unwrap();

        let materialize::response::validated::Binding {
            case_insensitive_fields,
            constraints,
            delta_updates,
            resource_path: validated_path,
            ser_policy,
        } = validated;

        for (field, constraint) in constraints {
            use materialize::response::validated::constraint::Type;
            let type_ = Type::try_from(constraint.r#type);
            if matches!(type_, Ok(Type::Invalid) | Err(_)) {
                Error::Connector {
                    detail: anyhow::anyhow!(
                        "connector returned invalid constraint for field {field}: {type_:?}"
                    ),
                }
                .push(scope, errors);
            }
        }

        if validated_path.is_empty() {
            Error::BindingMissingResourcePath {
                entity: "materialization",
            }
            .push(scope, errors);
        } else if path != *validated_path {
            path = validated_path.clone();
            model.resource = super::store_resource_meta(&model.resource, &path);
            n_meta_updated += 1;
        }

        // Map to the live binding now that we have a validated resource path.
        let live_spec: Option<&flow::materialization_spec::Binding> =
            live_bindings_spec.get(path.as_slice()).cloned();

        if let Some(live_spec) = live_spec {
            if model.backfill < live_spec.backfill {
                Error::BindingBackfillDecrease {
                    entity: "materialization binding",
                    resource: path.iter().join("."),
                    draft: model.backfill,
                    last: live_spec.backfill,
                }
                .push(scope, errors);
            }
        }

        let (field_selection, conflicts) = field_selection::evaluate(
            *case_insensitive_fields,
            &collection.projections,
            group_by,
            live_spec,
            &model,
            constraints,
        );

        // "Incompatible" conflicts have different treatment compared to other conflicts.
        let (conflicts_incompatible, conflicts_other): (Vec<_>, Vec<_>) =
            conflicts.into_iter().partition(|conflict| {
                matches!(
                    conflict.reject,
                    field_selection::Reject::ConnectorIncompatible { .. }
                )
            });

        match (
            !conflicts_other.is_empty(),
            !conflicts_incompatible.is_empty(),
            // Binding may override the model-wide handling.
            model
                .on_incompatible_schema_change
                .unwrap_or(on_incompatible_schema_change),
        ) {
            (false, false, _) => {
                // No conflicts.
            }
            (true, _, _) => {
                for conflict in conflicts_other {
                    Error::FieldConflict(conflict).push(scope, errors);
                }
            }
            (false, true, models::OnIncompatibleSchemaChange::Abort) => {
                for conflict in conflicts_incompatible {
                    Error::AbortOnIncompatibleSchemaChange {
                        this_entity: materialization.to_string(),
                        inner: Box::new(Error::FieldConflict(conflict)),
                    }
                    .push(scope, errors);
                }
            }
            (false, true, models::OnIncompatibleSchemaChange::Backfill) => {
                model_fixes.push(format!(
                    "backfilling binding {:?} due to incompatible field selection (onIncompatibleSchemaChange: backfill)",
                    path.iter().join(".")
                ));
                model.backfill += 1;

                // Note that `field_selection` is valid given our backfill.
            }
            (false, true, models::OnIncompatibleSchemaChange::DisableBinding) => {
                model_fixes.push(format!(
                    "disabling binding {:?} due to incompatible field selection (onIncompatibleSchemaChange: disableBinding)",
                    path.iter().join(".")
                ));
                model.disable = true;

                // Loop now to skip building a spec.
                bindings_path.push(path);
                bindings_model.push(model);
                continue;
            }
            (false, true, models::OnIncompatibleSchemaChange::DisableTask) => {
                model_fixes.push(format!(
                    "disabling materialization due to incompatible field selection of binding {:?} (onIncompatibleSchemaChange: disableTask)",
                    path.iter().join(".")
                ));
                shards.disable = true;

                // No need to continue. All `binding_specs` will be discarded later.
            }
        }

        // Build a partition LabelSelector for this source.
        let (source_partitions, not_before, not_after) = match &model.source {
            models::Source::Collection(_name) => (None, None, None),
            models::Source::Source(models::FullSource {
                name: _,
                partitions,
                not_before,
                not_after,
            }) => (partitions.as_ref(), not_before.as_ref(), not_after.as_ref()),
        };
        let partition_selector = Some(assemble::journal_selector(&collection, source_partitions));

        // Build a state key and read suffix using the validated resource path.
        let state_key = assemble::encode_state_key(&path, model.backfill);
        let journal_read_suffix = format!("materialize/{materialization}/{state_key}");

        let spec = flow::materialization_spec::Binding {
            resource_config_json,
            resource_path: path.clone(),
            collection: Some(collection),
            partition_selector,
            priority: model.priority,
            field_selection: Some(field_selection),
            delta_updates: *delta_updates,
            deprecated_shuffle: None,
            journal_read_suffix,
            not_before: not_before.map(assemble::pb_datetime),
            not_after: not_after.map(assemble::pb_datetime),
            backfill: model.backfill,
            state_key,
            ser_policy: *ser_policy,
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
        "materialization",
        bindings_path.len(),
        |index| &bindings_path[index],
        errors,
    );

    // Pluck out the current shard ID prefix, or create a unique one if it doesn't exist.
    let shard_id_prefix = if let Some(flow::MaterializationSpec {
        shard_template: Some(shard_template),
        ..
    }) = live_spec
    {
        shard_template.id.clone()
    } else {
        let generation_id = if let models::MaterializationEndpoint::Dekaf(_) = &endpoint {
            // Dekaf materializations don't have shards or recovery logs,
            // and thus don't need to distinguish across distinct generations.
            // We use zero to have a predictable shard template ID for use with
            // the authorization API.
            models::Id::zero()
        } else {
            pub_id
        };

        assemble::shard_id_prefix(
            generation_id,
            materialization,
            labels::TASK_TYPE_MATERIALIZATION,
        )
    };

    // If `shards.disable` was or has become true, then all live bindings are inactive.
    // Otherwise remove built bindings from `live_bindings_spec`, and the remainder must be inactive.
    let inactive_bindings = if shards.disable {
        bindings_spec.clear();
        live_bindings_spec.values().map(|v| (*v).clone()).collect()
    } else {
        for binding in &bindings_spec {
            live_bindings_spec.remove(binding.resource_path.as_slice());
        }
        live_bindings_spec.values().map(|v| (*v).clone()).collect()
    };

    let recovery_log_template = assemble::recovery_log_template(
        build_id,
        materialization,
        labels::TASK_TYPE_MATERIALIZATION,
        &shard_id_prefix,
        recovery_stores,
    );
    let shard_template = assemble::shard_template(
        build_id,
        materialization,
        labels::TASK_TYPE_MATERIALIZATION,
        &shards,
        &shard_id_prefix,
        false, // Don't disable wait_for_ack.
        &network_ports,
    );
    let spec = flow::MaterializationSpec {
        name: materialization.to_string(),
        connector_type,
        config_json,
        bindings: bindings_spec,
        recovery_log_template: Some(recovery_log_template),
        shard_template: Some(shard_template),
        network_ports,
        inactive_bindings,
    };
    let model = models::MaterializationDef {
        source: sources,
        on_incompatible_schema_change,
        endpoint,
        bindings: bindings_model,
        shards,
        expect_pub_id: None,
        delete: false,
    };

    std::mem::drop(request_tx);
    () = super::expect_eof(scope, response_rx, errors).await;

    // Compute the dependency hash after we're done with any potential modifications of the model,
    // since disabling a binding would change the hash.
    let dependency_hash = dependencies.compute_hash(&model);
    Some(tables::BuiltMaterialization {
        materialization: materialization.clone(),
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

fn walk_materialization_binding<'a>(
    scope: Scope<'a>,
    default_on_incompatible_schema_change: models::OnIncompatibleSchemaChange,
    mut model: models::MaterializationBinding,
    built_collections: &'a tables::BuiltCollections,
    catalog_name: &models::Materialization,
    data_plane_id: models::Id,
    disable: bool,
    live_bindings_model: &BTreeMap<Vec<String>, &models::MaterializationBinding>,
    live_bindings_spec: &BTreeMap<&[String], &flow::materialization_spec::Binding>,
    model_fixes: &mut Vec<String>,
    errors: &mut tables::Errors,
) -> (
    models::ResourcePath,           // Path extracted from the model resource.
    models::MaterializationBinding, // Model with fixes applied.
    bool,                           // Should we disable the task due to onIncompatibleSchemaChange?
    Option<materialize::request::validate::Binding>, // Validate request if active.
) {
    let model_path = super::load_resource_meta_path(model.resource.get());

    if model.disable {
        // A disabled binding may reference a non-extant collection.
        return (model_path, model, false, None);
    }

    let live_model = live_bindings_model.get(&model_path);
    let live_spec = live_bindings_spec.get(model_path.as_slice());
    let modified_source = Some(&model.source) != live_model.map(|l| &l.source);

    // We must resolve the source collection to continue.
    let (source, source_partitions) = match &model.source {
        models::Source::Collection(collection) => (collection, None),
        models::Source::Source(models::FullSource {
            name,
            partitions,
            not_before,
            not_after,
        }) => {
            if let (Some(not_before), Some(not_after)) = (not_before, not_after) {
                if not_before > not_after {
                    Error::NotBeforeAfterOrder.push(scope.push_prop("source"), errors);
                }
            }
            (name, partitions.as_ref())
        }
    };
    let Some((source_spec, built_collection)) = reference::walk_reference(
        scope,
        "this materialization binding",
        source,
        built_collections,
        modified_source.then_some(errors),
    ) else {
        model_fixes.push(format!("disabled binding of deleted collection {source}"));
        model.disable = true;
        return (model_path, model, false, None);
    };

    if disable {
        // Perform no further validations if the task is disabled.
        return (model_path, model, false, None);
    }

    if let Some(selector) = source_partitions {
        collection::walk_selector(scope, &source_spec, &selector, errors);
    }

    let field_config_json_map: BTreeMap<String, String>;
    let group_by: Vec<String>;
    (model.fields, field_config_json_map, group_by) = walk_materialization_fields(
        scope.push_prop("fields"),
        model.fields,
        &source_spec,
        live_model.map(|l| &l.fields),
        live_spec.and_then(|s| s.field_selection.as_ref()),
        model_fixes,
        errors,
    );

    super::temporary_cross_data_plane_read_check(scope, built_collection, data_plane_id, errors);

    // The binding's `onIncompatibleSchemaChange` takes precedence, if specified.
    let on_incompatible_schema_change = model
        .on_incompatible_schema_change
        .unwrap_or(default_on_incompatible_schema_change);

    // Was this binding's source collection reset under its current backfill count?
    let was_reset = live_spec.is_some_and(|live_spec| {
        live_spec.backfill == model.backfill
            && super::collection_was_reset(&source_spec, &live_spec.collection)
    });
    // Has the effective group-by key of the live materialization changed?
    let group_by_changed = live_spec.is_some_and(|live_spec| {
        live_spec.field_selection.as_ref().map(|f| &f.keys) != Some(&group_by)
    });

    match (was_reset, on_incompatible_schema_change) {
        (false, _) => {}
        (true, models::OnIncompatibleSchemaChange::Abort) => {
            Error::AbortOnIncompatibleSchemaChange {
                this_entity: catalog_name.to_string(),
                inner: Box::new(Error::SourceCollectionWasReset {
                    collection: source.to_string(),
                }),
            }
            .push(scope, errors);
            return (model_path, model, false, None);
        }
        (true, models::OnIncompatibleSchemaChange::Backfill) => {
            model_fixes.push(format!("backfilled binding of reset collection {source}"));
            model.backfill += 1;
        }
        (true, models::OnIncompatibleSchemaChange::DisableBinding) => {
            model_fixes.push(format!("disabling binding of reset collection {source}"));
            model.disable = true;
            return (model_path, model, false, None);
        }
        (true, models::OnIncompatibleSchemaChange::DisableTask) => {
            // This will be handled by the caller.
            return (model_path, model, true, None);
        }
    }

    // TODO(johnny): Take `on_incompatible_schema_change` action on `group_by_changed`.
    _ = group_by_changed; // Not used yet.

    // TODO(johnny): Update projections of `source_spec`, setting `is_primary_key`
    // for (only) those projections which are part of `group_by`

    let validate = materialize::request::validate::Binding {
        resource_config_json: super::strip_resource_meta(&model.resource),
        collection: Some(source_spec),
        field_config_json_map,
        backfill: model.backfill,
        group_by,
    };

    (model_path, model, false, Some(validate))
}

fn walk_materialization_fields<'a>(
    scope: Scope,
    model: models::MaterializationFields,
    collection: &flow::CollectionSpec,
    live_model: Option<&models::MaterializationFields>,
    live_selection: Option<&flow::FieldSelection>,
    model_fixes: &mut Vec<String>,
    errors: &mut tables::Errors,
) -> (
    models::MaterializationFields, // `model` with fixes.
    BTreeMap<String, String>,      // `field_config` for the connector.
    Vec<String>,                   // Effective group-by keys of the binding.
) {
    let models::MaterializationFields {
        group_by,
        require,
        mut exclude,
        recommended,
    } = model;

    let flow::CollectionSpec {
        key,
        name: source,
        projections,
        ..
    } = collection;

    // Temporary migration which adds `groupBy` as needed to materialization
    // bindings which are using non-canonical projections for their group-by.
    // TODO(johnny): Remove once all materializations have been migrated.
    let group_by = temporary_group_by_migration(group_by, live_selection, collection, model_fixes);

    let live_exclude = live_model.map(|l| l.exclude.as_slice()).unwrap_or_default();

    let mut effective_group_by = Vec::new();
    let mut field_config = BTreeMap::new();

    // Enforce each `groupBy` field is present in projections and is a key-able type.
    for (index, field) in group_by.iter().enumerate() {
        let scope = scope.push_prop("groupBy");
        let scope = scope.push_item(index);

        let Some(proj) = projections.iter().find(|p| p.field == field.as_str()) else {
            Error::NoSuchProjection {
                category: "groupBy".to_string(),
                field: field.to_string(),
                collection: source.clone(),
            }
            .push(scope, errors);
            continue;
        };

        let ty_set = proj
            .inference
            .as_ref()
            .map(|inf| types::Set::from_iter(&inf.types))
            .unwrap_or(types::INVALID);

        if !ty_set.is_keyable_type() {
            Error::GroupByWrongType {
                field: field.to_string(),
                type_: ty_set,
            }
            .push(scope, errors);
        }
        effective_group_by.push(field.to_string());
    }

    if effective_group_by.is_empty() {
        // Fall back to the canonical projections of collection key fields.
        effective_group_by.extend(
            key.iter()
                .map(|f| f.strip_prefix("/").unwrap_or(f).to_string()),
        );
    }

    for (field, config) in &require {
        let scope = scope.push_prop("require");
        let scope = scope.push_prop(field);

        if projections.iter().any(|p| p.field == field.as_str()) {
            field_config.insert(field.to_string(), config.to_string());
        } else {
            Error::NoSuchProjection {
                category: "required".to_string(),
                field: field.to_string(),
                collection: source.clone(),
            }
            .push(scope, errors);
        }
    }

    let mut index = 0;
    exclude.retain(|field| {
        let scope = scope.push_prop("exclude");
        let scope = scope.push_item(index);
        index += 1;

        if projections.iter().any(|p| p.field == field.as_str()) {
            true // Matches an existing collection projection.
        } else if live_exclude.contains(field) {
            // This exclusion doesn't match a collection projection,
            // but it also wasn't added by this draft.
            // This implies the projection was removed from the source collection,
            // and we should react by removing the exclusion rather than error.
            model_fixes.push(format!(
                "removed dropped exclude projection {field} of source collection {source}"
            ));
            false
        } else {
            Error::NoSuchProjection {
                category: "exclude".to_string(),
                field: field.to_string(),
                collection: source.clone(),
            }
            .push(scope, errors);
            false
        }
    });

    let model = models::MaterializationFields {
        group_by,
        require,
        exclude,
        recommended,
    };

    (model, field_config, effective_group_by)
}

fn temporary_group_by_migration(
    group_by: Vec<models::Field>,
    live_selection: Option<&flow::FieldSelection>,
    collection: &flow::CollectionSpec,
    model_fixes: &mut Vec<String>,
) -> Vec<models::Field> {
    if !group_by.is_empty() {
        return group_by; // Don't touch.
    }
    let Some(live) = live_selection else {
        return Vec::new(); // Cannot migrate without a live selection.
    };

    // Determine the canonical collection group-by.
    let canonical_fields = collection.key.iter().map(|f| &f[1..]);

    // Don't migrate if the live selection is already canonical.
    if canonical_fields.eq(live.keys.iter()) {
        return Vec::new();
    }

    // Map live selection keys into the pointers of their projections.
    let live_ptrs = live.keys.iter().map(|field| {
        if let Ok(ind) = collection
            .projections
            .binary_search_by_key(&field, |p| &p.field)
        {
            collection.projections[ind].ptr.as_str()
        } else {
            ""
        }
    });

    // Require that the live selection pointers match the collection key,
    // despite using different fields.
    if !live_ptrs.eq(collection.key.iter()) {
        return Vec::new();
    }

    model_fixes.push("added groupBy for migrated non-canonical key".to_string());

    live.keys
        .iter()
        .map(|field| models::Field::new(field))
        .collect()
}
