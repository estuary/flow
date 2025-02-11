use super::{
    collection, indexed, reference, schema, storage_mapping, Connectors, Error, NoOpConnectors,
    Scope,
};
use proto_flow::{
    derive,
    flow::{
        self,
        collection_spec::derivation::{ConnectorType, ShuffleType as ProtoShuffleType},
    },
    ops::log::Level as LogLevel,
};
use superslice::Ext;
use tables::EitherOrBoth as EOB;

pub async fn walk_all_derivations(
    pub_id: models::Id,
    build_id: models::Id,
    draft_collections: &tables::DraftCollections,
    live_collections: &tables::LiveCollections,
    built_collections: &tables::BuiltCollections,
    connectors: &dyn Connectors,
    data_planes: &tables::DataPlanes,
    default_plane_id: Option<models::Id>,
    imports: &tables::Imports,
    project_root: &url::Url,
    storage_mappings: &tables::StorageMappings,
    dependencies: &tables::Dependencies<'_>,
    errors: &mut tables::Errors,
) -> Vec<(
    usize,
    models::Derivation,
    derive::response::Validated,
    flow::collection_spec::Derivation,
    Option<String>,
)> {
    // Outer join of live and draft collections.
    let it = live_collections.outer_join(
        draft_collections.iter().map(|r| (&r.collection, r)),
        |eob| match eob {
            EOB::Left(live) => Some(EOB::Left(live)),
            EOB::Right((_collection, draft)) => Some(EOB::Right(draft)),
            EOB::Both(live, (_collection, draft)) => Some(EOB::Both(live, draft)),
        },
    );

    let futures: Vec<_> = it
        .map(|eob| async {
            let mut local_errors = tables::Errors::new();

            let built_derivation = walk_derivation(
                pub_id,
                build_id,
                eob,
                built_collections,
                connectors,
                data_planes,
                default_plane_id,
                imports,
                project_root,
                storage_mappings,
                dependencies,
                &mut local_errors,
            )
            .await;

            (built_derivation, local_errors)
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

async fn walk_derivation(
    pub_id: models::Id,
    build_id: models::Id,
    eob: EOB<&tables::LiveCollection, &tables::DraftCollection>,
    built_collections: &tables::BuiltCollections,
    connectors: &dyn Connectors,
    data_planes: &tables::DataPlanes,
    default_plane_id: Option<models::Id>,
    imports: &tables::Imports,
    project_root: &url::Url,
    storage_mappings: &tables::StorageMappings,
    dependencies: &tables::Dependencies<'_>,
    errors: &mut tables::Errors,
) -> Option<(
    usize,
    models::Derivation,
    derive::response::Validated,
    flow::collection_spec::Derivation,
    Option<String>,
)> {
    let (collection, scope, model, data_plane_id, last_pub_id, last_collection, dependency_hash) =
        match eob {
            // If this is a drafted derivation, pluck out its details.
            EOB::Right(tables::DraftCollection {
                collection,
                scope,
                model:
                    Some(
                        collection_model @ models::CollectionDef {
                            derive: Some(model),
                            ..
                        },
                    ),
                ..
            }) => (
                collection,
                scope,
                model.clone(),
                default_plane_id.unwrap_or(models::Id::zero()),
                None,
                None,
                dependencies.compute_hash(collection_model),
            ),

            EOB::Both(
                tables::LiveCollection {
                    spec,
                    last_pub_id,
                    data_plane_id,
                    ..
                },
                tables::DraftCollection {
                    collection,
                    scope,
                    model:
                        Some(
                            collection_model @ models::CollectionDef {
                                derive: Some(model),
                                ..
                            },
                        ),
                    ..
                },
            ) => (
                collection,
                scope,
                model.clone(),
                *data_plane_id,
                spec.derivation.is_some().then_some(last_pub_id),
                spec.derivation.is_some().then_some(spec),
                dependencies.compute_hash(collection_model),
            ),

            // For all other cases, don't build this derivation.
            _ => return None,
        };
    let scope = Scope::new(scope);
    let scope = scope.push_prop("derive");

    // Collect imports of this derivation, so that we can present the connector
    // with a relative mapping of its imports. This is used to generate more
    // helpful errors, where temporary files within the connector are re-mapped
    // to the user's relative filesystem.
    let import_map = {
        let scope = scope.flatten();

        let rng = imports.equal_range_by(|import| {
            if import.scope.as_str().starts_with(scope.as_str()) {
                std::cmp::Ordering::Equal
            } else {
                import.scope.cmp(&scope)
            }
        });

        let strip_len = scope.fragment().unwrap().len();

        imports[rng]
            .iter()
            .map(|import| {
                (
                    import.scope.fragment().unwrap()[strip_len..].to_string(),
                    import.to_resource.to_string(),
                )
            })
            .collect()
    };

    let models::Derivation {
        using,
        transforms: transforms_model,
        shuffle_key_types: shuffle_key_types_model,
        shards,
    } = model;

    // Unwrap `using` into a connector type and configuration.
    let (connector_type, config_json) = match &using {
        models::DeriveUsing::Connector(config) => (
            ConnectorType::Image as i32,
            serde_json::to_string(config).unwrap(),
        ),
        models::DeriveUsing::Local(config) => (
            ConnectorType::Local as i32,
            serde_json::to_string(config).unwrap(),
        ),
        models::DeriveUsing::Sqlite(config) => (
            ConnectorType::Sqlite as i32,
            serde_json::to_string(config).unwrap(),
        ),
        models::DeriveUsing::Typescript(config) => (
            ConnectorType::Typescript as i32,
            serde_json::to_string(config).unwrap(),
        ),
    };

    // Map enumerated transform models into paired validation requests.
    let scope_transforms = scope.push_prop("transforms");
    let transforms_model_len = transforms_model.len();

    let transforms: Vec<(usize, models::TransformDef, Option<ValidateContext>)> = transforms_model
        .into_iter()
        .enumerate()
        .filter_map(|(index, transform)| {
            let (model, validate) = walk_derive_transform(
                scope_transforms.push_item(index),
                transform,
                collection,
                built_collections,
                data_plane_id,
                errors,
            )?;
            Some((index, model, validate))
        })
        .collect();

    // Error if transform names are duplicated.
    indexed::walk_duplicates(
        transforms.iter().map(|(index, model, _validate)| {
            (
                "transform",
                model.name.as_str(),
                scope_transforms.push_item(*index),
            )
        }),
        errors,
    );

    // Select out non-empty inferred shuffle types of each transformation.
    let mut inferred_shuffle_types: Vec<(usize, &models::Transform, &Vec<_>)> = transforms
        .iter()
        .filter_map(|(index, model, validate)| {
            let validate = validate.as_ref()?;
            if !validate.inferred_shuffle_types.is_empty() {
                return Some((*index, &model.name, &validate.inferred_shuffle_types));
            }
            None
        })
        .collect();

    // Verify that shuffle key types & lengths align.
    let shuffle_key_types_spec: Vec<i32> = if !shuffle_key_types_model.is_empty() {
        // Map user-provided shuffle types from the `models` domain to `proto_flow`.
        let expect_types = shuffle_key_types_model
            .iter()
            .map(|t| match t {
                models::ShuffleType::Boolean => ProtoShuffleType::Boolean,
                models::ShuffleType::Integer => ProtoShuffleType::Integer,
                models::ShuffleType::String => ProtoShuffleType::String,
            })
            .collect::<Vec<_>>();

        // Require that `expect_types` matches every transform with inferred shuffle types.
        for (index, name, types) in inferred_shuffle_types {
            if types != &expect_types {
                Error::ShuffleKeyExplicitMismatch {
                    name: name.to_string(),
                    types: types.clone(),
                    given_types: expect_types.clone(),
                }
                .push(scope_transforms.push_item(index), errors);
            }
        }
        expect_types
    } else if let Some((lhs_ind, lhs_name, lhs_types)) = inferred_shuffle_types.pop() {
        for (_rhs_ind, rhs_name, rhs_types) in inferred_shuffle_types {
            if lhs_types != rhs_types {
                Error::ShuffleKeyImplicitMismatch {
                    lhs_name: lhs_name.to_string(),
                    lhs_types: lhs_types.clone(),
                    rhs_name: rhs_name.to_string(),
                    rhs_types: rhs_types.clone(),
                }
                .push(scope_transforms.push_item(lhs_ind), errors);
            }
        }
        lhs_types.clone()
    } else {
        if transforms
            .iter()
            .any(|(_index, model, _validate)| matches!(model.shuffle, models::Shuffle::Lambda(_)))
        {
            Error::ShuffleKeyCannotInfer {}.push(scope, errors);
        }
        Vec::new()
    }
    .into_iter()
    .map(|type_| type_ as i32)
    .collect();

    let Ok(built_index) = built_collections.binary_search_by_key(&collection, |b| &b.collection)
    else {
        return None; // Build of underlying collection errored out.
    };
    let built_collection = &built_collections[built_index];

    // Determine storage mappings for task recovery logs.
    let recovery_stores = storage_mapping::mapped_stores(
        scope,
        "derivation",
        &format!("recovery/{collection}"),
        storage_mappings,
        errors,
    );

    // Resolve the data-plane for this task. We cannot continue without it.
    let data_plane = reference::walk_data_plane(
        scope,
        &built_collection.collection,
        built_collection.data_plane_id,
        data_planes,
        errors,
    )?;

    // We've completed all cheap validation checks.
    // If we've already encountered errors then stop now.
    if !errors.is_empty() {
        return None;
    }

    let transforms_validate: Vec<_> = transforms
        .iter()
        .filter_map(|(_index, _model, validate)| {
            if let Some(validate) = validate {
                Some(validate.validate.clone())
            } else {
                None
            }
        })
        .collect();
    let transforms_validate_len = transforms_validate.len();

    let validate_request = derive::request::Validate {
        connector_type,
        config_json: config_json.clone(),
        collection: built_collection.spec.clone(),
        transforms: transforms_validate,
        shuffle_key_types: shuffle_key_types_spec.iter().map(|t| *t as i32).collect(),
        project_root: project_root.to_string(),
        import_map,
        last_collection: last_collection.cloned(),
        last_version: last_pub_id.map(models::Id::to_string).unwrap_or_default(),
    };
    let wrapped_request = derive::Request {
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
        NoOpConnectors.validate_derivation(wrapped_request, data_plane)
    } else {
        connectors.validate_derivation(wrapped_request, data_plane)
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

    let derive::response::Validated {
        transforms: transforms_validated,
        generated_files,
    } = &validated_response;

    if transforms_validate_len != transforms_validated.len() {
        Error::WrongConnectorBindings {
            expect: transforms_validate_len,
            got: transforms_validated.len(),
        }
        .push(scope, errors);
    }

    // Sanity check the URLs of generated files.
    for (maybe_url, _) in generated_files {
        if let Err(err) = url::Url::parse(&maybe_url) {
            Error::InvalidGeneratedFileUrl {
                url: maybe_url.clone(),
                detail: err,
            }
            .push(scope, errors)
        }
    }

    // Join transform models and their Validate requests with their Validated responses.
    let transforms = transforms.into_iter().scan(
        transforms_validated.into_iter(),
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

    let mut disable_wait_for_ack = false;
    let mut transforms_model = Vec::with_capacity(transforms_model_len);
    let mut transforms_spec = Vec::with_capacity(transforms_validate_len);

    // Map Validate / Validated pairs into DerivationSpec::Transforms.
    for (_index, model, validate_validated) in transforms {
        let Some((validate, validated)) = validate_validated else {
            transforms_model.push(model);
            continue;
        };

        let ValidateContext {
            validate:
                derive::request::validate::Transform {
                    name: transform_name,
                    collection: source_collection,
                    shuffle_lambda_config_json,
                    lambda_config_json,
                    backfill,
                },
            inferred_shuffle_types: _,
            reads_from_self,
        } = validate;

        let derive::response::validated::Transform { read_only } = validated;

        let models::TransformDef {
            name: _,
            source,
            shuffle,
            priority,
            read_delay,
            lambda: _,
            disable: _,
            backfill: _,
        } = &model;

        // If any transform reads from ourself, we must disable the "wait for ACK" read optimization.
        if reads_from_self {
            disable_wait_for_ack = true;
        }
        let shuffle_key = match shuffle {
            models::Shuffle::Key(key) => key.iter().map(|ptr| ptr.to_string()).collect(),
            _ => Vec::new(),
        };
        // models::Shuffle::Any is represented as an empty `shuffle_key`
        // and an empty `shuffle_lambda_config_json`.

        let read_delay_seconds = read_delay.map(|d| d.as_secs() as u32).unwrap_or_default();

        // Build a partition LabelSelector for this source.
        let (source_partitions, not_before, not_after) = match source {
            models::Source::Collection(_name) => (None, None, None),
            models::Source::Source(models::FullSource {
                name: _,
                partitions,
                not_before,
                not_after,
            }) => (partitions.as_ref(), not_before.as_ref(), not_after.as_ref()),
        };
        let partition_selector = Some(assemble::journal_selector(
            source_collection.as_ref().unwrap(),
            source_partitions,
        ));

        // Build a state key and read suffix using the transform name as it's resource path.
        let state_key = assemble::encode_state_key(&[&transform_name], backfill);
        let journal_read_suffix = format!("derive/{collection}/{state_key}");

        let spec = flow::collection_spec::derivation::Transform {
            name: transform_name,
            collection: source_collection,
            partition_selector,
            priority: *priority,
            read_delay_seconds,
            shuffle_key,
            shuffle_lambda_config_json,
            lambda_config_json,
            read_only: *read_only,
            journal_read_suffix,
            not_before: not_before.map(assemble::pb_datetime),
            not_after: not_after.map(assemble::pb_datetime),
            backfill,
        };

        transforms_model.push(model);
        transforms_spec.push(spec);
    }

    // Pluck out the current shard ID prefix, or create a unique one if it doesn't exist.
    let shard_id_prefix = if let Some(flow::CollectionSpec {
        derivation:
            Some(flow::collection_spec::Derivation {
                shard_template: Some(shard_template),
                ..
            }),
        ..
    }) = last_collection
    {
        shard_template.id.clone()
    } else {
        assemble::shard_id_prefix(pub_id, collection, labels::TASK_TYPE_DERIVATION)
    };

    let recovery_log_template = assemble::recovery_log_template(
        build_id,
        collection,
        labels::TASK_TYPE_DERIVATION,
        &shard_id_prefix,
        recovery_stores,
    );
    let shard_template = assemble::shard_template(
        build_id,
        collection,
        labels::TASK_TYPE_DERIVATION,
        &shards,
        &shard_id_prefix,
        disable_wait_for_ack,
        &network_ports,
    );
    let spec = flow::collection_spec::Derivation {
        connector_type,
        config_json,
        transforms: transforms_spec,
        shuffle_key_types: shuffle_key_types_spec.iter().map(|t| *t as i32).collect(),
        recovery_log_template: Some(recovery_log_template),
        shard_template: Some(shard_template),
        network_ports,
        inactive_transforms: Vec::new(),
    };
    let model = models::Derivation {
        shards,
        shuffle_key_types: shuffle_key_types_model,
        transforms: transforms_model,
        using,
    };

    Some((
        built_index,
        model,
        validated_response,
        spec,
        dependency_hash,
    ))
}

// ValidateContext composes a Transform's portion of a Validate request with
// additional transform metadata utilized in building the DerivationSpec.
struct ValidateContext {
    validate: derive::request::validate::Transform,
    reads_from_self: bool,
    inferred_shuffle_types: Vec<flow::collection_spec::derivation::ShuffleType>,
}

fn walk_derive_transform<'a>(
    scope: Scope<'a>,
    model: models::TransformDef,
    catalog_name: &models::Collection,
    built_collections: &'a tables::BuiltCollections,
    data_plane_id: models::Id,
    errors: &mut tables::Errors,
) -> Option<(models::TransformDef, Option<ValidateContext>)> {
    if model.disable {
        return Some((model, None)); // Retain but perform no further validation.
    }

    let models::TransformDef {
        backfill,
        disable: _,
        lambda,
        name,
        priority,
        read_delay,
        shuffle,
        source: source_model,
    } = model;

    indexed::walk_name(
        scope,
        "transform",
        name.as_ref(),
        models::Transform::regex(),
        errors,
    );

    let (source_name, source_partitions) = match &source_model {
        models::Source::Collection(name) => (name, None),
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

    // Dereference the transform's source. We can't continue without it.
    let (spec, source) = reference::walk_reference(
        scope,
        &format!("transform {name}"),
        source_name,
        built_collections,
        errors,
    )?;

    let source_schema = schema::Schema::new(if spec.read_schema_json.is_empty() {
        &spec.write_schema_json
    } else {
        &spec.read_schema_json
    })
    .unwrap();

    if let Some(selector) = source_partitions {
        collection::walk_selector(scope, &spec, &selector, errors);
    }

    let (shuffle_types, shuffle_lambda_config_json) = match &shuffle {
        models::Shuffle::Key(shuffle_key) => {
            let scope = scope.push_prop("shuffle");
            let scope = scope.push_prop("key");

            if shuffle_key.is_empty() {
                Error::ShuffleKeyEmpty {
                    transform: name.to_string(),
                }
                .push(scope, errors);
            }
            for (key_index, ptr) in shuffle_key.iter().enumerate() {
                if let Err(err) = schema::Schema::walk_ptr(&source_schema, None, ptr, true) {
                    Error::from(err).push(scope.push_item(key_index), errors);
                }
            }
            (
                source_schema.shuffle_key_types(shuffle_key.iter()),
                String::new(),
            )
        }
        // When shuffling using a lambda, we pass shuffle key types to the connector
        // and let it verify and error if they are incompatible with the lambda.
        models::Shuffle::Lambda(lambda) => (Vec::new(), lambda.to_string()),
        // Source documents may be processed by any shard.
        models::Shuffle::Any => (Vec::new(), String::new()),
        // Shuffle is unset.
        models::Shuffle::Unset => {
            Error::ShuffleUnset {
                transform: name.to_string(),
            }
            .push(scope, errors);
            (Vec::new(), String::new())
        }
    };

    super::temporary_cross_data_plane_read_check(scope, source, data_plane_id, errors);

    let validate = derive::request::validate::Transform {
        name: name.to_string(),
        collection: Some(spec),
        lambda_config_json: lambda.to_string(),
        shuffle_lambda_config_json,
        backfill,
    };

    let model = models::TransformDef {
        backfill,
        disable: false,
        lambda,
        name,
        priority,
        read_delay,
        shuffle,
        source: source_model,
    };

    Some((
        model,
        Some(ValidateContext {
            validate,
            inferred_shuffle_types: shuffle_types,
            reads_from_self: &source.collection == catalog_name,
        }),
    ))
}

fn extract_validated(
    response: anyhow::Result<derive::Response>,
) -> Result<(derive::response::Validated, Vec<flow::NetworkPort>), Error> {
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
