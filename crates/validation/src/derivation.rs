use super::{collection, indexed, reference, schema, storage_mapping, Connectors, Error, Scope};
use proto_flow::{
    derive, flow,
    flow::collection_spec::derivation::{ConnectorType, ShuffleType},
    ops::log::Level as LogLevel,
};
use superslice::Ext;
use tables::SpecRow;

pub async fn walk_all_derivations(
    build_id: &str,
    built_collections: &[tables::BuiltCollection],
    collections: &[tables::Collection],
    connectors: &dyn Connectors,
    imports: &[tables::Import],
    project_root: &url::Url,
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) -> Vec<(
    usize,
    derive::response::Validated,
    flow::collection_spec::Derivation,
)> {
    let mut validations = Vec::new();

    for collection in collections {
        let mut derive_errors = tables::Errors::new();

        // let tables::Collection {
        //     scope: _,
        //     collection,
        //     spec: models::CollectionDef { derive, .. },
        // } = collection;
        let models::CollectionDef { derive, .. } = collection.get_final_spec();

        // Look at only collections that are derivations,
        // and skip if we cannot map into a BuiltCollection.
        let Some(derive) = derive else { continue };
        let Ok(built_index) =
            built_collections.binary_search_by_key(&&collection.collection, |b| &b.collection)
        else {
            continue;
        };

        let validation = walk_derive_request(
            built_collections,
            built_index,
            derive,
            imports,
            project_root,
            &mut derive_errors,
        );

        if !derive_errors.is_empty() {
            errors.extend(derive_errors.into_iter());
        } else if let Some(validation) = validation {
            validations.push(validation);
        }
    }

    // Run all validations concurrently.
    let validations =
        validations
            .into_iter()
            .map(|(built_index, derivation, request)| async move {
                let wrapped = derive::Request {
                    validate: Some(request.clone()),
                    ..Default::default()
                }
                .with_internal(|internal| {
                    if let Some(s) = &derivation.shards.log_level {
                        internal.set_log_level(LogLevel::from_str_name(s).unwrap_or_default());
                    }
                });

                // For the moment, we continue to validate a disabled derivation.
                // There's an argument that we shouldn't, but it's currently inconclusive.
                let response = connectors.validate_derivation(wrapped);
                (built_index, derivation, request, response.await)
            });

    let validations: Vec<(
        usize,
        &models::Derivation,
        derive::request::Validate,
        anyhow::Result<derive::Response>,
    )> = futures::future::join_all(validations).await;

    let mut specs = Vec::new();

    for (built_index, derive, mut request, response) in validations {
        let tables::BuiltCollection {
            scope,
            collection: this_collection,
            validated: _,
            spec: flow::CollectionSpec { name, .. },
            inferred_schema_md5: _,
        } = &built_collections[built_index];
        let scope = Scope::new(scope);

        let models::Derivation {
            using: _,
            transforms: transform_models,
            shards,
            shuffle_key_types: _,
        } = derive;

        // Unwrap `response` and bail out if it failed.
        let (validated, network_ports) = match extract_validated(response) {
            Err(err) => {
                err.push(scope, errors);
                continue;
            }
            Ok(ok) => ok,
        };

        let derive::request::Validate {
            connector_type,
            config_json,
            collection: _,
            transforms: transform_requests,
            shuffle_key_types,
            project_root: _,
            import_map: _,
            last_collection: _,
            last_version: _,
        } = &mut request;

        let derive::response::Validated {
            generated_files,
            transforms: transform_responses,
        } = &validated;

        for (maybe_url, _) in generated_files {
            if let Err(err) = url::Url::parse(&maybe_url) {
                Error::InvalidGeneratedFileUrl {
                    url: maybe_url.clone(),
                    detail: err,
                }
                .push(scope, errors)
            }
        }

        if transform_requests.len() != transform_responses.len() {
            Error::WrongConnectorBindings {
                expect: transform_requests.len(),
                got: transform_responses.len(),
            }
            .push(scope, errors);
        }

        // We only validated non-disabled transforms, in transform order.
        // Filter `transform_models` correspondingly.
        let transform_models: Vec<_> = transform_models.iter().filter(|b| !b.disable).collect();

        let built_transforms: Vec<_> = std::mem::take(transform_requests)
            .into_iter()
            .zip(transform_responses.into_iter())
            .enumerate()
            .map(
                |(transform_index, (transform_request, transform_response))| {
                    let derive::request::validate::Transform {
                        name: transform_name,
                        collection: source_collection,
                        shuffle_lambda_config_json,
                        lambda_config_json,
                        backfill,
                    } = transform_request;

                    let derive::response::validated::Transform { read_only } = transform_response;

                    let models::TransformDef {
                        name: _,
                        source,
                        shuffle,
                        priority,
                        read_delay,
                        lambda: _,
                        disable: _,
                        backfill: _,
                    } = transform_models[transform_index];

                    let shuffle_key = match shuffle {
                        models::Shuffle::Key(key) => {
                            key.iter().map(|ptr| ptr.to_string()).collect()
                        }
                        _ => Vec::new(),
                    };
                    // models::Shuffle::Any is represented as an empty `shuffle_key`
                    // and an empty `shuffle_lambda_config_json`.

                    let read_delay_seconds =
                        read_delay.map(|d| d.as_secs() as u32).unwrap_or_default();

                    let (source_name, source_partitions, not_before, not_after) = match source {
                        models::Source::Collection(name) => (name, None, None, None),
                        models::Source::Source(models::FullSource {
                            name,
                            partitions,
                            not_before,
                            not_after,
                        }) => (
                            name,
                            partitions.as_ref(),
                            not_before.as_ref(),
                            not_after.as_ref(),
                        ),
                    };
                    let partition_selector =
                        Some(assemble::journal_selector(source_name, source_partitions));

                    let state_key = assemble::encode_state_key(&[&transform_name], backfill);
                    let journal_read_suffix = format!("derive/{}/{}", this_collection, state_key);

                    (
                        transform_index,
                        flow::collection_spec::derivation::Transform {
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
                        },
                    )
                },
            )
            .collect();

        // Unzip to strip transform indices, leaving built transforms.
        let (_, built_transforms): (Vec<_>, Vec<_>) = built_transforms.into_iter().unzip();

        // If this derivation reads from itself, we must disable the "wait for ack"
        // optimization so that we don't hold open transactions waiting for our
        // own ack that cannot come.
        let disable_wait_for_ack = built_transforms
            .iter()
            .any(|t| t.collection.as_ref().unwrap().name == name.as_str());

        let recovery_stores = storage_mapping::mapped_stores(
            scope,
            "derivation",
            &format!("recovery/{}", name.as_str()),
            storage_mappings,
            errors,
        );

        let spec = flow::collection_spec::Derivation {
            connector_type: *connector_type,
            config_json: std::mem::take(config_json),
            transforms: built_transforms,
            shuffle_key_types: std::mem::take(shuffle_key_types),
            recovery_log_template: Some(assemble::recovery_log_template(
                build_id,
                name,
                labels::TASK_TYPE_DERIVATION,
                recovery_stores,
            )),
            shard_template: Some(assemble::shard_template(
                build_id,
                name,
                labels::TASK_TYPE_DERIVATION,
                &shards,
                disable_wait_for_ack,
                &network_ports,
            )),
            network_ports,
        };
        specs.push((built_index, validated, spec));
    }

    specs
}

fn walk_derive_request<'a>(
    built_collections: &[tables::BuiltCollection],
    built_index: usize,
    derivation: &'a models::Derivation,
    imports: &[tables::Import],
    project_root: &url::Url,
    errors: &mut tables::Errors,
) -> Option<(usize, &'a models::Derivation, derive::request::Validate)> {
    let tables::BuiltCollection {
        scope,
        collection: _,
        validated: _,
        spec,
        inferred_schema_md5: _,
    } = &built_collections[built_index];

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
        transforms,
        shuffle_key_types: given_shuffle_types,
        shards: _,
    } = derivation;

    let (connector_type, config_json) = match using {
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

    let scope = scope.push_prop("transforms");

    let (transform_requests, inferred_shuffle_types): (
        Vec<derive::request::validate::Transform>,
        Vec<Vec<ShuffleType>>,
    ) = transforms
        .iter()
        .filter(|t| !t.disable)
        .enumerate()
        .map(|(transform_index, transform)| {
            walk_derive_transform(
                scope.push_item(transform_index),
                built_collections,
                transform,
                errors,
            )
        })
        // Force eager evaluation of all results.
        .collect::<Vec<Option<_>>>()
        .into_iter()
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .unzip();

    indexed::walk_duplicates(
        transforms
            .iter()
            .enumerate()
            .map(|(transform_index, transform)| {
                (
                    "transform",
                    transform.name.as_str(),
                    scope.push_item(transform_index),
                )
            }),
        errors,
    );

    // Verify that shuffle key types & lengths align.
    let mut inferred_shuffle_types = inferred_shuffle_types
        .iter()
        .enumerate()
        .filter(|(_, v)| !v.is_empty());

    let shuffle_key_types = if !given_shuffle_types.is_empty() {
        let given_shuffle_types = given_shuffle_types
            .iter()
            .map(|t| match t {
                models::ShuffleType::Boolean => ShuffleType::Boolean,
                models::ShuffleType::Integer => ShuffleType::Integer,
                models::ShuffleType::String => ShuffleType::String,
            })
            .collect::<Vec<_>>();

        for (transform_index, types) in inferred_shuffle_types {
            if types != &given_shuffle_types {
                Error::ShuffleKeyExplicitMismatch {
                    name: transforms[transform_index].name.to_string(),
                    types: types.clone(),
                    given_types: given_shuffle_types.clone(),
                }
                .push(scope.push_item(transform_index), errors);
            }
        }
        given_shuffle_types
    } else if let Some((transform_index, types)) = inferred_shuffle_types.next() {
        for (r_ind, r_types) in inferred_shuffle_types {
            if types != r_types {
                Error::ShuffleKeyImplicitMismatch {
                    lhs_name: transforms[transform_index].name.to_string(),
                    lhs_types: types.clone(),
                    rhs_name: transforms[r_ind].name.to_string(),
                    rhs_types: r_types.clone(),
                }
                .push(scope.push_item(transform_index), errors);
            }
        }
        types.clone()
    } else {
        if transforms
            .iter()
            .any(|t| matches!(t.shuffle, models::Shuffle::Lambda(_)))
        {
            Error::ShuffleKeyCannotInfer {}.push(scope, errors);
        }
        Vec::new()
    }
    .into_iter()
    .map(|t| t as i32)
    .collect();

    let request = derive::request::Validate {
        connector_type,
        config_json,
        collection: Some(spec.clone()),
        transforms: transform_requests,
        shuffle_key_types,
        project_root: project_root.to_string(),
        import_map,
        // TODO(johnny): Thread these through.
        last_collection: None,
        last_version: String::new(),
    };

    Some((built_index, derivation, request))
}

fn walk_derive_transform(
    scope: Scope,
    built_collections: &[tables::BuiltCollection],
    transform: &models::TransformDef,
    errors: &mut tables::Errors,
) -> Option<(derive::request::validate::Transform, Vec<ShuffleType>)> {
    let models::TransformDef {
        name,
        source,
        shuffle,
        priority: _,
        read_delay: _,
        lambda,
        disable: _,
        backfill,
    } = transform;

    indexed::walk_name(
        scope,
        "transform",
        name.as_ref(),
        models::Transform::regex(),
        errors,
    );

    let (source_name, source_partitions) = match source {
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
    let source = reference::walk_reference(
        scope,
        &format!("transform {name}"),
        "collection",
        source_name,
        built_collections,
        |c| (&c.collection, Scope::new(&c.scope)),
        errors,
    )?;
    let source_schema = schema::Schema::new(if source.spec.read_schema_json.is_empty() {
        &source.spec.write_schema_json
    } else {
        &source.spec.read_schema_json
    })
    .unwrap();

    if let Some(selector) = source_partitions {
        collection::walk_selector(scope, &source.spec, &selector, errors);
    }

    let (shuffle_types, shuffle_lambda_config_json) = match shuffle {
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
                if let Err(err) = source_schema.walk_ptr(ptr, true) {
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

    let request = derive::request::validate::Transform {
        name: name.to_string(),
        collection: Some(source.spec.clone()),
        lambda_config_json: lambda.to_string(),
        shuffle_lambda_config_json,
        backfill: *backfill,
    };

    Some((request, shuffle_types))
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
