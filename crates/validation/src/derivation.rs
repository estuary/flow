use super::{
    collection, image, indexed, reference, schema, storage_mapping, Connectors, Error, Scope,
};
use proto_flow::{
    derive,
    flow::{
        self,
        collection_spec::derivation::{ConnectorType, ShuffleType},
    },
};
use superslice::Ext;

pub async fn walk_all_derivations<C: Connectors>(
    build_config: &flow::build_api::Config,
    built_collections: &[tables::BuiltCollection],
    collections: &[tables::Collection],
    connectors: &C,
    images: &[image::Image],
    imports: &[tables::Import],
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

        let tables::Collection {
            scope: _,
            collection,
            spec: models::CollectionDef { derive, .. },
        } = collection;

        // Look at only collections that are derivations,
        // and skip if we cannot map into a BuiltCollection.
        let Some(derive) = derive else { continue };
        let Ok(built_index) = built_collections.binary_search_by_key(&collection, |b| &b.collection) else { continue };

        let validation = walk_derive_request(
            build_config,
            built_collections,
            built_index,
            derive,
            images,
            imports,
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
                let response = connectors.validate_derivation(request.clone());
                (built_index, derivation, request, response.await)
            });

    let validations: Vec<(
        usize,
        &models::Derivation,
        derive::request::Validate,
        anyhow::Result<derive::response::Validated>,
    )> = futures::future::join_all(validations).await;

    let mut specs = Vec::new();

    for (built_index, derive, request, response) in validations {
        let tables::BuiltCollection {
            scope,
            collection: this_collection,
            validated: _,
            spec: flow::CollectionSpec { name, .. },
        } = &built_collections[built_index];
        let scope = Scope::new(scope);

        let models::Derivation {
            using: _,
            transforms,
            shards,
            shuffle_key_types: _,
        } = derive;

        // Unwrap `response` and bail out if it failed.
        let validated = match response {
            Err(err) => {
                Error::Connector { detail: err }.push(scope, errors);
                continue;
            }
            Ok(response) => response,
        };

        let derive::request::Validate {
            connector_type,
            config_json,
            collection: _,
            transforms: transform_requests,
            shuffle_key_types,
            project_root: _,
            import_map: _,
            network_ports,
        } = request;

        let derive::response::Validated {
            transforms: transform_responses,
            generated_files: _,
        } = &validated;

        if transform_requests.len() != transform_responses.len() {
            Error::WrongConnectorBindings {
                expect: transform_requests.len(),
                got: transform_responses.len(),
            }
            .push(scope, errors);
        }

        let built_transforms: Vec<_> = transform_requests
            .into_iter()
            .zip(transform_responses.into_iter())
            .enumerate()
            .map(
                |(transform_index, (transform_request, transform_response))| {
                    let derive::request::validate::Transform {
                        name,
                        collection: source_collection,
                        shuffle_lambda_config_json,
                        lambda_config_json,
                    } = transform_request;

                    let derive::response::validated::Transform { read_only } = transform_response;

                    let models::TransformDef {
                        priority,
                        read_delay,
                        source,
                        shuffle,
                        ..
                    } = &transforms[transform_index];

                    let shuffle_key = match shuffle {
                        Some(models::Shuffle::Key(key)) => {
                            key.iter().map(|ptr| ptr.to_string()).collect()
                        }
                        _ => Vec::new(),
                    };

                    let read_delay_seconds =
                        read_delay.map(|d| d.as_secs() as u32).unwrap_or_default();

                    let (source_name, source_partitions) = match source {
                        models::Source::Collection(name) => (name, None),
                        models::Source::Source(models::FullSource { name, partitions }) => {
                            (name, partitions.as_ref())
                        }
                    };
                    let partition_selector =
                        Some(assemble::journal_selector(source_name, source_partitions));

                    let journal_read_suffix = format!("derive/{}/{}", this_collection, name);

                    (
                        transform_index,
                        flow::collection_spec::derivation::Transform {
                            name,
                            collection: source_collection,
                            partition_selector,
                            priority: *priority,
                            read_delay_seconds,
                            shuffle_key,
                            shuffle_lambda_config_json,
                            lambda_config_json,
                            read_only: *read_only,
                            journal_read_suffix,
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
            connector_type,
            config_json,
            transforms: built_transforms,
            shuffle_key_types,
            recovery_log_template: Some(assemble::recovery_log_template(
                build_config,
                name,
                labels::TASK_TYPE_DERIVATION,
                recovery_stores,
            )),
            shard_template: Some(assemble::shard_template(
                build_config,
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
    build_config: &flow::build_api::Config,
    built_collections: &[tables::BuiltCollection],
    built_index: usize,
    derivation: &'a models::Derivation,
    images: &[image::Image],
    imports: &[tables::Import],
    errors: &mut tables::Errors,
) -> Option<(usize, &'a models::Derivation, derive::request::Validate)> {
    let tables::BuiltCollection {
        scope,
        collection: _,
        validated: _,
        spec,
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
        shards,
    } = derivation;

    let (connector_type, config_json, network_ports) = match using {
        models::DeriveUsing::Connector(config) => (
            ConnectorType::Image as i32,
            serde_json::to_string(config).unwrap(),
            image::walk_image_network_ports(
                scope
                    .push_prop("using")
                    .push_prop("connector")
                    .push_prop("image"),
                shards.disable,
                &config.image,
                images,
                errors,
            ),
        ),
        models::DeriveUsing::Sqlite(config) => (
            ConnectorType::Sqlite as i32,
            serde_json::to_string(config).unwrap(),
            Vec::new(),
        ),
        models::DeriveUsing::Typescript(config) => (
            ConnectorType::Typescript as i32,
            serde_json::to_string(config).unwrap(),
            Vec::new(),
        ),
    };

    let scope = scope.push_prop("transforms");

    let (transform_requests, inferred_shuffle_types): (
        Vec<derive::request::validate::Transform>,
        Vec<Vec<ShuffleType>>,
    ) = transforms
        .iter()
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
            .all(|t| matches!(t.shuffle, Some(models::Shuffle::Lambda(_))))
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
        project_root: build_config.project_root.clone(),
        import_map,
        network_ports,
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
        models::Source::Source(models::FullSource { name, partitions }) => {
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
        Some(models::Shuffle::Key(shuffle_key)) => {
            let scope = scope.push_prop("shuffle");
            let scope = scope.push_prop("key");

            if shuffle_key
                .iter()
                .map(AsRef::as_ref)
                .eq(source.spec.key.iter())
            {
                Error::ShuffleKeyNotDifferent {
                    transform: name.to_string(),
                    collection: source.collection.to_string(),
                }
                .push(scope, errors);
            }
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
        Some(models::Shuffle::Lambda(lambda)) => (Vec::new(), lambda.to_string()),
        // If no shuffle is configured, we shuffle on the collection's key.
        None => (
            source_schema.shuffle_key_types(source.spec.key.iter()),
            String::new(),
        ),
    };

    let request = derive::request::validate::Transform {
        name: name.to_string(),
        collection: Some(source.spec.clone()),
        lambda_config_json: lambda.to_string(),
        shuffle_lambda_config_json,
    };

    Some((request, shuffle_types))
}

/*
async fn perform_validate(
    build_config: &flow::build_api::Config,
    request: &derive::request::Validate,
) -> anyhow::Result<derive::response::Validated> {

    match ConnectorType::from_i32(request.connector_type) {
        Some(ConnectorType::Image) => anyhow::bail!("image connectors are not supported (yet)"),
        Some(ConnectorType::Sqlite) => anyhow::bail!("image connectors are not supported (yet)"),
        Some(ConnectorType::Typescript) => Ok(derive_typescript::validate(
            &std::path::Path::new(&build_config.directory),
            request,
        )?),
        Some(ConnectorType::Invalid) | None => unreachable!("connector type is always valid"),
    }
}
*/
