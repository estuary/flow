use crate::Scope;
use superslice::Ext;

pub fn inline_sources(sources: &mut tables::Sources) {
    let tables::Sources {
        captures,
        collections,
        fetches: _,
        imports,
        materializations,
        resources,
        storage_mappings: _,
        tests,
        errors: _,
    } = sources;

    for capture in captures.iter_mut() {
        inline_capture(&capture.scope, &mut capture.spec, imports, resources);
    }
    for collection in collections.iter_mut() {
        inline_collection(&collection.scope, &mut collection.spec, imports, resources);
    }
    for materialization in materializations.iter_mut() {
        inline_materialization(
            &materialization.scope,
            &mut materialization.spec,
            imports,
            resources,
        );
    }
    for test in tests.iter_mut() {
        inline_test(&test.scope, &mut test.spec, imports, resources);
    }
}

pub fn inline_capture(
    scope: &url::Url,
    spec: &mut models::CaptureDef,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    let models::CaptureDef {
        endpoint, bindings, ..
    } = spec;

    match endpoint {
        models::CaptureEndpoint::Connector(models::ConnectorConfig { config, .. }) => {
            inline_config(
                Scope::new(scope)
                    .push_prop("endpoint")
                    .push_prop("connector")
                    .push_prop("config"),
                config,
                imports,
                resources,
            )
        }
        models::CaptureEndpoint::Local(models::LocalConfig { config, .. }) => inline_config(
            Scope::new(scope)
                .push_prop("endpoint")
                .push_prop("local")
                .push_prop("config"),
            config,
            imports,
            resources,
        ),
    }

    for (index, models::CaptureBinding { resource, .. }) in bindings.iter_mut().enumerate() {
        inline_config(
            Scope::new(scope)
                .push_prop("bindings")
                .push_item(index)
                .push_prop("resource"),
            resource,
            imports,
            resources,
        )
    }
}

fn inline_collection(
    scope: &url::Url,
    spec: &mut models::CollectionDef,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    let models::CollectionDef {
        schema,
        write_schema,
        read_schema,
        key: _,
        projections: _,
        journals: _,
        derive,
    } = spec;

    if let Some(schema) = schema {
        inline_schema(
            Scope::new(scope).push_prop("schema"),
            schema,
            imports,
            resources,
        )
    }
    if let Some(write_schema) = write_schema {
        inline_schema(
            Scope::new(scope).push_prop("writeSchema"),
            write_schema,
            imports,
            resources,
        )
    }
    if let Some(read_schema) = read_schema {
        inline_schema(
            Scope::new(scope).push_prop("readSchema"),
            read_schema,
            imports,
            resources,
        )
    }
    if let Some(derivation) = derive {
        inline_derivation(scope, derivation, imports, resources)
    }
}

fn inline_derivation(
    scope: &url::Url,
    derivation: &mut models::Derivation,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    let models::Derivation {
        using,
        transforms,
        shuffle_key_types: _,
        shards: _,
    } = derivation;

    match using {
        models::DeriveUsing::Connector(models::ConnectorConfig { config, .. }) => {
            inline_config(
                Scope::new(scope)
                    .push_prop("derive")
                    .push_prop("using")
                    .push_prop("connector")
                    .push_prop("config"),
                config,
                imports,
                resources,
            );
        }
        models::DeriveUsing::Local(models::LocalConfig { config, .. }) => inline_config(
            Scope::new(scope)
                .push_prop("derive")
                .push_prop("using")
                .push_prop("local")
                .push_prop("config"),
            config,
            imports,
            resources,
        ),
        models::DeriveUsing::Sqlite(models::DeriveUsingSqlite { migrations }) => {
            for (index, migration) in migrations.iter_mut().enumerate() {
                inline_config(
                    Scope::new(scope)
                        .push_prop("derive")
                        .push_prop("using")
                        .push_prop("sqlite")
                        .push_prop("migrations")
                        .push_item(index),
                    migration,
                    imports,
                    resources,
                );
            }
        }
        models::DeriveUsing::Typescript(models::DeriveUsingTypescript { module }) => {
            inline_config(
                Scope::new(scope)
                    .push_prop("derive")
                    .push_prop("using")
                    .push_prop("typescript")
                    .push_prop("module"),
                module,
                imports,
                resources,
            );
        }
    }

    for (
        index,
        models::TransformDef {
            lambda, shuffle, ..
        },
    ) in transforms.iter_mut().enumerate()
    {
        inline_config(
            Scope::new(scope)
                .push_prop("derive")
                .push_prop("transforms")
                .push_item(index)
                .push_prop("lambda"),
            lambda,
            imports,
            resources,
        );

        if let models::Shuffle::Lambda(lambda) = shuffle {
            inline_config(
                Scope::new(scope)
                    .push_prop("derive")
                    .push_prop("transforms")
                    .push_item(index)
                    .push_prop("shuffle")
                    .push_prop("lambda"),
                lambda,
                imports,
                resources,
            );
        }
    }
}

fn inline_materialization(
    scope: &url::Url,
    spec: &mut models::MaterializationDef,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    let models::MaterializationDef {
        source_capture: _,
        endpoint,
        bindings,
        shards: _,
    } = spec;

    match endpoint {
        models::MaterializationEndpoint::Connector(models::ConnectorConfig { config, .. }) => {
            inline_config(
                Scope::new(scope)
                    .push_prop("endpoint")
                    .push_prop("connector")
                    .push_prop("config"),
                config,
                imports,
                resources,
            )
        }
        models::MaterializationEndpoint::Local(models::LocalConfig { config, .. }) => {
            inline_config(
                Scope::new(scope)
                    .push_prop("endpoint")
                    .push_prop("connector")
                    .push_prop("config"),
                config,
                imports,
                resources,
            )
        }
    }

    for (index, models::MaterializationBinding { resource, .. }) in bindings.iter_mut().enumerate()
    {
        inline_config(
            Scope::new(scope)
                .push_prop("bindings")
                .push_item(index)
                .push_prop("resource"),
            resource,
            imports,
            resources,
        )
    }
}

fn inline_test(
    scope: &url::Url,
    spec: &mut Vec<models::TestStep>,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    for (index, step) in spec.iter_mut().enumerate() {
        let documents = match step {
            models::TestStep::Ingest(models::TestStepIngest { documents, .. })
            | models::TestStep::Verify(models::TestStepVerify { documents, .. }) => documents,
        };
        inline_config(
            Scope::new(scope).push_item(index).push_prop("documents"),
            documents,
            imports,
            resources,
        );
    }
}

fn inline_schema(
    scope: Scope,
    schema: &mut models::Schema,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    let scope = scope.flatten();
    *schema = models::Schema::new(
        serde_json::value::to_raw_value(&super::bundle_schema(&scope, schema, imports, resources))
            .unwrap()
            .into(),
    );

    // Remove all imports of the schema, as they've now been inlined into its bundle.
    let rng = imports.equal_range_by(|import| import.scope.cmp(&scope));
    imports.drain(rng);
}

fn inline_config(
    scope: Scope,
    config: &mut models::RawValue,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    match serde_json::from_str::<&str>(config.get()) {
        Ok(import) if !import.chars().any(char::is_whitespace) => {
            let scope = scope.flatten();
            let resource = scope.join(import).unwrap();

            if let Some(resource) = tables::Resource::fetch(resources, &resource) {
                *config = resource.content_dom.clone();

                // Remove the associated import.
                let rng = imports.equal_range_by(|import| {
                    import
                        .scope
                        .cmp(&scope)
                        .then(import.to_resource.cmp(&resource.resource))
                });
                assert_eq!(
                    rng.end - rng.start,
                    1,
                    "expected exactly one import from config scope {scope}"
                );
                imports.drain(rng);
            } else {
                // We failed to load the named resource. Replace with the absolute URL
                // that we *would* have loaded if we could.
                *config =
                    models::RawValue::from_string(serde_json::json!(resource).to_string()).unwrap();
            }
        }
        _ => {}
    }
}
