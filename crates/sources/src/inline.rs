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
        inline_capture(capture, resources);
    }
    for collection in collections.iter_mut() {
        inline_collection(collection, imports, resources);
    }
    for materialization in materializations.iter_mut() {
        inline_materialization(materialization, resources);
    }
    for test in tests.iter_mut() {
        inline_test(test, resources);
    }
}

fn inline_capture(capture: &mut tables::Capture, resources: &[tables::Resource]) {
    let tables::Capture {
        scope,
        capture: _,
        spec: models::CaptureDef {
            endpoint, bindings, ..
        },
    } = capture;

    match endpoint {
        models::CaptureEndpoint::Connector(models::ConnectorConfig { config, .. }) => {
            inline_config(config, scope, resources)
        }
    }

    for models::CaptureBinding { resource, .. } in bindings {
        inline_config(resource, scope, resources)
    }
}

fn inline_collection(
    collection: &mut tables::Collection,
    imports: &[tables::Import],
    resources: &[tables::Resource],
) {
    let tables::Collection {
        scope,
        collection: _,
        spec:
            models::CollectionDef {
                schema,
                write_schema,
                read_schema,
                key: _,
                projections: _,
                journals: _,
                derive,
                derivation: _,
            },
    } = collection;

    let extend_scope = |location: &str| {
        let mut scope = scope.clone();
        scope.set_fragment(Some(&format!(
            "{}/{location}",
            scope.fragment().unwrap_or_default()
        )));
        scope
    };

    if let Some(schema) = schema {
        inline_schema(schema, &extend_scope("schema"), imports, resources)
    }
    if let Some(write_schema) = write_schema {
        inline_schema(
            write_schema,
            &extend_scope("writeSchema"),
            imports,
            resources,
        )
    }
    if let Some(read_schema) = read_schema {
        inline_schema(read_schema, &extend_scope("readSchema"), imports, resources)
    }
    if let Some(derivation) = derive {
        inline_derivation(derivation, &extend_scope("derive"), resources)
    }
}

fn inline_derivation(
    derivation: &mut models::Derivation,
    scope: &url::Url,
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
            inline_config(config, scope, resources);
        }
        models::DeriveUsing::Sqlite(models::DeriveUsingSqlite { migrations }) => {
            for foo in migrations {
                inline_config(foo, scope, resources);
            }
        }
        models::DeriveUsing::Typescript(models::DeriveUsingTypescript { module }) => {
            inline_config(module, scope, resources);
        }
    }

    for models::TransformDef {
        lambda, shuffle, ..
    } in transforms
    {
        inline_config(lambda, scope, resources);

        if let Some(models::Shuffle::Lambda(lambda)) = shuffle {
            inline_config(lambda, scope, resources);
        }
    }
}

fn inline_materialization(
    materialization: &mut tables::Materialization,
    resources: &[tables::Resource],
) {
    let tables::Materialization {
        scope,
        materialization: _,
        spec:
            models::MaterializationDef {
                endpoint,
                bindings,
                shards: _,
            },
    } = materialization;

    match endpoint {
        models::MaterializationEndpoint::Connector(models::ConnectorConfig {
            image: _,
            config,
        }) => inline_config(config, scope, resources),
        models::MaterializationEndpoint::Sqlite(models::SqliteConfig { path }) => {
            if path.starts_with(":memory:") {
                // Already absolute.
            } else if let Ok(joined) = scope.join(&path) {
                // Resolve relative database path relative to current scope.
                *path = models::RelativeUrl::new(joined.to_string());
            }
        }
    }

    for models::MaterializationBinding { resource, .. } in bindings {
        inline_config(resource, scope, resources)
    }
}

fn inline_test(test: &mut tables::Test, resources: &[tables::Resource]) {
    let tables::Test {
        scope,
        test: _,
        spec,
    } = test;

    for step in spec {
        let documents = match step {
            models::TestStep::Ingest(models::TestStepIngest { documents, .. })
            | models::TestStep::Verify(models::TestStepVerify { documents, .. }) => documents,
        };
        inline_config(documents, scope, resources);
    }
}

fn inline_schema(
    schema: &mut models::Schema,
    scope: &url::Url,
    imports: &[tables::Import],
    resources: &[tables::Resource],
) {
    *schema = models::Schema::new(
        serde_json::value::to_raw_value(&super::bundle_schema(scope, schema, imports, resources))
            .unwrap()
            .into(),
    );
}

fn inline_config(config: &mut models::RawValue, scope: &url::Url, resources: &[tables::Resource]) {
    match serde_json::from_str::<&str>(config.get()) {
        Ok(import) if !import.chars().any(char::is_whitespace) => {
            let resource = scope.join(import).unwrap();

            if let Some(resource) = tables::Resource::fetch(resources, &resource) {
                *config = resource.content_dom.clone();
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
