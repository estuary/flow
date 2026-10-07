use json::Scope;
use superslice::Ext;

pub fn inline_draft_catalog(catalog: &mut tables::DraftCatalog) {
    let tables::DraftCatalog {
        captures,
        collections,
        fetches: _,
        imports,
        materializations,
        resources,
        tests,
        errors: _,
    } = catalog;

    for capture in captures.iter_mut() {
        if let Some(model) = &mut capture.model {
            inline_capture(&capture.scope, model, imports, resources);
        }
    }
    for collection in collections.iter_mut() {
        if let Some(model) = &mut collection.model {
            inline_collection(&collection.scope, model, imports, resources);
        }
    }
    for materialization in materializations.iter_mut() {
        if let Some(model) = &mut materialization.model {
            inline_materialization(&materialization.scope, model, imports, resources);
        }
    }
    for test in tests.iter_mut() {
        if let Some(model) = &mut test.model {
            inline_test(&test.scope, model, imports, resources);
        }
    }
}

pub fn inline_capture(
    scope: &url::Url,
    model: &mut models::CaptureDef,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    let models::CaptureDef {
        endpoint, bindings, ..
    } = model;

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
        models::CaptureEndpoint::Python(models::CapturePython {
            files,
            config,
            spec,
        }) => {
            let scope = Scope::new(scope);
            let scope = scope.push_prop("endpoint");
            let scope = scope.push_prop("python");

            inline_config(scope.push_prop("config"), config, imports, resources);
            inline_files(scope.push_prop("files"), files, imports, resources);
            inline_builtin_spec(scope.push_prop("spec"), spec, imports, resources);
        }
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
    model: &mut models::CollectionDef,
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
        expect_pub_id: _,
        delete: _,
        reset: _,
    } = model;

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
        redact_salt: _,
        secrets: _,
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
        models::DeriveUsing::Typescript(models::DeriveUsingTypescript {
            files,
            config,
            spec,
            module,
        }) => {
            let scope = Scope::new(scope);
            let scope = scope.push_prop("derive");
            let scope = scope.push_prop("using");
            let scope = scope.push_prop("typescript");

            if let Some(module) = module {
                inline_config(scope.push_prop("module"), module, imports, resources);
            }
            inline_config(scope.push_prop("config"), config, imports, resources);
            inline_files(scope.push_prop("files"), files, imports, resources);
            inline_builtin_spec(scope.push_prop("spec"), spec, imports, resources);
        }
        models::DeriveUsing::Python(models::DeriveUsingPython {
            files,
            config,
            spec,
            module,
            dependencies: _,
        }) => {
            let scope = Scope::new(scope);
            let scope = scope.push_prop("derive");
            let scope = scope.push_prop("using");
            let scope = scope.push_prop("python");

            if let Some(module) = module {
                inline_config(scope.push_prop("module"), module, imports, resources);
            }
            inline_config(scope.push_prop("config"), config, imports, resources);
            inline_files(scope.push_prop("files"), files, imports, resources);
            inline_builtin_spec(scope.push_prop("spec"), spec, imports, resources);
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
    model: &mut models::MaterializationDef,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    let models::MaterializationDef {
        source: _,
        target_naming: _,
        endpoint,
        bindings,
        shards: _,
        expect_pub_id: _,
        triggers: _,
        sync_schedule: _,
        delete: _,
        reset: _,
        on_incompatible_schema_change: _,
        secrets: _,
    } = model;

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
                    .push_prop("local")
                    .push_prop("config"),
                config,
                imports,
                resources,
            )
        }
        models::MaterializationEndpoint::Dekaf(models::DekafConfig { config, .. }) => {
            inline_config(
                Scope::new(scope)
                    .push_prop("endpoint")
                    .push_prop("dekaf")
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
    model: &mut models::TestDef,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    for (index, step) in model.steps.iter_mut().enumerate() {
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
    let Some(import) = config.as_import_url() else {
        return;
    };
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
        *config = models::RawValue::from_string(serde_json::json!(resource).to_string()).unwrap();
    }
}

/// Inline the schemas of a built-in connector's `spec` into self-contained
/// bundles, as is done for collection schemas.
fn inline_builtin_spec(
    scope: Scope,
    spec: &mut models::BuiltinSpec,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    let models::BuiltinSpec {
        config_schema,
        resource_config_schema,
        oauth2: _,
    } = spec;

    for (prop, schema) in [
        ("configSchema", config_schema),
        ("resourceConfigSchema", resource_config_schema),
    ] {
        if let Some(schema) = schema {
            inline_schema(scope.push_prop(prop), schema, imports, resources);
        }
    }
}

/// Inline an indirect `files` list into the object form, mapping each
/// path to its text content. A file which failed to load maps to `None`:
/// its load error has already been recorded, and its connector may offer
/// starter content for it.
///
/// Content is the resource's own bytes, whatever its content type, as a
/// listed file may also be loaded as (say) a schema or a configuration.
fn inline_files(
    scope: Scope,
    files: &mut models::ProjectFiles,
    imports: &mut tables::Imports,
    resources: &[tables::Resource],
) {
    let models::ProjectFiles::Indirect(paths) = files else {
        return;
    };
    let mut inline = std::collections::BTreeMap::new();

    for (index, path) in paths.iter().enumerate() {
        let scope = scope.push_item(index).flatten();
        let resource = scope
            .join(path)
            .ok()
            .and_then(|resource| tables::Resource::fetch(resources, &resource));

        let Some(resource) = resource else {
            inline.insert(path.clone(), None);
            continue;
        };
        // A load error was recorded for content which isn't UTF-8.
        let content = std::str::from_utf8(&resource.content)
            .ok()
            .map(str::to_string);
        inline.insert(path.clone(), content);

        let rng = imports.equal_range_by(|import| {
            import
                .scope
                .cmp(&scope)
                .then(import.to_resource.cmp(&resource.resource))
        });
        imports.drain(rng);
    }
    *files = models::ProjectFiles::Inline(inline);
}

#[cfg(test)]
mod test {
    use json::Scope;

    #[test]
    fn files_are_inlined_as_text_whatever_their_content_type() {
        let scope =
            url::Url::parse("test://example/flow.yaml#/captures/acmeCo~1source-acme").unwrap();
        let mut imports = tables::Imports::new();
        let mut resources = tables::Resources::new();

        // A listed file which was also loaded as a schema has a parsed DOM.
        resources.insert_row(
            url::Url::parse("test://example/config.schema.yaml").unwrap(),
            proto_flow::flow::ContentType::JsonSchema,
            bytes::Bytes::from_static(b"type: object\n"),
            models::RawValue::from_str(r#"{"type":"object"}"#).unwrap(),
        );
        resources.insert_row(
            url::Url::parse("test://example/pyproject.toml").unwrap(),
            proto_flow::flow::ContentType::Text,
            bytes::Bytes::from_static(b"[project]\n"),
            models::RawValue::from_str(r#""[project]\n""#).unwrap(),
        );
        let mut files = models::ProjectFiles::Indirect(vec![
            "pyproject.toml".to_string(),
            "config.schema.yaml".to_string(),
            "missing.py".to_string(),
        ]);
        super::inline_files(
            Scope::new(&scope).push_prop("files"),
            &mut files,
            &mut imports,
            &resources,
        );

        insta::assert_debug_snapshot!(files, @r#"
        Inline(
            {
                "config.schema.yaml": Some(
                    "type: object\n",
                ),
                "missing.py": None,
                "pyproject.toml": Some(
                    "[project]\n",
                ),
            },
        )
        "#);
    }
}
