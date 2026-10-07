use super::Format;
use json::Scope;
use proto_flow::flow::ContentType;
use std::collections::BTreeMap;

// Indirect sub-locations within `sources` into external resources which
// are referenced through relative imports.
pub fn indirect_large_files(draft: &mut tables::DraftCatalog, threshold: usize) {
    let tables::DraftCatalog {
        captures,
        collections,
        fetches: _,
        imports,
        materializations,
        resources,
        tests,
        errors: _,
    } = draft;

    for tables::DraftCapture {
        capture,
        scope,
        expect_pub_id: _,
        data_plane_id: _,
        model,
        is_touch: _,
    } in captures.iter_mut()
    {
        if let Some(model) = model {
            indirect_capture(scope, capture, model, imports, resources, threshold);
        }
    }
    for tables::DraftCollection {
        collection,
        scope,
        expect_pub_id: _,
        data_plane_id: _,
        model,
        is_touch: _,
    } in collections.iter_mut()
    {
        if let Some(model) = model {
            indirect_collection(scope, collection, model, imports, resources, threshold);
        }
    }
    for tables::DraftMaterialization {
        materialization,
        scope,
        expect_pub_id: _,
        data_plane_id: _,
        model,
        is_touch: _,
    } in materializations.iter_mut()
    {
        if let Some(model) = model {
            indirect_materialization(scope, materialization, model, imports, resources, threshold);
        }
    }
    for tables::DraftTest {
        test,
        scope,
        expect_pub_id: _,
        model,
        is_touch: _,
    } in tests.iter_mut()
    {
        if let Some(model) = model {
            indirect_test(scope, test, model, imports, resources, threshold);
        }
    }
}

// Extend Resources with Resource instances for each catalog specification
// URL which is referenced by any and all imports, captures, collections,
// materializations, and tests.
pub fn rebuild_catalog_resources(draft: &mut tables::DraftCatalog) {
    let tables::DraftCatalog {
        captures,
        collections,
        fetches: _,
        imports,
        materializations,
        resources,
        tests,
        errors: _,
    } = draft;

    let mut catalogs: BTreeMap<url::Url, models::Catalog> = BTreeMap::new();

    let strip_scope = |scope: &url::Url| {
        let mut scope = scope.clone();
        scope.set_fragment(None);
        scope
    };

    for tables::Import { scope, to_resource } in imports.iter() {
        if !scope.fragment().unwrap().starts_with("/import") {
            continue; // Skip implicit imports.
        }
        let scope = strip_scope(scope);
        let import = match scope.make_relative(&to_resource) {
            Some(rel) => rel,
            None => to_resource.to_string(),
        };

        let entry = catalogs.entry(scope).or_default();
        entry.import.push(models::RelativeUrl::new(import));
    }

    for tables::DraftCapture {
        capture,
        scope,
        expect_pub_id,
        data_plane_id: _,
        model,
        is_touch: _,
    } in captures.iter()
    {
        if let Some(model) = model {
            let entry = catalogs.entry(strip_scope(scope)).or_default();
            let mut model = model.clone();
            model.expect_pub_id = expect_pub_id.clone();
            entry.captures.insert(capture.clone(), model);
        }
    }

    for tables::DraftCollection {
        collection,
        scope,
        expect_pub_id,
        data_plane_id: _,
        model,
        is_touch: _,
    } in collections.iter()
    {
        if let Some(model) = model {
            let entry = catalogs.entry(strip_scope(scope)).or_default();
            let mut model = model.clone();
            model.expect_pub_id = expect_pub_id.clone();
            entry.collections.insert(collection.clone(), model);
        }
    }

    for tables::DraftMaterialization {
        materialization,
        scope,
        expect_pub_id,
        data_plane_id: _,
        model,
        is_touch: _,
    } in materializations.iter()
    {
        if let Some(model) = model {
            let entry = catalogs.entry(strip_scope(scope)).or_default();
            let mut model = model.clone();
            model.expect_pub_id = expect_pub_id.clone();
            entry
                .materializations
                .insert(materialization.clone(), model);
        }
    }

    for tables::DraftTest {
        test,
        scope,
        expect_pub_id,
        model,
        is_touch: _,
    } in tests.iter()
    {
        if let Some(model) = model {
            let entry = catalogs.entry(strip_scope(scope)).or_default();
            let mut model = model.clone();
            model.expect_pub_id = expect_pub_id.clone();
            entry.tests.insert(test.clone(), model);
        }
    }

    for (resource, mut catalog) in catalogs {
        catalog.import.sort();
        catalog.import.dedup();

        let content_dom: models::RawValue =
            serde_json::value::to_raw_value(&catalog).unwrap().into();
        let content_raw = Format::from_scope(&resource).serialize(&content_dom);

        tables::Resource {
            resource,
            content_dom,
            content: content_raw.into(),
            content_type: ContentType::Catalog,
        }
        .upsert_if_changed(resources)
    }
}

fn indirect_capture(
    scope: &url::Url,
    capture: &models::Capture,
    model: &mut models::CaptureDef,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
    threshold: usize,
) {
    let models::CaptureDef {
        endpoint, bindings, ..
    } = model;
    let (base, ext) = (base_name(capture), Format::from_scope(scope).extension());

    let (variant, config) = match endpoint {
        models::CaptureEndpoint::Connector(models::ConnectorConfig { config, .. }) => {
            ("connector", config)
        }
        models::CaptureEndpoint::Local(models::LocalConfig { config, .. }) => ("local", config),
        models::CaptureEndpoint::Python(models::CapturePython {
            files,
            config,
            spec,
        }) => {
            let scope = Scope::new(scope);
            let scope = scope.push_prop("endpoint");
            let scope = scope.push_prop("python");

            indirect_files(scope, files, imports, resources);
            indirect_builtin_spec(
                scope.push_prop("spec"),
                spec,
                base,
                ext,
                imports,
                resources,
                threshold,
            );
            ("python", config)
        }
    };
    indirect(
        Scope::new(scope)
            .push_prop("endpoint")
            .push_prop(variant)
            .push_prop("config"),
        config,
        &format!("{base}.config.{ext}"),
        ContentType::Config,
        imports,
        resources,
        threshold,
    );

    for (index, models::CaptureBinding { resource, .. }) in bindings.iter_mut().enumerate() {
        indirect(
            Scope::new(scope)
                .push_prop("bindings")
                .push_item(index)
                .push_prop("resource"),
            resource,
            &format!("{base}.resource.{index}.config.{ext}"),
            ContentType::Config,
            imports,
            resources,
            threshold,
        )
    }
}

fn indirect_collection(
    scope: &url::Url,
    collection: &models::Collection,
    model: &mut models::CollectionDef,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
    threshold: usize,
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
    let (base, ext) = (base_name(collection), Format::from_scope(scope).extension());

    for (prop, schema, filename) in [
        ("schema", schema, format!("{base}.schema.{ext}")),
        (
            "writeSchema",
            write_schema,
            format!("{base}.write.schema.{ext}"),
        ),
        (
            "readSchema",
            read_schema,
            format!("{base}.read.schema.{ext}"),
        ),
    ] {
        let Some(schema) = schema else {
            continue;
        };
        strip_file_id(schema);

        indirect(
            Scope::new(scope).push_prop(prop),
            schema,
            &filename,
            ContentType::JsonSchema,
            imports,
            resources,
            threshold,
        );
    }
    if let Some(derivation) = derive {
        indirect_derivation(scope, derivation, base, imports, resources, threshold);
    }
}

fn indirect_derivation(
    scope: &url::Url,
    derivation: &mut models::Derivation,
    base: &str,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
    threshold: usize,
) {
    let models::Derivation {
        using,
        transforms,
        shuffle_key_types: _,
        shards: _,
        redact_salt: _,
        secrets: _,
    } = derivation;
    let ext = Format::from_scope(scope).extension();
    let mut is_sql = false;

    match using {
        models::DeriveUsing::Connector(models::ConnectorConfig { config, .. }) => {
            indirect(
                Scope::new(scope)
                    .push_prop("derive")
                    .push_prop("using")
                    .push_prop("connector")
                    .push_prop("config"),
                config,
                &format!("{base}.config.{ext}"),
                ContentType::Config,
                imports,
                resources,
                threshold,
            );
        }
        models::DeriveUsing::Local(models::LocalConfig { config, .. }) => {
            indirect(
                Scope::new(scope)
                    .push_prop("derive")
                    .push_prop("using")
                    .push_prop("local")
                    .push_prop("config"),
                config,
                &format!("{base}.config.{ext}"),
                ContentType::Config,
                imports,
                resources,
                threshold,
            );
        }
        models::DeriveUsing::Sqlite(models::DeriveUsingSqlite { migrations }) => {
            is_sql = true;

            for (index, migration) in migrations.iter_mut().enumerate() {
                indirect(
                    Scope::new(scope)
                        .push_prop("derive")
                        .push_prop("using")
                        .push_prop("sqlite")
                        .push_prop("migrations")
                        .push_item(index),
                    migration,
                    &format!("{base}.migration.{index}.sql"),
                    ContentType::Config,
                    imports,
                    resources,
                    threshold,
                );
            }
        }
        models::DeriveUsing::Typescript(models::DeriveUsingTypescript {
            module,
            config,
            spec,
        }) => {
            let scope = Scope::new(scope);
            let scope = scope.push_prop("derive");
            let scope = scope.push_prop("using");
            let scope = scope.push_prop("typescript");

            indirect(
                scope.push_prop("module"),
                module,
                &format!("{base}.ts"),
                ContentType::Config,
                imports,
                resources,
                0, // Always indirect.
            );
            indirect(
                scope.push_prop("config"),
                config,
                &format!("{base}.config.{ext}"),
                ContentType::Config,
                imports,
                resources,
                threshold,
            );
            indirect_builtin_spec(
                scope.push_prop("spec"),
                spec,
                base,
                ext,
                imports,
                resources,
                threshold,
            );
        }
        models::DeriveUsing::Python(models::DeriveUsingPython {
            module,
            files,
            config,
            spec,
            dependencies: _,
        }) => {
            let scope = Scope::new(scope);
            let scope = scope.push_prop("derive");
            let scope = scope.push_prop("using");
            let scope = scope.push_prop("python");

            indirect(
                scope.push_prop("module"),
                module,
                &format!("{base}.py"),
                ContentType::Config,
                imports,
                resources,
                0, // Always indirect.
            );
            indirect(
                scope.push_prop("config"),
                config,
                &format!("{base}.config.{ext}"),
                ContentType::Config,
                imports,
                resources,
                threshold,
            );
            indirect_files(scope, files, imports, resources);
            indirect_builtin_spec(
                scope.push_prop("spec"),
                spec,
                base,
                ext,
                imports,
                resources,
                threshold,
            );
        }
    }

    for (
        index,
        models::TransformDef {
            name,
            lambda,
            shuffle,
            ..
        },
    ) in transforms.iter_mut().enumerate()
    {
        // SQL lambdas are written as SQL files, and all others as documents.
        let lambda_ext = if is_sql { "sql" } else { ext };

        indirect(
            Scope::new(scope)
                .push_prop("derive")
                .push_prop("transforms")
                .push_item(index)
                .push_prop("lambda"),
            lambda,
            &format!("{base}.lambda.{name}.{lambda_ext}"),
            ContentType::Config,
            imports,
            resources,
            threshold,
        );
        if let models::Shuffle::Lambda(lambda) = shuffle {
            indirect(
                Scope::new(scope)
                    .push_prop("derive")
                    .push_prop("transforms")
                    .push_item(index)
                    .push_prop("shuffle")
                    .push_prop("lambda"),
                lambda,
                &format!("{base}.lambda.{name}.shuffle.{lambda_ext}"),
                ContentType::Config,
                imports,
                resources,
                threshold,
            );
        }
    }
}

fn indirect_materialization(
    scope: &url::Url,
    materialization: &models::Materialization,
    model: &mut models::MaterializationDef,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
    threshold: usize,
) {
    let models::MaterializationDef {
        endpoint, bindings, ..
    } = model;
    let (base, ext) = (
        base_name(materialization),
        Format::from_scope(scope).extension(),
    );

    let (variant, config) = match endpoint {
        models::MaterializationEndpoint::Connector(models::ConnectorConfig { config, .. }) => {
            ("connector", config)
        }
        models::MaterializationEndpoint::Local(models::LocalConfig { config, .. }) => {
            ("local", config)
        }
        models::MaterializationEndpoint::Dekaf(models::DekafConfig { config, .. }) => {
            ("dekaf", config)
        }
    };
    indirect(
        Scope::new(scope)
            .push_prop("endpoint")
            .push_prop(variant)
            .push_prop("config"),
        config,
        &format!("{base}.config.{ext}"),
        ContentType::Config,
        imports,
        resources,
        threshold,
    );

    for (index, models::MaterializationBinding { resource, .. }) in bindings.iter_mut().enumerate()
    {
        indirect(
            Scope::new(scope)
                .push_prop("bindings")
                .push_item(index)
                .push_prop("resource"),
            resource,
            &format!("{base}.resource.{index}.config.{ext}"),
            ContentType::Config,
            imports,
            resources,
            threshold,
        )
    }
}

fn indirect_test(
    scope: &url::Url,
    test: &models::Test,
    model: &mut models::TestDef,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
    threshold: usize,
) {
    let (base, ext) = (base_name(test), Format::from_scope(scope).extension());

    for (index, step) in model.steps.iter_mut().enumerate() {
        let documents = match step {
            models::TestStep::Ingest(models::TestStepIngest { documents, .. })
            | models::TestStep::Verify(models::TestStepVerify { documents, .. }) => documents,
        };
        indirect(
            Scope::new(scope).push_item(index).push_prop("documents"),
            documents,
            &format!("{base}.step.{index}.{ext}"),
            ContentType::Config,
            imports,
            resources,
            threshold,
        );
    }
}

/// Remove a superfluous `file://` $id of a schema, which would otherwise
/// be written into its indirect resource.
fn strip_file_id(schema: &mut models::RawValue) {
    let serde_json::Value::Object(mut m) = schema.to_value() else {
        return;
    };
    if m.contains_key("definitions") || m.contains_key("$defs") {
        // We can't touch $id, as it provides the canonical base against which
        // $ref is resolved to definitions.
        return;
    }
    if let Some(true) = m
        .get("$id")
        .and_then(serde_json::Value::as_str)
        .map(|s| s.starts_with("file://"))
    {
        m.remove("$id");
        *schema = models::RawValue::from_value(&serde_json::Value::Object(m));
    }
}

/// Indirect `value` into a resource at `filename`, relative to `scope`,
/// and replace `value` with that relative import.
///
/// A `filename` with a JSON or YAML extension is written as a document of
/// that format. Otherwise `value` is written as text and must be a string,
/// or it's left inline.
///
/// Values which are already imports, or whose JSON encoding isn't longer
/// than `threshold`, are left as-is.
fn indirect(
    scope: Scope,
    value: &mut models::RawValue,
    filename: &str,
    content_type: ContentType,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
    threshold: usize,
) {
    if value.as_import_url().is_some() || value.get().len() <= threshold {
        return;
    }
    let scope = scope.flatten();
    let resource = scope.join(filename).unwrap();

    let content: bytes::Bytes = if crate::is_dom_path(filename) {
        Format::from_scope(&resource).serialize(value).into()
    } else if let Ok(text) = serde_json::from_str::<String>(value.get()) {
        text.into()
    } else {
        return;
    };

    tables::Resource {
        resource: resource.clone(),
        content_type,
        content,
        content_dom: std::mem::take(value),
    }
    .upsert_if_changed(resources);

    imports.insert_row(&scope, resource);

    *value = models::RawValue::from_string(serde_json::to_string(filename).unwrap()).unwrap();
}

/// Indirect the schemas of a built-in connector's `spec`, as is done for
/// collection schemas.
fn indirect_builtin_spec(
    scope: Scope,
    spec: &mut models::BuiltinSpec,
    base: &str,
    ext: &str,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
    threshold: usize,
) {
    let models::BuiltinSpec {
        config_schema,
        resource_config_schema,
        oauth2: _,
    } = spec;

    for (prop, schema, filename) in [
        (
            "configSchema",
            config_schema,
            format!("{base}.config.schema.{ext}"),
        ),
        (
            "resourceConfigSchema",
            resource_config_schema,
            format!("{base}.resource.schema.{ext}"),
        ),
    ] {
        let Some(schema) = schema else {
            continue;
        };
        strip_file_id(schema);

        indirect(
            scope.push_prop(prop),
            schema,
            &filename,
            ContentType::JsonSchema,
            imports,
            resources,
            threshold,
        );
    }
}

/// Indirect an inline `files` object of the task at `scope` into the array
/// form, writing each file as text at its path relative to the specification.
/// Sibling tasks listing a common path write the same resource, which
/// validation requires to have identical content.
fn indirect_files(
    scope: Scope,
    files: &mut models::ProjectFiles,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
) {
    let models::ProjectFiles::Inline(inline) = files else {
        return;
    };
    let scope = scope.push_prop("files");
    let mut paths = Vec::with_capacity(inline.len());

    for (index, (path, content)) in std::mem::take(inline).into_iter().enumerate() {
        let item = scope.push_item(index).flatten();
        let Ok(resource) = item.join(&path) else {
            continue;
        };

        tables::Resource {
            resource: resource.clone(),
            content_type: ContentType::Text,
            content_dom: models::RawValue::from_string(serde_json::to_string(&content).unwrap())
                .unwrap(),
            content: content.into(),
        }
        .upsert_if_changed(resources);

        imports.insert_row(&item, resource);
        paths.push(path);
    }
    *files = models::ProjectFiles::Indirect(paths);
}

fn base_name(name: &impl AsRef<str>) -> &str {
    let name = name.as_ref();

    match name.rsplit_once("/") {
        Some((_, base)) => base,
        None => name,
    }
}

#[cfg(test)]
mod test {
    use super::indirect;
    use json::Scope;
    use proto_flow::flow::ContentType;

    #[test]
    fn values_are_written_by_their_extension() {
        let scope =
            url::Url::parse("test://example/catalog.yaml#/collections/acmeCo~1orders").unwrap();
        let mut imports = tables::Imports::new();
        let mut resources = tables::Resources::new();

        let values: Vec<(&str, String)> = [
            ("lib/__init__.py", r#""""#),
            ("lib/already.py", r#""lib/already.py""#),
            ("lib/geo.py", r#""def region_for(doc):\n    return doc\n""#),
            ("data/regions.json", r#"{"north":1,"south":2}"#),
            ("data/config.yaml", r#"{"labels":["alpha"],"retries":3}"#),
            ("data/notjson", r#"{"not":"text"}"#),
        ]
        .into_iter()
        .map(|(key, value)| {
            let mut value = models::RawValue::from_str(value).unwrap();
            indirect(
                Scope::new(&scope),
                &mut value,
                key,
                ContentType::Config,
                &mut imports,
                &mut resources,
                0,
            );
            (key, value.get().to_string())
        })
        .collect();

        let written: Vec<(String, String)> = resources
            .iter()
            .map(|resource| {
                (
                    resource.resource.to_string(),
                    String::from_utf8(resource.content.to_vec()).unwrap(),
                )
            })
            .collect();

        insta::assert_debug_snapshot!((values, written, imports));
    }
}
