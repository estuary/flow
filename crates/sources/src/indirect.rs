use super::Format;
use json::Scope;
use proto_flow::flow::ContentType;
use std::collections::BTreeMap;

// Indirect sub-locations within `sources` into external resources which
// are referenced through relative imports.
//
// Tasks may write a common resource only with identical content. Otherwise
// the last write would win, and every task importing the resource would
// silently get its content. Each conflicting write is an error of `draft`.
pub fn indirect_large_files(draft: &mut tables::DraftCatalog, threshold: usize) {
    let tables::DraftCatalog {
        captures,
        collections,
        fetches: _,
        imports,
        materializations,
        resources,
        tests,
        errors,
    } = draft;

    // Each resource written by this pass, and the task which first wrote it.
    let mut written = BTreeMap::new();

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
            let mut staged = tables::Resources::new();
            indirect_capture(scope, capture, model, imports, &mut staged, threshold);
            upsert_staged(scope, capture, staged, resources, &mut written, errors);
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
            let mut staged = tables::Resources::new();
            indirect_collection(scope, collection, model, imports, &mut staged, threshold);
            upsert_staged(scope, collection, staged, resources, &mut written, errors);
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
            let mut staged = tables::Resources::new();
            indirect_materialization(
                scope,
                materialization,
                model,
                imports,
                &mut staged,
                threshold,
            );
            upsert_staged(
                scope,
                materialization,
                staged,
                resources,
                &mut written,
                errors,
            );
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
            let mut staged = tables::Resources::new();
            indirect_test(scope, test, model, imports, &mut staged, threshold);
            upsert_staged(scope, test, staged, resources, &mut written, errors);
        }
    }
}

/// Upsert each resource `staged` by the task `name` into `resources`,
/// unless an earlier write of this pass gave it different content.
/// `staged` keeps every write of the task, so that the task's own writes
/// are also checked against each other.
fn upsert_staged(
    scope: &url::Url,
    name: &impl AsRef<str>,
    staged: tables::Resources,
    resources: &mut tables::Resources,
    written: &mut BTreeMap<url::Url, String>,
    errors: &mut tables::Errors,
) {
    for row in staged {
        let Some(other) = written.get(&row.resource) else {
            written.insert(row.resource.clone(), name.as_ref().to_string());
            row.upsert_if_changed(resources);
            continue;
        };
        let current = resources
            .get_key(&row.resource)
            .expect("written resources are in `resources`");

        if current.content_dom.get() != row.content_dom.get() {
            errors.insert_row(
                scope,
                anyhow::anyhow!(
                    "{} and {other} both write {}, with different content",
                    name.as_ref(),
                    row.resource,
                ),
            );
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
            indirect_custom_spec(
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
            files,
            config,
            spec,
            module,
            environment: _,
        }) => {
            let scope = Scope::new(scope);
            let scope = scope.push_prop("derive");
            let scope = scope.push_prop("using");
            let scope = scope.push_prop("typescript");

            if let Some(module) = module {
                indirect(
                    scope.push_prop("module"),
                    module,
                    &format!("{base}.ts"),
                    ContentType::Config,
                    imports,
                    resources,
                    0, // Always indirect.
                );
            }
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
            indirect_custom_spec(
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
            files,
            config,
            spec,
            module,
            dependencies: _,
            environment: _,
        }) => {
            let scope = Scope::new(scope);
            let scope = scope.push_prop("derive");
            let scope = scope.push_prop("using");
            let scope = scope.push_prop("python");

            if let Some(module) = module {
                indirect(
                    scope.push_prop("module"),
                    module,
                    &format!("{base}.py"),
                    ContentType::Config,
                    imports,
                    resources,
                    0, // Always indirect.
                );
            }
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
            indirect_custom_spec(
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

    resources.insert(tables::Resource {
        resource: resource.clone(),
        content_type,
        content,
        content_dom: std::mem::take(value),
    });

    imports.insert_row(&scope, resource);

    *value = models::RawValue::from_string(serde_json::to_string(filename).unwrap()).unwrap();
}

/// Indirect the schemas of a custom connector's `spec`, as is done for
/// collection schemas.
fn indirect_custom_spec(
    scope: Scope,
    spec: &mut models::custom_connector::Spec,
    base: &str,
    ext: &str,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
    threshold: usize,
) {
    let models::custom_connector::Spec {
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
/// Sibling tasks listing a common path write the same resource.
///
/// `files` having a path which validation rejects are left inline, so that
/// they're never written outside of the specification's directory. A file
/// without content (which failed to load) is listed, but not written.
fn indirect_files(
    scope: Scope,
    files: &mut models::custom_connector::Files,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
) {
    let models::custom_connector::Files::Inline(inline) = files else {
        return;
    };
    if !inline
        .keys()
        .all(|path| models::custom_connector::Files::is_valid_path(path))
    {
        return;
    }
    let scope = scope.push_prop("files");
    let mut paths = Vec::with_capacity(inline.len());

    for (index, (path, content)) in std::mem::take(inline).into_iter().enumerate() {
        let item = scope.push_item(index).flatten();
        let resource = item
            .join(&path)
            .expect("valid project paths are relative URLs");

        if let Some(content) = content {
            resources.insert(tables::Resource {
                resource: resource.clone(),
                content_type: ContentType::Text,
                content_dom: models::RawValue::from_string(
                    serde_json::to_string(&content).unwrap(),
                )
                .unwrap(),
                content: content.into(),
            });

            imports.insert_row(&item, resource);
        }
        paths.push(path);
    }
    *files = models::custom_connector::Files::Indirect(paths);
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
    use super::{indirect, indirect_files};
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

    #[test]
    fn tasks_conflict_over_differing_resources() {
        // Tasks of a flat layout share a directory. Both name `orders`, and
        // so write the same schema file, and both list `pyproject.toml`.
        let derive = |schema: &str, pyproject: &str| {
            serde_json::json!({
                "schema": {"const": schema},
                "key": ["/id"],
                "derive": {
                    "using": {"python": {"files": {
                        "pyproject.toml": pyproject,
                        "shared.py": "SHARED = True\n",
                    }}},
                    "transforms": [],
                },
            })
        };
        let fixture = serde_json::json!({
            "test://example/catalog.yaml": {
                "collections": {
                    "acmeCo/a/orders": derive("a", "[project]\nname = \"a\"\n"),
                    "acmeCo/b/orders": derive("b", "[project]\nname = \"b\"\n"),
                },
            },
        });
        let mut draft = crate::scenarios::evaluate_fixtures(Default::default(), &fixture);
        assert!(draft.errors.is_empty(), "{:?}", draft.errors);

        super::indirect_large_files(&mut draft, 0);

        let errors: Vec<String> = draft
            .errors
            .iter()
            .map(|tables::Error { scope, error }| format!("{scope}: {error}"))
            .collect();
        let written: Vec<(String, String)> = draft
            .resources
            .iter()
            .filter(|r| r.resource.path() != "/catalog.yaml")
            .map(|r| {
                (
                    r.resource.to_string(),
                    String::from_utf8_lossy(&r.content).to_string(),
                )
            })
            .collect();

        insta::assert_debug_snapshot!((errors, written));
    }

    #[test]
    fn files_are_written_only_at_valid_paths() {
        let scope = url::Url::parse("test://example/acmeCo/flow.yaml#/collections/acmeCo~1derived")
            .unwrap();
        let scope = Scope::new(&scope);
        let mut imports = tables::Imports::new();
        let mut resources = tables::Resources::new();

        let mut valid = models::custom_connector::Files::Inline(
            [
                (
                    "pyproject.toml".to_string(),
                    Some("[project]\n".to_string()),
                ),
                ("derived/__init__.py".to_string(), None),
            ]
            .into(),
        );
        let mut escaping = models::custom_connector::Files::Inline(
            [
                (
                    "pyproject.toml".to_string(),
                    Some("[project]\n".to_string()),
                ),
                (
                    "../escape.py".to_string(),
                    Some("pwned = True\n".to_string()),
                ),
            ]
            .into(),
        );
        indirect_files(scope, &mut valid, &mut imports, &mut resources);
        indirect_files(scope, &mut escaping, &mut imports, &mut resources);

        let written: Vec<String> = resources.iter().map(|r| r.resource.to_string()).collect();

        insta::assert_debug_snapshot!((valid, escaping, written), @r#"
        (
            Indirect(
                [
                    "derived/__init__.py",
                    "pyproject.toml",
                ],
            ),
            Inline(
                {
                    "../escape.py": Some(
                        "pwned = True\n",
                    ),
                    "pyproject.toml": Some(
                        "[project]\n",
                    ),
                },
            ),
            [
                "test://example/acmeCo/pyproject.toml",
            ],
        )
        "#);
    }
}
