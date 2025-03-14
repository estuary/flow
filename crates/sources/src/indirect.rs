use super::Format;
use crate::Scope;
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
    let base = base_name(capture);

    match endpoint {
        models::CaptureEndpoint::Connector(models::ConnectorConfig { config, .. }) => {
            indirect_dom(
                Scope::new(scope)
                    .push_prop("endpoint")
                    .push_prop("connector")
                    .push_prop("config"),
                config,
                ContentType::Config,
                format!("{base}.config"),
                imports,
                resources,
                threshold,
            );
        }
        models::CaptureEndpoint::Local(models::LocalConfig { config, .. }) => {
            indirect_dom(
                Scope::new(scope)
                    .push_prop("endpoint")
                    .push_prop("local")
                    .push_prop("config"),
                config,
                ContentType::Config,
                format!("{base}.config"),
                imports,
                resources,
                threshold,
            );
        }
    }

    for (index, models::CaptureBinding { resource, .. }) in bindings.iter_mut().enumerate() {
        indirect_dom(
            Scope::new(scope)
                .push_prop("bindings")
                .push_item(index)
                .push_prop("resource"),
            resource,
            ContentType::Config,
            format!("{base}.resource.{index}.config"),
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
    let base = base_name(collection);

    if let Some(schema) = schema {
        indirect_schema(
            Scope::new(scope).push_prop("schema"),
            schema,
            format!("{base}.schema"),
            imports,
            resources,
            threshold,
        );
    }
    if let Some(write_schema) = write_schema {
        indirect_schema(
            Scope::new(scope).push_prop("writeSchema"),
            write_schema,
            format!("{base}.write.schema"),
            imports,
            resources,
            threshold,
        )
    }
    if let Some(read_schema) = read_schema {
        indirect_schema(
            Scope::new(scope).push_prop("readSchema"),
            read_schema,
            format!("{base}.read.schema"),
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
    } = derivation;
    let mut is_sql = false;

    match using {
        models::DeriveUsing::Connector(models::ConnectorConfig { config, .. }) => {
            indirect_dom(
                Scope::new(scope)
                    .push_prop("derive")
                    .push_prop("using")
                    .push_prop("connector")
                    .push_prop("config"),
                config,
                ContentType::Config,
                format!("{base}.config"),
                imports,
                resources,
                threshold,
            );
        }
        models::DeriveUsing::Local(models::LocalConfig { config, .. }) => {
            indirect_dom(
                Scope::new(scope)
                    .push_prop("derive")
                    .push_prop("using")
                    .push_prop("local")
                    .push_prop("config"),
                config,
                ContentType::Config,
                format!("{base}.config"),
                imports,
                resources,
                threshold,
            );
        }
        models::DeriveUsing::Sqlite(models::DeriveUsingSqlite { migrations }) => {
            is_sql = true;

            for (index, migration) in migrations.iter_mut().enumerate() {
                indirect_raw(
                    Scope::new(scope)
                        .push_prop("derive")
                        .push_prop("using")
                        .push_prop("sqlite")
                        .push_prop("migrations")
                        .push_item(index),
                    migration,
                    format!("{base}.migration.{index}.sql"),
                    imports,
                    resources,
                    threshold,
                );
            }
        }
        models::DeriveUsing::Typescript(models::DeriveUsingTypescript { module }) => {
            indirect_raw(
                Scope::new(scope)
                    .push_prop("derive")
                    .push_prop("using")
                    .push_prop("typescript")
                    .push_prop("module"),
                module,
                format!("{base}.ts"),
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
        if is_sql {
            indirect_raw(
                Scope::new(scope)
                    .push_prop("derive")
                    .push_prop("transforms")
                    .push_item(index)
                    .push_prop("lambda"),
                lambda,
                format!("{base}.lambda.{name}.sql"),
                imports,
                resources,
                threshold,
            );
            if let models::Shuffle::Lambda(lambda) = shuffle {
                indirect_raw(
                    Scope::new(scope)
                        .push_prop("derive")
                        .push_prop("transforms")
                        .push_item(index)
                        .push_prop("shuffle")
                        .push_prop("lambda"),
                    lambda,
                    format!("{base}.lambda.{name}.shuffle.sql"),
                    imports,
                    resources,
                    threshold,
                );
            }
        } else {
            indirect_dom(
                Scope::new(scope)
                    .push_prop("derive")
                    .push_prop("transforms")
                    .push_item(index)
                    .push_prop("lambda"),
                lambda,
                ContentType::Config,
                format!("{base}.lambda.{name}"),
                imports,
                resources,
                threshold,
            );
            if let models::Shuffle::Lambda(lambda) = shuffle {
                indirect_dom(
                    Scope::new(scope)
                        .push_prop("derive")
                        .push_prop("transforms")
                        .push_item(index)
                        .push_prop("shuffle")
                        .push_prop("lambda"),
                    lambda,
                    ContentType::Config,
                    format!("{base}.lambda.{name}.shuffle"),
                    imports,
                    resources,
                    threshold,
                );
            }
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
    let base = base_name(materialization);

    match endpoint {
        models::MaterializationEndpoint::Connector(models::ConnectorConfig { config, .. }) => {
            indirect_dom(
                Scope::new(scope)
                    .push_prop("endpoint")
                    .push_prop("connector")
                    .push_prop("config"),
                config,
                ContentType::Config,
                format!("{base}.config"),
                imports,
                resources,
                threshold,
            )
        }
        models::MaterializationEndpoint::Local(models::LocalConfig { config, .. }) => indirect_dom(
            Scope::new(scope)
                .push_prop("endpoint")
                .push_prop("local")
                .push_prop("config"),
            config,
            ContentType::Config,
            format!("{base}.config"),
            imports,
            resources,
            threshold,
        ),
        models::MaterializationEndpoint::Dekaf(models::DekafConfig { config, .. }) => indirect_dom(
            Scope::new(scope)
                .push_prop("endpoint")
                .push_prop("dekaf")
                .push_prop("config"),
            config,
            ContentType::Config,
            format!("{base}.config"),
            imports,
            resources,
            threshold,
        ),
    }

    for (index, models::MaterializationBinding { resource, .. }) in bindings.iter_mut().enumerate()
    {
        indirect_dom(
            Scope::new(scope)
                .push_prop("bindings")
                .push_item(index)
                .push_prop("resource"),
            resource,
            ContentType::Config,
            format!("{base}.resource.{index}.config"),
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
    let base = base_name(test);

    for (index, step) in model.steps.iter_mut().enumerate() {
        let documents = match step {
            models::TestStep::Ingest(models::TestStepIngest { documents, .. })
            | models::TestStep::Verify(models::TestStepVerify { documents, .. }) => documents,
        };
        indirect_dom(
            Scope::new(scope).push_item(index).push_prop("documents"),
            documents,
            ContentType::Config,
            format!("{base}.step.{index}"),
            imports,
            resources,
            threshold,
        );
    }
}

fn indirect_schema(
    scope: Scope,
    content_dom: &mut models::RawValue,
    filename: String,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
    threshold: usize,
) {
    let schema = content_dom.to_value();

    // Attempt to clean up the schema by removing a superfluous $id.
    match schema {
        serde_json::Value::Object(mut m) => {
            if m.contains_key("definitions") || m.contains_key("$defs") {
                // We can't touch $id, as it provides the canonical base against which
                // $ref is resolved to definitions.
            } else if let Some(true) = m
                .get("$id")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.starts_with("file://"))
            {
                m.remove("$id");
                *content_dom = models::RawValue::from_value(&serde_json::Value::Object(m))
            }
        }
        _ => (),
    };

    indirect_dom(
        scope,
        content_dom,
        ContentType::JsonSchema,
        filename,
        imports,
        resources,
        threshold,
    )
}

fn indirect_dom(
    scope: Scope,
    content_dom: &mut models::RawValue,
    content_type: ContentType,
    filename: String,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
    threshold: usize,
) {
    if content_dom.get().len() <= threshold {
        // Leave small DOMs in-place.
        // This includes content_dom's which are already indirect.
        return;
    }
    let scope = scope.flatten();

    let fmt = Format::from_scope(&scope);
    let filename = format!("{filename}.{}", fmt.extension());

    tables::Resource {
        resource: scope.join(&filename).unwrap(),
        content_type,
        content: fmt.serialize(content_dom).into(),
        content_dom: content_dom.clone(),
    }
    .upsert_if_changed(resources);

    imports.insert_row(&scope, scope.join(&filename).unwrap());

    *content_dom =
        models::RawValue::from_string(serde_json::to_string(&filename).unwrap()).unwrap();
}

fn indirect_raw(
    scope: Scope,
    content_dom: &mut models::RawValue,
    filename: String,
    imports: &mut tables::Imports,
    resources: &mut tables::Resources,
    threshold: usize,
) {
    if content_dom.get().len() <= threshold {
        // Leave small raw strings in-place.
        // This includes content_dom's which are already indirect.
        return;
    }
    let scope = scope.flatten();

    let content_str =
        serde_json::from_str::<String>(content_dom.get()).expect("value must be a JSON string");

    tables::Resource {
        resource: scope.join(&filename).unwrap(),
        content_type: ContentType::Config,
        content: content_str.into(),
        content_dom: std::mem::take(content_dom),
    }
    .upsert_if_changed(resources);

    imports.insert_row(&scope, scope.join(&filename).unwrap());

    *content_dom =
        models::RawValue::from_string(serde_json::to_string(&filename).unwrap()).unwrap();
}

fn base_name(name: &impl AsRef<str>) -> &str {
    let name = name.as_ref();

    match name.rsplit_once("/") {
        Some((_, base)) => base,
        None => name,
    }
}
