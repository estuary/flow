use anyhow::Context;
use futures::future::LocalBoxFuture;
use itertools::Itertools;
use proto_flow::{capture, flow, materialize};
use std::collections::BTreeMap;
use superslice::Ext;

// Bundle a source, given as a local filesystem path or a URL, into a fully inline Catalog.
pub async fn bundle(source: &str) -> anyhow::Result<models::Catalog> {
    // Resolve source to a canonicalized filesystem path or URL.
    let source = match url::Url::parse(source) {
        Ok(url) => url,
        Err(err) => {
            tracing::debug!(
                source = %source,
                ?err,
                "source is not a URL; assuming it's a filesystem path",
            );
            let source = std::fs::canonicalize(source)
                .context(format!("finding {source} in the local filesystem"))?;
            // Safe unwrap since we've canonicalized the path.
            url::Url::from_file_path(&source).unwrap()
        }
    };

    // Load all catalog sources.
    let loader = sources::Loader::new(tables::Sources::default(), crate::Fetcher {});
    loader
        .load_resource(
            sources::Scope::new(&source),
            &source,
            flow::ContentType::Catalog,
        )
        .await;
    let t = loader.into_tables();

    // Bail if errors occurred while resolving sources.
    if !t.errors.is_empty() {
        for err in t.errors.iter() {
            tracing::error!(scope = %err.scope, error = ?err.error);
        }
        anyhow::bail!("errors while loading catalog sources");
    }

    // Perform a best-effort local validation of all sources.
    let tables::Validations { errors, .. } = validation::validate(
        &flow::build_api::Config {
            build_id: "a-build-id".to_string(),
            ..Default::default()
        },
        &NoOpDrivers {},
        &t.capture_bindings,
        &t.captures,
        &t.collections,
        &t.derivations,
        &t.fetches,
        &t.imports,
        &t.materialization_bindings,
        &t.materializations,
        &t.npm_dependencies,
        &t.projections,
        &t.resources,
        &t.storage_mappings,
        &t.test_steps,
        &t.transforms,
    )
    .await;

    // Authored drafts are not expected to satisfy all referential integrity checks.
    let errors = errors
        .into_iter()
        .filter(|err| match err.error.downcast_ref() {
            // Ok if a referenced collection doesn't exist
            // (it may within the control-plane).
            Some(
                validation::Error::NoSuchEntity { ref_entity, .. }
                | validation::Error::NoSuchEntitySuggest { ref_entity, .. },
            ) if *ref_entity == "collection" => false,
            // Ok if *no* storage mappings are defined.
            // If at least one mapping is defined, then we do require that all
            // collections have appropriate mappings.
            Some(validation::Error::NoStorageMappings { .. }) => false,
            // All other validation errors bubble up as top-level errors.
            _ => true,
        })
        .collect::<Vec<_>>();

    if !errors.is_empty() {
        for tables::Error { scope, error } in errors.iter() {
            tracing::error!(%scope, ?error);
        }
        anyhow::bail!("errors while validating catalog sources");
    }

    Ok(models::Catalog {
        collections: t
            .collections
            .iter()
            .map(|c| bundled_collection(&t, c))
            .collect(),
        captures: t.captures.iter().map(|c| bundled_capture(&t, c)).collect(),
        materializations: t
            .materializations
            .iter()
            .map(|m| bundled_materialization(&t, m))
            .collect(),
        tests: bundled_tests(&t),
        ..Default::default()
    })
}

fn bundled_collection(
    t: &tables::Sources,
    collection: &tables::Collection,
) -> (models::Collection, models::CollectionDef) {
    let projections = &t.projections[t
        .projections
        .equal_range_by_key(&&collection.collection, |p| &p.collection)];

    let projections = projections
        .iter()
        .map(|p| (p.field.clone(), p.spec.clone()))
        .collect();

    (
        collection.collection.to_owned(),
        models::CollectionDef {
            schema: bundled_schema(t, &collection.schema),
            key: collection.spec.key.clone(),
            projections,
            derivation: bundled_derivation(t, &collection.collection),
            journals: collection.spec.journals.clone(),
        },
    )
}

fn bundled_derivation(
    t: &tables::Sources,
    derivation: &models::Collection,
) -> Option<models::Derivation> {
    let derivation = match t
        .derivations
        .binary_search_by_key(&derivation, |d| &d.derivation)
        .map(|ind| &t.derivations[ind])
    {
        Ok(derivation) => derivation,
        Err(_) => return None,
    };

    let typescript = match &derivation.typescript_module {
        Some(module) => {
            let module = t
                .resources
                .binary_search_by_key(&module, |r| &r.resource)
                .map(|ind| String::from_utf8_lossy(&t.resources[ind].content).to_string())
                .unwrap();

            let npm_dependencies = &t.npm_dependencies[t
                .npm_dependencies
                .equal_range_by_key(&&derivation.derivation, |d| &d.derivation)];

            let npm_dependencies = npm_dependencies
                .iter()
                .map(|p| (p.package.clone(), p.version.clone()))
                .collect();

            Some(models::TypescriptModule {
                module,
                npm_dependencies,
            })
        }
        None => None,
    };

    let transforms = &t.transforms[t
        .transforms
        .equal_range_by_key(&&derivation.derivation, |p| &p.derivation)];

    let transform = transforms
        .iter()
        .map(|p| (p.transform.clone(), bundled_transform(t, p)))
        .collect();

    Some(models::Derivation {
        register: models::Register {
            schema: bundled_schema(t, &derivation.register_schema),
            initial: derivation.spec.register.initial.clone(),
        },
        transform,
        typescript,
        shards: derivation.spec.shards.clone(),
    })
}

fn bundled_transform(t: &tables::Sources, transform: &tables::Transform) -> models::TransformDef {
    let mut out = transform.spec.clone();
    out.source.schema = match &transform.source_schema {
        Some(m) => Some(bundled_schema(t, m)),
        None => None,
    };
    out
}

fn bundled_schema(t: &tables::Sources, schema: &url::Url) -> models::Schema {
    match assemble::bundled_schema(schema, &t.imports, &t.resources) {
        serde_json::Value::Object(m) => models::Schema::Object(m),
        serde_json::Value::Bool(b) => models::Schema::Bool(b),
        _ => unreachable!("invalid schema bundle"),
    }
}

fn bundled_capture(
    t: &tables::Sources,
    capture: &tables::Capture,
) -> (models::Capture, models::CaptureDef) {
    let bindings = &t.capture_bindings[t
        .capture_bindings
        .equal_range_by_key(&&capture.capture, |p| &p.capture)];
    let bindings = bindings.iter().map(|p| p.spec.clone()).collect();

    let endpoint = match &capture.spec.endpoint {
        models::CaptureEndpoint::Connector(models::ConnectorConfig { image, config: _ }) => {
            let image = image.clone();
            let config = capture.endpoint_config.as_ref().unwrap();
            let config = t
                .resources
                .binary_search_by_key(&config, |r| &r.resource)
                .map(|ind| t.resources[ind].content_dom.clone())
                .unwrap();
            models::CaptureEndpoint::Connector(models::ConnectorConfig { image, config })
        }
        models::CaptureEndpoint::Ingest(ingest) => models::CaptureEndpoint::Ingest(ingest.clone()),
    };

    (
        capture.capture.to_owned(),
        models::CaptureDef {
            endpoint,
            bindings,
            interval: capture.spec.interval,
            shards: capture.spec.shards.clone(),
        },
    )
}

fn bundled_materialization(
    t: &tables::Sources,
    materialization: &tables::Materialization,
) -> (models::Materialization, models::MaterializationDef) {
    let bindings = &t.materialization_bindings[t
        .materialization_bindings
        .equal_range_by_key(&&materialization.materialization, |p| &p.materialization)];
    let bindings = bindings.iter().map(|p| p.spec.clone()).collect();

    let endpoint = match &materialization.spec.endpoint {
        models::MaterializationEndpoint::Connector(models::ConnectorConfig {
            image,
            config: _,
        }) => {
            let image = image.clone();
            let config = materialization.endpoint_config.as_ref().unwrap();
            let config = t
                .resources
                .binary_search_by_key(&config, |r| &r.resource)
                .map(|ind| t.resources[ind].content_dom.clone())
                .unwrap();
            models::MaterializationEndpoint::Connector(models::ConnectorConfig { image, config })
        }
        models::MaterializationEndpoint::Sqlite(sqlite) => {
            models::MaterializationEndpoint::Sqlite(sqlite.clone())
        }
    };

    (
        materialization.materialization.to_owned(),
        models::MaterializationDef {
            endpoint,
            bindings,
            shards: materialization.spec.shards.clone(),
        },
    )
}

fn bundled_tests(t: &tables::Sources) -> BTreeMap<models::Test, Vec<models::TestStep>> {
    let mut out = BTreeMap::new();

    for (test, steps) in &t.test_steps.iter().group_by(|s| &s.test) {
        let steps = steps
            .map(|step| {
                let documents = t
                    .resources
                    .binary_search_by_key(&&step.documents, |r| &r.resource)
                    .map(|ind| &t.resources[ind].content_dom)
                    .unwrap();
                let documents: Vec<models::Object> = serde_json::from_str(documents.get()).unwrap();

                match &step.spec {
                    models::TestStep::Ingest(ingest) => {
                        let mut ingest = ingest.clone();
                        ingest.documents = models::TestDocuments::Inline(documents);
                        models::TestStep::Ingest(ingest)
                    }
                    models::TestStep::Verify(verify) => {
                        let mut verify = verify.clone();
                        verify.documents = models::TestDocuments::Inline(documents);
                        models::TestStep::Verify(verify)
                    }
                }
            })
            .collect();

        out.insert(test.clone(), steps);
    }

    out
}

// NoOpDrivers are placeholders for interaction with connectors, which happen
// only within the control-plane and not within the client `flowctl` cli.
struct NoOpDrivers;

impl validation::Drivers for NoOpDrivers {
    fn validate_materialization<'a>(
        &'a self,
        request: materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<materialize::ValidateResponse, anyhow::Error>> {
        use materialize::{
            constraint::Type, validate_response::Binding, Constraint, ValidateResponse,
        };
        use std::collections::HashMap;

        Box::pin(async move {
            let response_bindings = request
                .bindings
                .into_iter()
                .enumerate()
                .map(|(i, b)| {
                    let resource_path = vec![format!("binding-{}", i)];
                    let constraints = b
                        .collection
                        .expect("collection must exist")
                        .projections
                        .into_iter()
                        .map(|proj| {
                            (
                                proj.field,
                                Constraint {
                                    r#type: Type::FieldOptional as i32,
                                    reason: "no-op validator allows everything".to_string(),
                                },
                            )
                        })
                        .collect::<HashMap<_, _>>();
                    Binding {
                        constraints,
                        resource_path,
                        delta_updates: true,
                    }
                })
                .collect::<Vec<_>>();
            Ok(ValidateResponse {
                bindings: response_bindings,
            })
        })
    }

    fn validate_capture<'a>(
        &'a self,
        request: capture::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<capture::ValidateResponse, anyhow::Error>> {
        use capture::{validate_response::Binding, ValidateResponse};
        Box::pin(async move {
            let bindings = request
                .bindings
                .into_iter()
                .enumerate()
                .map(|(i, _)| Binding {
                    resource_path: vec![format!("binding-{}", i)],
                })
                .collect::<Vec<_>>();
            Ok(ValidateResponse { bindings })
        })
    }
}
