use itertools::Itertools;
use protocol::flow;
use std::collections::BTreeMap;
use superslice::Ext;

#[derive(Debug, clap::Args)]
pub struct DraftArgs {
    /// Catalog source file or URL to build
    #[clap(long)]
    pub source: String,
}

pub fn run(DraftArgs { source }: DraftArgs) -> Result<(), anyhow::Error> {
    let source = build::source_to_url(&source)?;
    let loader = sources::Loader::new(tables::Sources::default(), super::combine::Fetcher {});

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(loader.load_resource(
        sources::Scope::new(&source),
        &source,
        flow::ContentType::Catalog,
    ));
    let t = loader.into_tables();

    if !t.errors.is_empty() {
        for err in t.errors.iter() {
            tracing::error!(scope = %err.scope, error = ?err.error);
        }
        anyhow::bail!("failed to gather catalog sources");
    }

    let catalog = models::Catalog {
        _schema: None,
        resources: BTreeMap::new(),
        import: Vec::new(),

        collections: t
            .collections
            .iter()
            .map(|collection| build_collection(&t, collection))
            .collect(),

        materializations: t
            .materializations
            .iter()
            .map(|materialization| build_materialization(&t, materialization))
            .collect(),

        captures: t
            .captures
            .iter()
            .map(|capture| build_capture(&t, capture))
            .collect(),

        tests: build_tests(&t),

        storage_mappings: t
            .storage_mappings
            .iter()
            .map(|m| {
                (
                    m.prefix.clone(),
                    models::StorageDef {
                        stores: m.stores.clone(),
                    },
                )
            })
            .collect(),
    };

    serde_json::to_writer_pretty(std::io::stdout(), &catalog)?;

    Ok(())
}

fn build_tests(t: &tables::Sources) -> BTreeMap<models::Test, Vec<models::TestStep>> {
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

fn build_capture(
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

fn build_materialization(
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

fn build_collection(
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
            schema: build_schema(t, &collection.schema),
            key: collection.spec.key.clone(),
            projections,
            derivation: build_derivation(t, &collection.collection),
            journals: collection.spec.journals.clone(),
        },
    )
}

fn build_derivation(
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
        .map(|p| (p.transform.clone(), build_transform(t, p)))
        .collect();

    Some(models::Derivation {
        register: models::Register {
            schema: build_schema(t, &derivation.register_schema),
            initial: derivation.spec.register.initial.clone(),
        },
        transform,
        typescript,
        shards: derivation.spec.shards.clone(),
    })
}

fn build_transform(t: &tables::Sources, transform: &tables::Transform) -> models::TransformDef {
    let mut out = transform.spec.clone();
    out.source.schema = match &transform.source_schema {
        Some(m) => Some(build_schema(t, m)),
        None => None,
    };
    out
}

fn build_schema(t: &tables::Sources, schema: &url::Url) -> models::Schema {
    match assemble::bundled_schema(schema, &t.imports, &t.resources) {
        serde_json::Value::Object(m) => models::Schema::Object(m),
        serde_json::Value::Bool(b) => models::Schema::Bool(b),
        _ => unreachable!("invalid schema bundle"),
    }
}
