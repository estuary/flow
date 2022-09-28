use super::Scope;
use anyhow::Context;
use doc::Schema as CompiledSchema;
use futures::future::{FutureExt, LocalBoxFuture};
use json::schema::{build::build_schema, Application, Keyword};
use proto_flow::flow;
use serde_json::value::RawValue;
use std::cell::RefCell;
use std::collections::BTreeMap;
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum LoadError {
    #[error("failed to parse URL")]
    URLParse(#[from] url::ParseError),
    #[error("failed to fetch resource {uri}")]
    Fetch {
        uri: String,
        #[source]
        detail: anyhow::Error,
    },
    #[error("failed to parse configuration document (https://go.estuary.dev/qpSUoq)")]
    ConfigParseErr(#[source] serde_json::Error),
    #[error("failed to parse document fixtures (https://go.estuary.dev/NGT3es)")]
    DocumentFixturesParseErr(#[source] serde_json::Error),
    #[error("failed to parse document ({})", .0)]
    JsonErr(#[from] serde_json::Error),
    #[error("failed to parse document")]
    YAMLErr(#[from] serde_yaml::Error),
    #[error("failed to merge YAML alias nodes")]
    YAMLMergeErr(#[from] yaml_merge_keys::MergeKeyError),
    #[error("failed to build JSON schema")]
    SchemaBuild(#[from] json::schema::BuildError),
    #[error("failed to index JSON schema")]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error("resources cannot have fragments")]
    ResourceWithFragment,
}

// FetchResult is the result type of a fetch operation,
// and returns the resolved content of the resource.
pub type FetchResult = Result<bytes::Bytes, anyhow::Error>;
// FetchFuture is a Future of FetchResult.
pub type FetchFuture<'a> = LocalBoxFuture<'a, FetchResult>;

/// Fetcher resolves a resource URL to its byte content.
pub trait Fetcher {
    fn fetch<'a>(
        &'a self,
        // Resource to fetch.
        resource: &'a Url,
        // Expected content type of the resource.
        content_type: flow::ContentType,
    ) -> FetchFuture<'a>;
}

/// Loader provides a stack-based driver for traversing catalog source
/// models, with dispatch to a Visitor trait and having fine-grained
/// tracking of location context.
pub struct Loader<F: Fetcher> {
    // Inlined resource definitions which have been observed, but not loaded.
    inlined: RefCell<BTreeMap<Url, models::ResourceDef>>,
    // Tables loaded by the build process.
    tables: RefCell<tables::Sources>,
    // Fetcher for retrieving discovered, unvisited resources.
    fetcher: F,
}

impl<F: Fetcher> Loader<F> {
    /// Build and return a new Loader.
    pub fn new(tables: tables::Sources, fetcher: F) -> Loader<F> {
        Loader {
            inlined: RefCell::new(BTreeMap::new()),
            tables: RefCell::new(tables),
            fetcher,
        }
    }

    pub fn into_tables(self) -> tables::Sources {
        self.tables.into_inner()
    }

    /// Load (or re-load) a resource of the given ContentType.
    pub async fn load_resource<'a>(
        &'a self,
        scope: Scope<'a>,
        resource: &'a Url,
        content_type: flow::ContentType,
    ) {
        if resource.fragment().is_some() {
            self.tables.borrow_mut().errors.insert_row(
                &scope.flatten(),
                anyhow::anyhow!(LoadError::Fetch {
                    uri: resource.to_string(),
                    detail: LoadError::ResourceWithFragment.into(),
                }),
            );
            return;
        }

        // Mark as visited, so that recursively-loaded imports don't re-visit.
        self.tables
            .borrow_mut()
            .fetches
            .insert_row(scope.resource_depth() as u32, resource);

        let inlined = self.inlined.borrow_mut().remove(&resource); // Don't hold guard.

        let content : Result<bytes::Bytes, anyhow::Error> = match inlined {
            // Resource has an inline definition of the expected content-type.
            Some(models::ResourceDef{content, content_type: expected_type}) if proto_content_type(expected_type) == content_type => {
                if content.get().chars().next().unwrap() == '"' {
                    base64::decode(content.get()).context("base64-decode of inline resource failed").map(Into::into)
                } else {
                    Ok(content.get().as_bytes().to_owned().into())
                }
            }
            // Resource has an inline definition of the _wrong_ type.
            Some(models::ResourceDef{content_type: expected_type, ..}) => {
                Err(anyhow::anyhow!("inline resource has content-type {expected_type:?}, not the requested {content_type:?}"))
            }
            // No inline definition. Delegate to the Fetcher.
            None => self.fetcher.fetch(&resource, content_type.into()).await,
        };

        match content {
            Ok(content) => {
                self.load_resource_content(scope, resource, content, content_type)
                    .await
            }
            Err(err) => {
                self.tables.borrow_mut().errors.insert_row(
                    &scope.flatten(),
                    anyhow::anyhow!(LoadError::Fetch {
                        uri: resource.to_string(),
                        detail: err,
                    }),
                );
            }
        }
    }

    // Resources are loaded recursively, and Rust requires that recursive
    // async calls be made through a boxed future. Otherwise, the generated
    // state machine would have infinite size!
    fn load_resource_content<'a>(
        &'a self,
        scope: Scope<'a>,
        resource: &'a Url,
        content: bytes::Bytes,
        content_type: flow::ContentType,
    ) -> LocalBoxFuture<'a, ()> {
        async move {
            let scope = scope.push_resource(&resource);

            let content_dom = match self.load_resource_content_dom(scope, &content, content_type) {
                Some(d) => d,
                None => {
                    return; // Cannot process further.
                }
            };

            match content_type {
                flow::ContentType::Catalog => {
                    self.load_catalog(scope, &content_dom).await;
                }
                flow::ContentType::JsonSchema => {
                    self.load_schema_document(scope, &content_dom).await;
                }
                flow::ContentType::Config => {
                    self.fallible(
                        scope,
                        serde_json::from_str::<models::Object>(content_dom.get())
                            .map_err(|e| LoadError::ConfigParseErr(e)),
                    );
                }
                flow::ContentType::DocumentsFixture => {
                    self.fallible(
                        scope,
                        serde_json::from_str::<Vec<models::Object>>(content_dom.get())
                            .map_err(|e| LoadError::DocumentFixturesParseErr(e)),
                    );
                }
                _ => {}
            };

            self.tables.borrow_mut().resources.insert_row(
                resource.clone(),
                content_type,
                content,
                content_dom,
            );
        }
        .boxed_local()
    }

    fn load_resource_content_dom<'s>(
        &'s self,
        scope: Scope<'s>,
        content: &[u8],
        content_type: flow::ContentType,
    ) -> Option<Box<RawValue>> {
        use flow::ContentType as CT;

        // These types are not documents and have a placeholder DOM.
        if matches!(content_type, CT::NpmPackage | CT::TypescriptModule) {
            return Some(RawValue::from_string("null".to_string()).unwrap());
        }

        let mut dom: serde_yaml::Value = self.fallible(scope, serde_yaml::from_slice(&content))?;

        // We support YAML merge keys in catalog documents (only).
        // We don't allow YAML aliases in schema documents as they're redundant
        // with JSON Schema's $ref mechanism.
        if let flow::ContentType::Catalog = content_type {
            dom = self.fallible(scope, yaml_merge_keys::merge_keys_serde(dom))?;
        }

        // Our models embed serde_json::RawValue, which cannot be directly
        // deserialized from serde_yaml::Value. We cannot transmute to serde_json::Value
        // because that could re-order elements along the way (Value is a BTreeMap),
        // which could violate the message authentication code (MAC) of inlined and
        // sops-encrypted documents. So, directly transcode into serialized JSON.
        let mut buf = Vec::<u8>::new();
        let mut serializer = serde_json::Serializer::new(&mut buf);
        serde_transcode::transcode(dom, &mut serializer).expect("must transcode");

        Some(RawValue::from_string(String::from_utf8(buf).unwrap()).unwrap())
    }

    async fn load_schema_document<'s>(
        &'s self,
        scope: Scope<'s>,
        content_dom: &RawValue,
    ) -> Option<()> {
        let dom: serde_json::Value = serde_json::from_str(content_dom.get()).unwrap();

        let doc: CompiledSchema =
            self.fallible(scope, build_schema(scope.resource().clone(), &dom))?;

        let mut index = doc::SchemaIndexBuilder::new();
        self.fallible(scope, index.add(&doc))?;
        let index = index.into_index();

        self.load_schema_node(scope, &index, &doc).await;

        self.tables
            .borrow_mut()
            .schema_docs
            .insert_row(scope.flatten(), dom);

        Some(())
    }

    fn load_schema_node<'s>(
        &'s self,
        scope: Scope<'s>,
        index: &'s doc::SchemaIndex<'s>,
        schema: &'s CompiledSchema,
    ) -> LocalBoxFuture<'s, ()> {
        let mut tasks = Vec::with_capacity(schema.kw.len());

        // Walk keywords, looking for named schemas and references we must resolve.
        for kw in &schema.kw {
            match kw {
                Keyword::Application(app, child) => {
                    // Does |app| map to an external URL that's not contained by this CompiledSchema?
                    let uri = match app {
                        Application::Ref(uri) => {
                            // $ref applications often use #fragment suffixes which indicate
                            // a sub-schema of the base schema document to use.
                            let mut uri = uri.clone();
                            uri.set_fragment(None);

                            if index.fetch(&uri).is_none() {
                                Some(uri)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };

                    tasks.push(async move {
                        // Add Application keywords to the Scope's Location.
                        let location = app.push_keyword(&scope.location);
                        let scope = Scope {
                            location: app.push_keyword_target(&location),
                            ..scope
                        };

                        // Recursive call to walk the schema.
                        let recurse = self.load_schema_node(scope, index, child);

                        if let Some(uri) = uri {
                            // Concurrently fetch |uri| while continuing to walk the schema.
                            let ((), ()) = futures::join!(
                                recurse,
                                self.load_import(scope, &uri, flow::ContentType::JsonSchema)
                            );
                        } else {
                            let () = recurse.await;
                        }
                    });
                }
                _ => (),
            }
        }

        futures::future::join_all(tasks.into_iter())
            .map(|_: Vec<()>| ())
            .boxed_local()
    }

    /// Load a schema reference, which may be an inline schema.
    async fn load_schema_reference<'s>(
        &'s self,
        scope: Scope<'s>,
        schema: models::Schema,
    ) -> Option<Url> {
        // If schema is a relative URL, then import it.
        if let models::Schema::Url(import) = schema {
            let mut import = self.fallible(scope, scope.resource().join(&import))?;

            // Temporarily strip schema fragment to import base document.
            let fragment = import.fragment().map(str::to_string);
            import.set_fragment(None);

            self.load_import(scope, &import, flow::ContentType::JsonSchema)
                .await;

            import.set_fragment(fragment.as_deref());
            Some(import)
        } else {
            // Schema is an object or bool.
            let content = serde_json::to_vec(&schema).unwrap().into();
            let import = self
                .load_synthetic_resource(scope, content, flow::ContentType::JsonSchema)
                .await;
            Some(import)
        }
    }

    /// Load a test documents reference, which may be in an inline form.
    async fn load_test_documents<'s>(
        &'s self,
        scope: Scope<'s>,
        documents: models::TestDocuments,
    ) -> Option<Url> {
        if let models::TestDocuments::Url(import) = documents {
            let import = self.fallible(scope, scope.resource().join(import.as_ref()))?;
            self.load_import(scope, &import, flow::ContentType::DocumentsFixture)
                .await;
            Some(import)
        } else {
            let content = serde_json::to_vec(&documents).unwrap().into();
            let import = self
                .load_synthetic_resource(scope, content, flow::ContentType::DocumentsFixture)
                .await;
            Some(import)
        }
    }

    async fn load_synthetic_resource<'s>(
        &'s self,
        scope: Scope<'s>,
        content: bytes::Bytes,
        content_type: flow::ContentType,
    ) -> Url {
        // Create a synthetic resource URL by extending the parent scope with a `ptr` query parameter,
        // encoding the json pointer path of the schema.
        let mut import = scope.resource().clone();
        import.set_query(Some(&format!("ptr={}", scope.location.url_escaped())));

        self.load_resource_content(scope, &import, content, content_type)
            .await;

        self.tables
            .borrow_mut()
            .imports
            .insert_row(scope.flatten(), scope.resource(), &import);

        import
    }

    // Load an import to another resource, recursively fetching if not yet visited.
    async fn load_import<'s>(
        &'s self,
        scope: Scope<'s>,
        import: &'s Url,
        content_type: flow::ContentType,
    ) {
        // Recursively process the import if it's not already visited.
        if !self
            .tables
            .borrow_mut()
            .fetches
            .iter()
            .any(|f| f.resource == *import)
        {
            self.load_resource(scope, &import, content_type).await;
        }

        self.tables
            .borrow_mut()
            .imports
            .insert_row(scope.flatten(), scope.resource(), import);
    }

    // Load a top-level catalog specification.
    async fn load_catalog<'s>(&'s self, scope: Scope<'s>, content_dom: &RawValue) -> Option<()> {
        let models::Catalog {
            _schema,
            resources,
            import,
            collections,
            materializations,
            captures,
            tests,
            storage_mappings,
        } = self.fallible(scope, serde_json::from_str(content_dom.get()))?;

        // Collect inlined resources. These don't participate in loading until
        // we encounter an import of the resource.
        for (url, resource) in resources {
            if let Some(url) = self.fallible(
                scope.push_prop("resources").push_prop(&url),
                Url::parse(&url),
            ) {
                self.inlined.borrow_mut().insert(url, resource);
            }
        }

        // Collect storage mappings.
        for (prefix, storage) in storage_mappings.into_iter() {
            let scope = scope
                .push_prop("storageMappings")
                .push_prop(prefix.as_str())
                .flatten();
            let models::StorageDef { stores } = storage;

            self.tables
                .borrow_mut()
                .storage_mappings
                .insert_row(scope, prefix, stores)
        }

        // Task which loads all imports.
        let import = import.into_iter().enumerate().map(|(index, import)| {
            async move {
                let scope = scope.push_prop("import");
                let scope = scope.push_item(index);

                // Map from relative to absolute URL.
                if let Some(url) =
                    self.fallible(scope, scope.resource().join(import.relative_url()))
                {
                    self.load_import(scope, &url, proto_content_type(import.content_type()))
                        .await;
                }
            }
        });
        let import = futures::future::join_all(import);

        // Task which loads all collections.
        let collections = collections
            .into_iter()
            .map(|(name, collection)| async move {
                self.load_collection(
                    scope.push_prop("collections").push_prop(name.as_ref()),
                    &name,
                    collection,
                )
                .await;
            });
        let collections = futures::future::join_all(collections);

        // Task which loads all captures.
        let captures = captures.into_iter().map(|(name, capture)| async move {
            self.load_capture(scope.push_prop("captures").push_prop(&name), &name, capture)
                .await;
        });
        let captures = futures::future::join_all(captures);

        // Task which loads all materializations.
        let materializations =
            materializations
                .into_iter()
                .map(|(name, materialization)| async move {
                    self.load_materialization(
                        scope.push_prop("materializations").push_prop(&name),
                        &name,
                        materialization,
                    )
                    .await;
                });
        let materializations = futures::future::join_all(materializations);

        // Task which loads all tests.
        let tests = tests.into_iter().map(|(name, test)| async move {
            self.load_test(scope.push_prop("tests").push_prop(&name), &name, test)
                .await;
        });
        let tests = futures::future::join_all(tests);

        let (_, _, _, _, _): (Vec<()>, Vec<()>, Vec<()>, Vec<()>, Vec<()>) =
            futures::join!(import, collections, captures, materializations, tests);
        Some(())
    }

    async fn load_collection<'s>(
        &'s self,
        scope: Scope<'s>,
        collection_name: &'s models::Collection,
        mut spec: models::CollectionDef,
    ) {
        let derivation = std::mem::take(&mut spec.derivation);
        let projections = std::mem::take(&mut spec.projections);
        let schema = std::mem::replace(&mut spec.schema, models::Schema::Bool(false));

        // Visit all collection projections.
        let mut saw_root = false;
        for (field, spec) in projections {
            if spec.as_parts().0.as_ref() == "" {
                saw_root = true;
            }

            self.tables.borrow_mut().projections.insert_row(
                scope.push_prop("projections").push_prop(&field).flatten(),
                collection_name,
                field,
                spec,
            );
        }

        // If we didn't see an explicit projection of the root document,
        // add a default projection with field "flow_document".
        if !saw_root {
            self.tables.borrow_mut().projections.insert_row(
                scope
                    .push_prop("projections")
                    .push_prop("flow_document")
                    .flatten(),
                collection_name,
                models::Field::new("flow_document"),
                models::Projection::Pointer(models::JsonPointer::new("")),
            );
        }

        // Task which loads & maps collection schema => URL.
        // Recoverable failures project to Ok(None).
        let schema = self.load_schema_reference(scope.push_prop("schema"), schema);

        // If this collection is a derivation, concurrently
        // load the collection's schema and its derivation.
        let schema = match derivation {
            Some(derivation) => {
                let derivation = self.load_derivation(
                    scope.push_prop("derivation"),
                    collection_name,
                    derivation,
                );

                let (schema, ()) = futures::join!(schema, derivation);
                schema
            }
            None => schema.await,
        };

        if let Some(schema) = schema {
            self.tables.borrow_mut().collections.insert_row(
                scope.flatten(),
                collection_name,
                spec,
                schema,
            );
        }
    }

    async fn load_derivation<'s>(
        &'s self,
        scope: Scope<'s>,
        derivation_name: &'s models::Collection,
        mut spec: models::Derivation,
    ) {
        // Destructure |spec|, taking components which are loaded and normalized.
        let transforms = std::mem::take(&mut spec.transform);
        let register_schema =
            std::mem::replace(&mut spec.register.schema, models::Schema::Bool(false));
        let (typescript_module, npm_dependencies) = match &mut spec.typescript {
            Some(models::TypescriptModule {
                module,
                npm_dependencies,
            }) => (
                Some(std::mem::take(module)),
                std::mem::take(npm_dependencies),
            ),
            None => (None, BTreeMap::new()),
        };

        // Task which loads & maps register schema => URL.
        let register_schema = async move {
            self.load_schema_reference(
                scope.push_prop("register").push_prop("schema"),
                register_schema,
            )
            .await
        };

        // Task which loads & maps typescript module => URL.
        let typescript_module = async move {
            let scope = scope.push_prop("typescript");
            let scope = scope.push_prop("module");

            let typescript_module = match typescript_module {
                Some(m) => m,
                None => {
                    return None;
                }
            };

            // If the module contains a newline, it's an inline module.
            // Otherwise it's a relative URL to a TypeScript file.
            if typescript_module.contains("\n") {
                let content = typescript_module.into();
                let import = self
                    .load_synthetic_resource(scope, content, flow::ContentType::TypescriptModule)
                    .await;
                Some(import)
            } else if let Some(import) =
                self.fallible(scope, scope.resource().join(&typescript_module))
            {
                self.load_import(scope, &import, flow::ContentType::TypescriptModule)
                    .await;
                Some(import)
            } else {
                None // Failed to map to URL. We reported an error already.
            }
        };

        // Tasks which load each derivation transform.
        let transforms =
            transforms
                .into_iter()
                .map(|(transform_name, transform_spec)| async move {
                    self.load_transform(
                        scope
                            .push_prop("transform")
                            .push_prop(transform_name.as_ref()),
                        &transform_name,
                        derivation_name,
                        transform_spec,
                    )
                    .await
                });

        // Poll until register schema and all transforms are loaded.
        let (register_schema, typescript_module, _): (_, _, Vec<()>) = futures::join!(
            register_schema,
            typescript_module,
            futures::future::join_all(transforms)
        );

        // Load any NPM package depenencies of the derivation's TypeScript module (if present).
        for (package, version) in npm_dependencies {
            let scope = scope
                .push_prop("typescript")
                .push_prop("npmDependencies")
                .push_prop(&package)
                .flatten();

            self.tables.borrow_mut().npm_dependencies.insert_row(
                scope,
                derivation_name,
                package,
                version,
            );
        }

        if let Some(register_schema) = register_schema {
            self.tables.borrow_mut().derivations.insert_row(
                scope.flatten(),
                derivation_name,
                spec,
                register_schema,
                typescript_module,
            );
        }
    }

    async fn load_transform<'s>(
        &'s self,
        scope: Scope<'s>,
        transform_name: &'s models::Transform,
        derivation: &'s models::Collection,
        mut spec: models::TransformDef,
    ) {
        // Map optional source schema => URL.
        let source_schema = match std::mem::take(&mut spec.source.schema) {
            Some(url) => {
                self.load_schema_reference(scope.push_prop("source").push_prop("schema"), url)
                    .await
            }
            None => None,
        };

        self.tables.borrow_mut().transforms.insert_row(
            scope.flatten(),
            derivation,
            transform_name,
            spec,
            source_schema,
        );
    }

    async fn load_capture<'s>(
        &'s self,
        scope: Scope<'s>,
        capture_name: &'s models::Capture,
        mut spec: models::CaptureDef,
    ) {
        let bindings = std::mem::take(&mut spec.bindings);

        let endpoint_config = match &mut spec.endpoint {
            models::CaptureEndpoint::Connector(models::ConnectorConfig { config, .. }) => {
                let config =
                    std::mem::replace(config, RawValue::from_string("null".to_string()).unwrap());

                self.load_config(
                    scope
                        .push_prop("endpoint")
                        .push_prop("connector")
                        .push_prop("config"),
                    config,
                )
                .await
            }
            models::CaptureEndpoint::Ingest(_) => None,
        };

        for (index, binding_spec) in bindings.into_iter().enumerate() {
            let scope = scope.push_prop("bindings");
            let scope = scope.push_item(index);

            self.tables.borrow_mut().capture_bindings.insert_row(
                scope.flatten(),
                capture_name,
                index as u32,
                binding_spec,
            );
        }

        self.tables.borrow_mut().captures.insert_row(
            scope.flatten(),
            capture_name,
            spec,
            endpoint_config,
        );
    }

    async fn load_materialization<'s>(
        &'s self,
        scope: Scope<'s>,
        materialization_name: &'s models::Materialization,
        mut spec: models::MaterializationDef,
    ) {
        let bindings = std::mem::take(&mut spec.bindings);

        let endpoint_config = match &mut spec.endpoint {
            models::MaterializationEndpoint::Connector(models::ConnectorConfig {
                config, ..
            }) => {
                let config =
                    std::mem::replace(config, RawValue::from_string("null".to_string()).unwrap());

                self.load_config(
                    scope
                        .push_prop("endpoint")
                        .push_prop("connector")
                        .push_prop("config"),
                    config,
                )
                .await
            }
            models::MaterializationEndpoint::Sqlite(sqlite) => {
                if sqlite.path.starts_with(":memory:") {
                    // Already absolute.
                } else if let Some(path) =
                    self.fallible(scope, scope.resource().join(sqlite.path.as_ref()))
                {
                    // Resolve relative database path relative to current scope.
                    sqlite.path = models::RelativeUrl::new(path.to_string());
                } else {
                    // We reported a join() error.
                }
                None
            }
        };

        for (index, binding_spec) in bindings.into_iter().enumerate() {
            let scope = scope.push_prop("bindings");
            let scope = scope.push_item(index);

            self.tables
                .borrow_mut()
                .materialization_bindings
                .insert_row(
                    scope.flatten(),
                    materialization_name,
                    index as u32,
                    binding_spec,
                );
        }

        self.tables.borrow_mut().materializations.insert_row(
            scope.flatten(),
            materialization_name,
            spec,
            endpoint_config,
        );
    }

    async fn load_test<'s>(
        &'s self,
        scope: Scope<'s>,
        test_name: &'s models::Test,
        specs: Vec<models::TestStep>,
    ) {
        // Task which loads all steps of this test.
        let specs = specs
            .into_iter()
            .enumerate()
            .map(|(step_index, mut spec)| async move {
                let scope = scope.push_item(step_index);

                let documents = match &mut spec {
                    models::TestStep::Ingest(models::TestStepIngest { documents, .. }) => {
                        std::mem::replace(documents, models::TestDocuments::Inline(Vec::new()))
                    }
                    models::TestStep::Verify(models::TestStepVerify { documents, .. }) => {
                        std::mem::replace(documents, models::TestDocuments::Inline(Vec::new()))
                    }
                };

                if let Some(documents) = self.load_test_documents(scope, documents).await {
                    self.tables.borrow_mut().test_steps.insert_row(
                        scope.flatten(),
                        test_name.clone(),
                        step_index as u32,
                        spec,
                        documents,
                    );
                }
            });
        futures::future::join_all(specs).await;
    }

    async fn load_config<'s>(&'s self, scope: Scope<'s>, config: Box<RawValue>) -> Option<Url> {
        let config_parsed: models::Config = self.fallible(
            scope,
            serde_json::from_str(config.get()).map_err(|e| LoadError::JsonErr(e)),
        )?;

        match config_parsed {
            // If config is a relative URL, then import it.
            models::Config::Url(import) => {
                let import = self.fallible(scope, scope.resource().join(&import))?;
                self.load_import(scope, &import, flow::ContentType::Config)
                    .await;

                Some(import)
            }
            models::Config::Inline(_) => {
                let content = serde_json::to_vec(&*config).unwrap().into();
                let import = self
                    .load_synthetic_resource(scope, content, flow::ContentType::Config)
                    .await;

                Some(import)
            }
        }
    }

    // Consume a result capable of producing a LoadError.
    // Pass through a Result::Ok<T> as Some<T>.
    // Or, record a Result::Err<T> and return None.
    fn fallible<'s, T, E>(&self, scope: Scope<'s>, r: Result<T, E>) -> Option<T>
    where
        E: Into<LoadError>,
    {
        match r {
            Ok(t) => Some(t),
            Err(err) => {
                self.tables
                    .borrow_mut()
                    .errors
                    .insert_row(scope.flatten(), anyhow::anyhow!(err.into()));
                None
            }
        }
    }
}

fn proto_content_type(t: models::ContentType) -> flow::ContentType {
    match t {
        models::ContentType::Catalog => flow::ContentType::Catalog,
        models::ContentType::JsonSchema => flow::ContentType::JsonSchema,
        models::ContentType::TypescriptModule => flow::ContentType::TypescriptModule,
        models::ContentType::Config => flow::ContentType::Config,
        models::ContentType::DocumentsFixture => flow::ContentType::DocumentsFixture,
    }
}
