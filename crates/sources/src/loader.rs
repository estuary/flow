use super::Scope;
use doc::Schema as CompiledSchema;
use futures::future::{BoxFuture, FutureExt};
use json::schema::{self, build::build_schema};
use models::RawValue;
use proto_flow::flow;
use std::sync::{Mutex, MutexGuard};
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum LoadError {
    #[error("failed to parse URL")]
    URLParse(#[from] url::ParseError),
    #[error("failed to fetch resource {uri}")]
    Fetch {
        uri: Url,
        #[source]
        detail: anyhow::Error,
        content_type: flow::ContentType,
    },
    #[error("failed to parse document fixtures as an array of objects")]
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
    #[error("resource content is not UTF-8")]
    ResourceNotUTF8,
}

/// Fetcher resolves a resource URL to its byte content.
pub trait Fetcher: Send + Sync {
    fn fetch<'a>(
        &'a self,
        // Resource to fetch.
        resource: &'a Url,
        // Expected content type of the resource.
        content_type: flow::ContentType,
    ) -> BoxFuture<'a, anyhow::Result<bytes::Bytes>>;
}

/// Loader provides a stack-based driver for traversing catalog source
/// models, with dispatch to a Visitor trait and having fine-grained
/// tracking of location context.
pub struct Loader<F: Fetcher> {
    // Tables loaded by the build process.
    // `tables` is never held across await points or accessed across threads, and the
    // tables_mut() accessor asserts that no other lock is held and does not block.
    // Wrapping in a Mutex makes it easy to pass around futures holding a Loader.
    tables: Mutex<tables::Sources>,
    // Fetcher for retrieving discovered, unvisited resources.
    fetcher: F,
}

impl<F: Fetcher> Loader<F> {
    /// Build and return a new Loader.
    pub fn new(tables: tables::Sources, fetcher: F) -> Loader<F> {
        Loader {
            tables: Mutex::new(tables),
            fetcher,
        }
    }

    pub fn into_tables(self) -> tables::Sources {
        std::mem::take(&mut *self.tables_mut())
    }

    /// Load (or re-load) a resource of the given ContentType.
    pub async fn load_resource<'a>(
        &'a self,
        scope: Scope<'a>,
        resource: &'a Url,
        content_type: flow::ContentType,
    ) {
        if resource.fragment().is_some() {
            self.tables_mut().errors.insert_row(
                &scope.flatten(),
                anyhow::anyhow!(LoadError::Fetch {
                    uri: resource.clone(),
                    detail: LoadError::ResourceWithFragment.into(),
                    content_type,
                }),
            );
            return;
        }

        // Mark as visited, so that recursively-loaded imports don't re-visit.
        self.tables_mut()
            .fetches
            .insert_row(scope.resource_depth() as u32, resource);

        let content: Result<bytes::Bytes, anyhow::Error> =
            self.fetcher.fetch(&resource, content_type.into()).await;

        match content {
            Ok(content) => {
                self.load_resource_content(scope, resource, content, content_type)
                    .await
            }
            Err(err) => {
                self.tables_mut().errors.insert_row(
                    &scope.flatten(),
                    anyhow::anyhow!(LoadError::Fetch {
                        uri: resource.clone(),
                        detail: err,
                        content_type,
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
    ) -> BoxFuture<'a, ()> {
        async move {
            let scope = scope.push_resource(&resource);

            // Can we expect that `content` is a document-object model (YAML or JSON)?
            use flow::ContentType as CT;
            let is_dom = match content_type {
                CT::Catalog | CT::JsonSchema | CT::DocumentsFixture => true,
                CT::Config => {
                    let path = scope.resource().path().to_lowercase();
                    path.ends_with("yaml") || path.ends_with("yml") || path.ends_with("json")
                }
            };

            // We must map the raw `content` into a document object model.
            // * If we expect this resource is a document, parse it as such.
            // * If it's UTF8, then wrap it in a string.
            // * Otherwise, record a LoadError for non-UTF8 content.
            let content_dom = if is_dom {
                // Parse YAML and JSON.
                let mut content_dom = self.fallible(scope, serde_yaml::from_slice(&content))?;

                // We support YAML merge keys in catalog documents (only).
                // We don't allow YAML aliases in schema documents as they're redundant
                // with JSON Schema's $ref mechanism.
                if let flow::ContentType::Catalog = content_type {
                    content_dom =
                        self.fallible(scope, yaml_merge_keys::merge_keys_serde(content_dom))?;
                }

                // Our models embed serde_json::RawValue, which cannot be directly
                // deserialized from serde_yaml::Value. We cannot transmute to serde_json::Value
                // because that could re-order elements along the way (Value is a BTreeMap),
                // which could violate the message authentication code (MAC) of inlined and
                // sops-encrypted documents. So, directly transcode into serialized JSON.
                let mut buf = Vec::<u8>::new();
                let mut serializer = serde_json::Serializer::new(&mut buf);
                serde_transcode::transcode(content_dom, &mut serializer).expect("must transcode");

                RawValue::from_string(String::from_utf8(buf).unwrap()).unwrap()
            } else if let Ok(content) = std::str::from_utf8(&content) {
                RawValue::from_string(serde_json::to_string(&content).unwrap()).unwrap()
            } else {
                self.tables_mut().errors.insert_row(
                    &scope.flatten(),
                    anyhow::anyhow!(LoadError::ResourceNotUTF8),
                );
                return None;
            };

            match content_type {
                flow::ContentType::Catalog => {
                    self.load_catalog(scope, &content_dom).await;
                }
                flow::ContentType::JsonSchema => {
                    self.load_schema_document(scope, &content_dom).await;
                }
                flow::ContentType::DocumentsFixture => {
                    self.load_documents_fixture(scope, &content_dom);
                }
                _ => {}
            };

            self.tables_mut().resources.insert_row(
                resource.clone(),
                content_type,
                content,
                content_dom,
            );
            None
        }
        .map(|_: Option<()>| ())
        .boxed()
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
        Some(())
    }

    fn load_schema_node<'s>(
        &'s self,
        scope: Scope<'s>,
        index: &'s doc::SchemaIndex<'s>,
        schema: &'s CompiledSchema,
    ) -> BoxFuture<'s, ()> {
        let mut tasks = Vec::with_capacity(schema.kw.len());

        // Walk keywords, looking for named schemas and references we must resolve.
        for kw in &schema.kw {
            match kw {
                schema::Keyword::Application(app, child) => {
                    // Does |app| map to an external URL that's not contained by this CompiledSchema?
                    let uri = match app {
                        schema::Application::Ref(uri) => {
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

                        // Recurse to walk the schema, while fetching `uri` in parallel.
                        let ((), ()) = futures::join!(
                            self.load_schema_node(scope, index, child),
                            self.load_import(scope, uri, flow::ContentType::JsonSchema)
                        );
                    });
                }
                _ => (),
            }
        }

        futures::future::join_all(tasks.into_iter())
            .map(|_: Vec<()>| ())
            .boxed()
    }

    /// Load a schema reference, which may be an inline schema.
    async fn load_schema_reference<'s>(&'s self, scope: Scope<'s>, schema: &models::Schema) {
        // If schema is a relative URL, then import it.
        if let Ok(import) = serde_json::from_str::<String>(schema.get()) {
            let import = self
                .fallible(scope, scope.resource().join(&import))
                .map(|mut import| {
                    // Strip schema fragment to import the base document.
                    import.set_fragment(None);
                    import
                });

            self.load_import(scope, import, flow::ContentType::JsonSchema)
                .await;
        } else {
            self.load_schema_document(scope, &schema).await;
        }
    }

    /// Load a test documents reference, which may be in an inline form.
    async fn load_test_documents<'s>(
        &'s self,
        scope: Scope<'s>,
        documents: &models::TestDocuments,
    ) {
        if let Ok(import) = serde_json::from_str::<String>(documents.get()) {
            let import = self.fallible(scope, scope.resource().join(import.as_ref()));
            self.load_import(scope, import, flow::ContentType::DocumentsFixture)
                .await;
        } else {
            self.load_documents_fixture(scope, &documents);
        }
    }

    fn load_documents_fixture<'s>(&'s self, scope: Scope<'s>, content_dom: &RawValue) {
        // Require that the fixture is an array of objects.
        let _: Option<Vec<serde_json::Map<String, serde_json::Value>>> = self.fallible(
            scope,
            serde_json::from_str(content_dom.get())
                .map_err(|e| LoadError::DocumentFixturesParseErr(e)),
        );
    }

    // Load an import to another resource, recursively fetching if not yet visited.
    async fn load_import<'s>(
        &'s self,
        scope: Scope<'s>,
        import: Option<Url>,
        content_type: flow::ContentType,
    ) {
        let Some(import) = import else { return };

        // Recursively process the import if it's not already visited.
        if !self
            .tables_mut()
            .fetches
            .iter()
            .any(|f| f.resource == import)
        {
            self.load_resource(scope, &import, content_type).await;
        }

        self.tables_mut()
            .imports
            .insert_row(scope.flatten(), import);
    }

    // Load a top-level catalog specification.
    async fn load_catalog<'s>(&'s self, scope: Scope<'s>, content_dom: &RawValue) {
        let mut tasks = Vec::new();

        let Some(models::Catalog {
            _schema,
            import,
            captures,
            collections,
            materializations,
            tests,
            storage_mappings,
        }) = self.fallible(scope, serde_json::from_str(content_dom.get())) else {
            return
        };

        // Load all imports.
        for (index, import) in import.into_iter().enumerate() {
            tasks.push(
                async move {
                    // Map from relative to absolute URL.
                    self.load_import(
                        scope.push_prop("import").push_item(index),
                        self.fallible(scope, scope.resource().join(&import)),
                        flow::ContentType::Catalog,
                    )
                    .await;
                }
                .boxed(),
            );
        }

        // Load all captures.
        for (name, capture) in captures {
            tasks.push(
                async move {
                    self.load_capture(scope.push_prop("captures").push_prop(&name), &name, capture)
                        .await;
                }
                .boxed(),
            );
        }

        // Loads all collections.
        for (name, collection) in collections {
            tasks.push(
                async move {
                    self.load_collection(
                        scope.push_prop("collections").push_prop(name.as_ref()),
                        &name,
                        collection,
                    )
                    .await;
                }
                .boxed(),
            );
        }

        // Load all materializations.
        for (name, materialization) in materializations {
            tasks.push(
                async move {
                    self.load_materialization(
                        scope.push_prop("materializations").push_prop(&name),
                        &name,
                        materialization,
                    )
                    .await;
                }
                .boxed(),
            );
        }

        // Load all tests.
        for (name, test) in tests {
            tasks.push(
                async move {
                    self.load_test(scope.push_prop("tests").push_prop(&name), &name, test)
                        .await;
                }
                .boxed(),
            );
        }

        // Gather storage mappings.
        for (prefix, storage) in storage_mappings.into_iter() {
            let models::StorageDef { stores } = storage;

            self.tables_mut().storage_mappings.insert_row(
                scope
                    .push_prop("storageMappings")
                    .push_prop(prefix.as_str())
                    .flatten(),
                prefix,
                stores,
            )
        }

        let _: Vec<()> = futures::future::join_all(tasks.into_iter()).await;
    }

    async fn load_collection<'s>(
        &'s self,
        scope: Scope<'s>,
        collection_name: &'s models::Collection,
        spec: models::CollectionDef,
    ) {
        let mut tasks = Vec::new();

        if let Some(schema) = &spec.schema {
            tasks.push(
                self.load_schema_reference(scope.push_prop("schema"), schema)
                    .boxed(),
            );
        }
        if let Some(schema) = &spec.write_schema {
            tasks.push(
                self.load_schema_reference(scope.push_prop("writeSchema"), schema)
                    .boxed(),
            );
        }
        if let Some(schema) = &spec.read_schema {
            tasks.push(
                self.load_schema_reference(scope.push_prop("readSchema"), schema)
                    .boxed(),
            );
        }
        if let Some(derive) = &spec.derive {
            tasks.push(
                self.load_derivation(scope.push_prop("derive"), derive)
                    .boxed(),
            );
        }

        let _: Vec<()> = futures::future::join_all(tasks.into_iter()).await;

        self.tables_mut()
            .collections
            .insert_row(scope.flatten(), collection_name, spec)
    }

    async fn load_derivation<'s>(&'s self, scope: Scope<'s>, spec: &models::Derivation) {
        let mut tasks = Vec::new();

        match &spec.using {
            models::DeriveUsing::Connector(models::ConnectorConfig { config, .. }) => {
                tasks.push(
                    async move {
                        self.load_config(
                            scope
                                .push_prop("using")
                                .push_prop("connector")
                                .push_prop("config"),
                            config,
                        )
                        .await
                    }
                    .boxed(),
                );
            }
            models::DeriveUsing::Sqlite(models::DeriveUsingSqlite { migrations }) => {
                for (index, migration) in migrations.iter().enumerate() {
                    tasks.push(
                        async move {
                            self.load_config(
                                scope
                                    .push_prop("using")
                                    .push_prop("sqlite")
                                    .push_prop("migrations")
                                    .push_item(index),
                                migration,
                            )
                            .await
                        }
                        .boxed(),
                    );
                }
            }
            models::DeriveUsing::Typescript(models::DeriveUsingTypescript { module, .. }) => {
                tasks.push(
                    async move {
                        self.load_config(
                            scope
                                .push_prop("using")
                                .push_prop("typescript")
                                .push_prop("module"),
                            module,
                        )
                        .await
                    }
                    .boxed(),
                );
            }
        };

        for (index, transform) in spec.transforms.iter().enumerate() {
            tasks.push(
                async move {
                    self.load_config(
                        scope
                            .push_prop("transforms")
                            .push_item(index)
                            .push_prop("lambda"),
                        &transform.lambda,
                    )
                    .await
                }
                .boxed(),
            );

            if let models::Shuffle::Lambda(lambda) = &transform.shuffle {
                tasks.push(
                    async move {
                        self.load_config(
                            scope
                                .push_prop("transforms")
                                .push_item(index)
                                .push_prop("shuffle")
                                .push_prop("lambda"),
                            lambda,
                        )
                        .await
                    }
                    .boxed(),
                );
            }
        }

        let _: Vec<()> = futures::future::join_all(tasks.into_iter()).await;
    }

    async fn load_capture<'s>(
        &'s self,
        scope: Scope<'s>,
        capture_name: &'s models::Capture,
        spec: models::CaptureDef,
    ) {
        let mut tasks = Vec::new();

        match &spec.endpoint {
            models::CaptureEndpoint::Connector(models::ConnectorConfig { config, .. }) => {
                tasks.push(
                    async move {
                        self.load_config(
                            scope
                                .push_prop("endpoint")
                                .push_prop("connector")
                                .push_prop("config"),
                            config,
                        )
                        .await
                    }
                    .boxed(),
                );
            }
        };

        for (index, binding) in spec.bindings.iter().enumerate() {
            tasks.push(
                async move {
                    self.load_config(
                        scope
                            .push_prop("bindings")
                            .push_item(index)
                            .push_prop("resource"),
                        &binding.resource,
                    )
                    .await
                }
                .boxed(),
            );
        }

        let _: Vec<()> = futures::future::join_all(tasks.into_iter()).await;

        self.tables_mut()
            .captures
            .insert_row(scope.flatten(), capture_name, spec);
    }

    async fn load_materialization<'s>(
        &'s self,
        scope: Scope<'s>,
        materialization_name: &'s models::Materialization,
        spec: models::MaterializationDef,
    ) {
        let mut tasks = Vec::new();

        match &spec.endpoint {
            models::MaterializationEndpoint::Connector(models::ConnectorConfig {
                config, ..
            }) => {
                tasks.push(
                    async move {
                        self.load_config(
                            scope
                                .push_prop("endpoint")
                                .push_prop("connector")
                                .push_prop("config"),
                            config,
                        )
                        .await
                    }
                    .boxed(),
                );
            }
            models::MaterializationEndpoint::Sqlite(_sqlite) => {}
        };

        for (index, binding) in spec.bindings.iter().enumerate() {
            if !binding.disable {
                tasks.push(
                    async move {
                        self.load_config(
                            scope
                                .push_prop("bindings")
                                .push_item(index)
                                .push_prop("resource"),
                            &binding.resource,
                        )
                        .await
                    }
                    .boxed(),
                );
            }
        }

        let _: Vec<()> = futures::future::join_all(tasks.into_iter()).await;

        self.tables_mut()
            .materializations
            .insert_row(scope.flatten(), materialization_name, spec);
    }

    async fn load_test<'s>(
        &'s self,
        scope: Scope<'s>,
        test_name: &'s models::Test,
        spec: Vec<models::TestStep>,
    ) {
        let mut tasks = Vec::new();

        for (index, test_step) in spec.iter().enumerate() {
            let documents = match test_step {
                models::TestStep::Ingest(models::TestStepIngest { documents, .. })
                | models::TestStep::Verify(models::TestStepVerify { documents, .. }) => documents,
            };

            tasks.push(async move {
                self.load_test_documents(scope.push_item(index).push_prop("documents"), documents)
                    .await
            });
        }

        let _: Vec<()> = futures::future::join_all(tasks.into_iter()).await;

        self.tables_mut()
            .tests
            .insert_row(scope.flatten(), test_name.clone(), spec);
    }

    async fn load_config<'s>(&'s self, scope: Scope<'s>, config: &RawValue) {
        // If `config` is a JSON string that has no whitespace then presume and
        // require that it's a relative or absolute URL to an imported file.
        match serde_json::from_str::<&str>(config.get()) {
            Ok(import) if !import.chars().any(char::is_whitespace) => {
                self.load_import(
                    scope,
                    self.fallible(scope, scope.resource().join(&import)),
                    flow::ContentType::Config,
                )
                .await;
            }
            _ => {}
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
                self.tables_mut()
                    .errors
                    .insert_row(scope.flatten(), anyhow::anyhow!(err.into()));
                None
            }
        }
    }

    fn tables_mut<'a>(&'a self) -> MutexGuard<'a, tables::Sources> {
        self.tables
            .try_lock()
            .expect("tables should never be accessed concurrently or locked across await points")
    }
}
