use futures::future::{BoxFuture, FutureExt};
use json::Scope;
use models::RawValue;
use proto_flow::flow;
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
    #[error(transparent)]
    SchemaBuild(#[from] json::schema::build::Error<doc::Annotation>),
    #[error(transparent)]
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
    tables: std::sync::Mutex<tables::DraftCatalog>,
    // Fetcher for retrieving discovered, unvisited resources.
    fetcher: F,
}

impl<F: Fetcher> Loader<F> {
    /// Build and return a new Loader.
    pub fn new(tables: tables::DraftCatalog, fetcher: F) -> Loader<F> {
        Loader {
            tables: std::sync::Mutex::new(tables),
            fetcher,
        }
    }

    pub fn into_tables(self) -> tables::DraftCatalog {
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

    fn load_resource_content<'a>(
        &'a self,
        scope: Scope<'a>,
        resource: &'a Url,
        content: bytes::Bytes,
        content_type: flow::ContentType,
    ) -> BoxFuture<'a, ()> {
        // Resources are loaded recursively, and Rust requires that recursive
        // async calls be made through a boxed future. Otherwise, the generated
        // state machine would have infinite size!
        async move {
            self.load_resource_content_inner(scope, resource, content, content_type)
                .await
        }
        .boxed()
    }

    async fn load_resource_content_inner<'a>(
        &'a self,
        scope: Scope<'a>,
        resource: &'a Url,
        content: bytes::Bytes,
        content_type: flow::ContentType,
    ) {
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
            let Some(mut content_dom) = self.fallible(scope, serde_yaml::from_slice(&content))
            else {
                return;
            };

            // We support YAML merge keys in catalog documents (only).
            // We don't allow YAML aliases in schema documents as they're redundant
            // with JSON Schema's $ref mechanism.
            if let flow::ContentType::Catalog = content_type {
                let Some(merged_dom) =
                    self.fallible(scope, yaml_merge_keys::merge_keys_serde(content_dom))
                else {
                    return;
                };
                content_dom = merged_dom;
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
            return;
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
    }

    async fn load_schema_document<'s>(&'s self, scope: Scope<'s>, content_dom: &RawValue) {
        let dom: serde_json::Value = serde_json::from_str(content_dom.get()).unwrap();

        let schema = match json::schema::build::<doc::Annotation>(&scope.flatten(), &dom) {
            Ok(schema) => schema,
            Err(schema_errors) => {
                let errors = &mut self.tables_mut().errors;
                for json::schema::build::ScopedError { scope, inner: err } in schema_errors.0 {
                    errors.insert_row(scope, anyhow::anyhow!(LoadError::SchemaBuild(err)));
                }
                return;
            }
        };

        let mut builder = json::schema::index::Builder::new();
        let Some(()) = self.fallible(scope, builder.add(&schema)) else {
            return;
        };

        self.load_schema_node(scope, &schema, &builder.into_index())
            .await;
    }

    fn load_schema_node<'s>(
        &'s self,
        scope: Scope<'s>,
        schema: &'s doc::Schema,
        schema_index: &'s doc::SchemaIndex<'s>,
    ) -> impl futures::Future<Output = ()> + Send + 's {
        let mut tasks = Vec::with_capacity(schema.keywords.len());

        // Walk keywords, looking for named schemas and references we must resolve.
        for kw in schema.keywords.iter() {
            match kw {
                json::schema::Keyword::Ref { r#ref } => {
                    // Is the $ref inline in the current schema and already indexed?
                    // Also, the "flow://" scheme is used to inject contextual schemas
                    // and is not attempted to be fetched.
                    if schema_index.fetch(r#ref).is_some() || r#ref.starts_with("flow://") {
                        continue;
                    }

                    // $ref applications often use #fragment suffixes which indicate
                    // a sub-schema of the base schema document to use.
                    let mut uri = url::Url::parse(r#ref.as_ref()).unwrap();
                    uri.set_fragment(None);

                    tasks.push(
                        async move {
                            let scope = scope.push_prop(kw.keyword());
                            self.load_import(scope, Some(uri), flow::ContentType::JsonSchema)
                                .await
                        }
                        .boxed(),
                    );
                }
                json::schema::Keyword::AdditionalProperties {
                    additional_properties,
                } => {
                    tasks.push(
                        async move {
                            let scope = scope.push_prop(kw.keyword());
                            self.load_schema_node(scope, &**additional_properties, schema_index)
                                .await;
                        }
                        .boxed(),
                    );
                }
                json::schema::Keyword::AllOf { all_of } => {
                    for (index, schema) in all_of.iter().enumerate() {
                        tasks.push(
                            async move {
                                let scope = scope.push_prop(kw.keyword());
                                let scope = scope.push_item(index);
                                self.load_schema_node(scope, schema, schema_index).await;
                            }
                            .boxed(),
                        );
                    }
                }
                json::schema::Keyword::AnyOf { any_of } => {
                    for (index, schema) in any_of.iter().enumerate() {
                        tasks.push(
                            async move {
                                let scope = scope.push_prop(kw.keyword());
                                let scope = scope.push_item(index);
                                self.load_schema_node(scope, schema, schema_index).await;
                            }
                            .boxed(),
                        );
                    }
                }
                json::schema::Keyword::Definitions { definitions } => {
                    for (name, schema) in definitions.iter() {
                        tasks.push(
                            async move {
                                let scope = scope.push_prop(kw.keyword());
                                let scope = scope.push_prop(name);
                                self.load_schema_node(scope, schema, schema_index).await;
                            }
                            .boxed(),
                        );
                    }
                }
                json::schema::Keyword::Defs { defs } => {
                    for (name, schema) in defs.iter() {
                        tasks.push(
                            async move {
                                let scope = scope.push_prop(kw.keyword());
                                let scope = scope.push_prop(name);
                                self.load_schema_node(scope, schema, schema_index).await;
                            }
                            .boxed(),
                        );
                    }
                }
                json::schema::Keyword::DependentSchemas { dependent_schemas } => {
                    for (name, schema) in dependent_schemas.iter() {
                        tasks.push(
                            async move {
                                let scope = scope.push_prop(kw.keyword());
                                let scope = scope.push_prop(name);
                                self.load_schema_node(scope, schema, schema_index).await;
                            }
                            .boxed(),
                        );
                    }
                }
                json::schema::Keyword::Else { r#else } => {
                    tasks.push(
                        async move {
                            let scope = scope.push_prop(kw.keyword());
                            self.load_schema_node(scope, &r#else, schema_index).await;
                        }
                        .boxed(),
                    );
                }
                json::schema::Keyword::If { r#if } => {
                    tasks.push(
                        async move {
                            let scope = scope.push_prop(kw.keyword());
                            self.load_schema_node(scope, &r#if, schema_index).await;
                        }
                        .boxed(),
                    );
                }
                json::schema::Keyword::Items { items } => {
                    tasks.push(
                        async move {
                            let scope = scope.push_prop(kw.keyword());
                            self.load_schema_node(scope, &items, schema_index).await;
                        }
                        .boxed(),
                    );
                }
                json::schema::Keyword::Not { not } => {
                    tasks.push(
                        async move {
                            let scope = scope.push_prop(kw.keyword());
                            self.load_schema_node(scope, &not, schema_index).await;
                        }
                        .boxed(),
                    );
                }
                json::schema::Keyword::OneOf { one_of } => {
                    for (index, schema) in one_of.iter().enumerate() {
                        tasks.push(
                            async move {
                                let scope = scope.push_prop(kw.keyword());
                                let scope = scope.push_item(index);
                                self.load_schema_node(scope, schema, schema_index).await;
                            }
                            .boxed(),
                        );
                    }
                }
                json::schema::Keyword::PatternProperties { pattern_properties } => {
                    for (name, schema) in pattern_properties.iter() {
                        tasks.push(
                            async move {
                                let scope = scope.push_prop(kw.keyword());
                                let scope = scope.push_prop(name.as_str());
                                self.load_schema_node(scope, schema, schema_index).await;
                            }
                            .boxed(),
                        );
                    }
                }
                json::schema::Keyword::PrefixItems { prefix_items } => {
                    for (index, schema) in prefix_items.iter().enumerate() {
                        tasks.push(
                            async move {
                                let scope = scope.push_prop(kw.keyword());
                                let scope = scope.push_item(index);
                                self.load_schema_node(scope, schema, schema_index).await;
                            }
                            .boxed(),
                        );
                    }
                }
                json::schema::Keyword::Properties { properties } => {
                    for (name, schema) in properties.iter() {
                        tasks.push(
                            async move {
                                let scope = scope.push_prop(kw.keyword());
                                let scope = scope.push_prop(&name[1..]); // Skip leading status byte.
                                self.load_schema_node(scope, schema, schema_index).await;
                            }
                            .boxed(),
                        );
                    }
                }
                json::schema::Keyword::PropertyNames { property_names } => {
                    tasks.push(
                        async move {
                            let scope = scope.push_prop(kw.keyword());
                            self.load_schema_node(scope, &property_names, schema_index)
                                .await;
                        }
                        .boxed(),
                    );
                }
                json::schema::Keyword::Then { then } => {
                    tasks.push(
                        async move {
                            let scope = scope.push_prop(kw.keyword());
                            self.load_schema_node(scope, &then, schema_index).await;
                        }
                        .boxed(),
                    );
                }
                json::schema::Keyword::UnevaluatedItems { unevaluated_items } => {
                    tasks.push(
                        async move {
                            let scope = scope.push_prop(kw.keyword());
                            self.load_schema_node(scope, &unevaluated_items, schema_index)
                                .await;
                        }
                        .boxed(),
                    );
                }
                json::schema::Keyword::UnevaluatedProperties {
                    unevaluated_properties,
                } => {
                    tasks.push(
                        async move {
                            let scope = scope.push_prop(kw.keyword());
                            self.load_schema_node(scope, &unevaluated_properties, schema_index)
                                .await;
                        }
                        .boxed(),
                    );
                }

                // Validation or annotation keywords that don't have sub-schemas.
                json::schema::Keyword::Annotation { .. } => (),
                json::schema::Keyword::False => (),
                json::schema::Keyword::Anchor { .. } => (),
                json::schema::Keyword::Const { .. } => (),
                json::schema::Keyword::Contains { .. } => (),
                json::schema::Keyword::DynamicAnchor { .. } => (),
                json::schema::Keyword::DynamicRef { .. } => (),
                json::schema::Keyword::Enum { .. } => (),
                json::schema::Keyword::ExclusiveMaximumPosInt { .. } => (),
                json::schema::Keyword::ExclusiveMaximumNegInt { .. } => (),
                json::schema::Keyword::ExclusiveMaximumFloat { .. } => (),
                json::schema::Keyword::ExclusiveMinimumPosInt { .. } => (),
                json::schema::Keyword::ExclusiveMinimumNegInt { .. } => (),
                json::schema::Keyword::ExclusiveMinimumFloat { .. } => (),
                json::schema::Keyword::Format { .. } => (),
                json::schema::Keyword::Id { .. } => (),
                json::schema::Keyword::MaximumPosInt { .. } => (),
                json::schema::Keyword::MaximumNegInt { .. } => (),
                json::schema::Keyword::MaximumFloat { .. } => (),
                json::schema::Keyword::MaxContains { .. } => (),
                json::schema::Keyword::MaxItems { .. } => (),
                json::schema::Keyword::MaxLength { .. } => (),
                json::schema::Keyword::MaxProperties { .. } => (),
                json::schema::Keyword::MinimumPosInt { .. } => (),
                json::schema::Keyword::MinimumNegInt { .. } => (),
                json::schema::Keyword::MinimumFloat { .. } => (),
                json::schema::Keyword::MinContains { .. } => (),
                json::schema::Keyword::MinItems { .. } => (),
                json::schema::Keyword::MinLength { .. } => (),
                json::schema::Keyword::MinProperties { .. } => (),
                json::schema::Keyword::MultipleOfPosInt { .. } => (),
                json::schema::Keyword::MultipleOfNegInt { .. } => (),
                json::schema::Keyword::MultipleOfFloat { .. } => (),
                json::schema::Keyword::Pattern { .. } => (),
                json::schema::Keyword::Type { .. } => (),
                json::schema::Keyword::UniqueItems { .. } => (),
            };
        }

        futures::future::join_all(tasks.into_iter()).map(|_: Vec<()>| ())
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
        }) = self.fallible(scope, serde_json::from_str(content_dom.get()))
        else {
            return;
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

        let _: Vec<()> = futures::future::join_all(tasks.into_iter()).await;
    }

    async fn load_collection<'s>(
        &'s self,
        scope: Scope<'s>,
        catalog_name: &'s models::Collection,
        mut spec: models::CollectionDef,
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

        if let Some(models::Derivation {
            using: models::DeriveUsing::Local(local),
            ..
        }) = &mut spec.derive
        {
            self.local_command_to_absolute(
                scope
                    .push_prop("derive")
                    .push_prop("using")
                    .push_prop("local")
                    .push_prop("command"),
                &mut local.command,
            );
        }

        self.tables_mut().collections.insert_row(
            catalog_name,
            scope.flatten(),
            spec.expect_pub_id.take(),
            (!spec.delete).then_some(spec),
            false, // !is_touch
        )
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
            models::DeriveUsing::Python(models::DeriveUsingPython { module, .. }) => {
                tasks.push(
                    async move {
                        self.load_config(
                            scope
                                .push_prop("using")
                                .push_prop("python")
                                .push_prop("module"),
                            module,
                        )
                        .await
                    }
                    .boxed(),
                );
            }
            models::DeriveUsing::Local(models::LocalConfig { config, .. }) => {
                tasks.push(
                    async move {
                        self.load_config(
                            scope
                                .push_prop("using")
                                .push_prop("local")
                                .push_prop("config"),
                            config,
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
        catalog_name: &'s models::Capture,
        mut spec: models::CaptureDef,
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
            models::CaptureEndpoint::Local(models::LocalConfig { config, .. }) => {
                tasks.push(
                    async move {
                        self.load_config(
                            scope
                                .push_prop("endpoint")
                                .push_prop("local")
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

        if let models::CaptureEndpoint::Local(local) = &mut spec.endpoint {
            self.local_command_to_absolute(
                scope
                    .push_prop("endpoint")
                    .push_prop("local")
                    .push_prop("command"),
                &mut local.command,
            );
        }

        self.tables_mut().captures.insert_row(
            catalog_name,
            scope.flatten(),
            spec.expect_pub_id.take(),
            (!spec.delete).then_some(spec),
            false, // !is_touch
        );
    }

    async fn load_materialization<'s>(
        &'s self,
        scope: Scope<'s>,
        catalog_name: &'s models::Materialization,
        mut spec: models::MaterializationDef,
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
            models::MaterializationEndpoint::Local(models::LocalConfig { config, .. }) => {
                tasks.push(
                    async move {
                        self.load_config(
                            scope
                                .push_prop("endpoint")
                                .push_prop("local")
                                .push_prop("config"),
                            config,
                        )
                        .await
                    }
                    .boxed(),
                );
            }
            models::MaterializationEndpoint::Dekaf(models::DekafConfig { config, .. }) => {
                tasks.push(
                    async move {
                        self.load_config(
                            scope
                                .push_prop("endpoint")
                                .push_prop("dekaf")
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

        if let models::MaterializationEndpoint::Local(local) = &mut spec.endpoint {
            self.local_command_to_absolute(
                scope
                    .push_prop("endpoint")
                    .push_prop("local")
                    .push_prop("command"),
                &mut local.command,
            );
        }

        self.tables_mut().materializations.insert_row(
            catalog_name,
            scope.flatten(),
            spec.expect_pub_id.take(),
            (!spec.delete).then_some(spec),
            false, // !is_touch
        );
    }

    async fn load_test<'s>(
        &'s self,
        scope: Scope<'s>,
        catalog_name: &'s models::Test,
        mut spec: models::TestDef,
    ) {
        let mut tasks = Vec::new();

        for (index, test_step) in spec.steps.iter().enumerate() {
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

        self.tables_mut().tests.insert_row(
            catalog_name,
            scope.flatten(),
            spec.expect_pub_id.take(),
            (!spec.delete).then_some(spec),
            false, // !is_touch
        );
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

    // Rewrite the `command` of a `local:` connector endpoint so that its
    // program (`command[0]`) is an absolute path.
    fn local_command_to_absolute(&self, scope: Scope, command: &mut [String]) {
        match resolve_local_command(scope.resource(), command) {
            Ok(Some(program)) => command[0] = program,
            Ok(None) => {}
            Err(error) => self.tables_mut().errors.insert_row(scope.flatten(), error),
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

    fn tables_mut<'a>(&'a self) -> std::sync::MutexGuard<'a, tables::DraftCatalog> {
        self.tables
            .try_lock()
            .expect("tables should never be accessed concurrently or locked across await points")
    }
}

/// Resolve `command[0]` of a `local:` connector endpoint to an absolute path,
/// returning the resolved program when it was rewritten or `None` when it was
/// left unchanged.
///
/// Resolution mirrors how a shell interprets a command, and how Flow resolves
/// every other relative reference (imports, schema `$ref`s, `config` files)
/// against the file that declares it:
///   * A bare name like `python` is located on the `$PATH`.
///   * A relative path like `./connector` is resolved against the directory of
///     the source file that declares it.
///   * An absolute path is left unchanged.
///
/// This applies only while loading local `file://` sources.
fn resolve_local_command(resource: &Url, command: &[String]) -> anyhow::Result<Option<String>> {
    use anyhow::Context;

    if resource.scheme() != "file" {
        return Ok(None);
    }
    let Some(program) = command.first() else {
        return Ok(None); // Empty command; reported by connector validation.
    };
    if std::path::Path::new(program).is_absolute() {
        return Ok(None);
    }

    let resolved = if program.contains('/') {
        // A relative path: resolve against the source file's directory.
        resource
            .join(program)
            .with_context(|| format!("resolving local command {program:?}"))?
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("local command {program:?} is not a local file path"))?
    } else {
        // A bare program name: locate it on the `$PATH`, as a shell would.
        locate_bin::locate(program)
            .with_context(|| format!("locating local command {program:?}"))?
    };

    Ok(Some(resolved.to_string_lossy().into_owned()))
}

#[cfg(test)]
mod local_command_test {
    use super::resolve_local_command;
    use url::Url;

    fn cmd(parts: &[&str]) -> Vec<String> {
        parts.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn test_resolve_local_command() {
        let src = Url::parse("file:///repo/tests/soak/soak.flow.yaml").unwrap();

        // A relative path resolves against the source file's directory.
        assert_eq!(
            resolve_local_command(&src, &cmd(&["./source-soak", "--flag"])).unwrap(),
            Some("/repo/tests/soak/source-soak".to_string()),
        );
        assert_eq!(
            resolve_local_command(&src, &cmd(&["../sibling/run"])).unwrap(),
            Some("/repo/tests/sibling/run".to_string()),
        );

        // Absolute path, empty command, and non-`file://` scope are left as-is.
        assert_eq!(
            resolve_local_command(&src, &cmd(&["/usr/bin/python", "-m", "x"])).unwrap(),
            None,
        );
        assert_eq!(resolve_local_command(&src, &[]).unwrap(), None);
        let from_db = Url::parse("flow://capture/acmeCo/source").unwrap();
        assert_eq!(
            resolve_local_command(&from_db, &cmd(&["./source-soak"])).unwrap(),
            None,
        );

        // A bare program name is located on the `$PATH`.
        let resolved = resolve_local_command(&src, &cmd(&["sh"]))
            .unwrap()
            .expect("`sh` should resolve to an absolute path on the $PATH");
        assert!(std::path::Path::new(&resolved).is_absolute());
        assert!(
            resolved.ends_with("/sh"),
            "unexpected resolution: {resolved}"
        );

        // A bare program name that isn't on the `$PATH` is an error.
        resolve_local_command(&src, &cmd(&["definitely-not-a-real-binary-xyz9000"])).unwrap_err();
    }
}
