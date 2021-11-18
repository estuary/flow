use super::Scope;
use doc::Schema as CompiledSchema;
use futures::future::{FutureExt, LocalBoxFuture};
use json::schema::{build::build_schema, Application, Keyword};
use models::{self, tables};
use protocol::flow::test_spec::step::Type as TestStepType;
use regex::Regex;
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
    #[error("failed to parse YAML (location {:?})", .0.location())]
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

#[derive(Default, Debug)]
pub struct Tables {
    pub capture_bindings: tables::CaptureBindings,
    pub captures: tables::Captures,
    pub collections: tables::Collections,
    pub derivations: tables::Derivations,
    pub errors: tables::Errors,
    pub fetches: tables::Fetches,
    pub imports: tables::Imports,
    pub materialization_bindings: tables::MaterializationBindings,
    pub materializations: tables::Materializations,
    pub named_schemas: tables::NamedSchemas,
    pub npm_dependencies: tables::NPMDependencies,
    pub projections: tables::Projections,
    pub resources: tables::Resources,
    pub schema_docs: tables::SchemaDocs,
    pub storage_mappings: tables::StorageMappings,
    pub test_steps: tables::TestSteps,
    pub transforms: tables::Transforms,
}

// FetchResult is the result type of a fetch operation,
// and returns the resolved content of the resource.
pub type FetchResult = Result<bytes::Bytes, anyhow::Error>;
// FetchFuture is a Future of FetchResult.
pub type FetchFuture<'a> = LocalBoxFuture<'a, FetchResult>;

/// Fetcher resolves a resource URL to its content and, optionally, a re-written
/// URL to use for the resource rather than the |resource| URL.
pub trait Fetcher {
    fn fetch<'a>(
        &'a self,
        // Resource to fetch.
        resource: &'a Url,
        // Expected content type of the resource.
        content_type: models::ContentType,
    ) -> FetchFuture<'a>;
}

/// Loader provides a stack-based driver for traversing catalog source
/// models, with dispatch to a Visitor trait and having fine-grained
/// tracking of location context.
pub struct Loader<F: Fetcher> {
    // Inlined resource definitions which have been observed, but not loaded.
    inlined: RefCell<BTreeMap<Url, models::ResourceDef>>,
    // Tables loaded by the build process.
    tables: RefCell<Tables>,
    // Fetcher for retrieving discovered, unvisited resources.
    fetcher: F,
}

impl<F: Fetcher> Loader<F> {
    /// Build and return a new Loader.
    pub fn new(tables: Tables, fetcher: F) -> Loader<F> {
        Loader {
            inlined: RefCell::new(BTreeMap::new()),
            tables: RefCell::new(tables),
            fetcher,
        }
    }

    pub fn into_tables(self) -> Tables {
        self.tables.into_inner()
    }

    /// Load (or re-load) a resource of the given ContentType.
    pub async fn load_resource<'a>(
        &'a self,
        scope: Scope<'a>,
        resource: &'a Url,
        content_type: models::ContentType,
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

        // If an inline definition of a resource is already available, then use it.
        // Otherwise delegate to the Fetcher.
        // TODO(johnny): Sanity check expected vs actual content-types.
        let inlined = self.inlined.borrow_mut().remove(&resource); // Don't hold guard.
        let content = if let Some(resource) = inlined {
            Ok(resource.content.clone())
        } else {
            self.fetcher.fetch(&resource, content_type.into()).await
        };

        match content {
            Ok(content) => {
                self.load_resource_content(scope, resource, content, content_type)
                    .await
            }
            Err(err) if matches!(content_type, models::ContentType::TypescriptModule) => {
                // Not every catalog spec need have an accompanying TypescriptModule.
                // We optimistically load them, but do not consider it an error if
                // it doesn't exist. We'll do more handling of this condition within
                // Typescript building, including surfacing compiler errors of missing
                // files and potentially stubbing an implementation for the user.
                tracing::debug!(?err, %resource, "did not fetch typescript module");
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
        content_type: models::ContentType,
    ) -> LocalBoxFuture<'a, ()> {
        async move {
            self.tables
                .borrow_mut()
                .resources
                .insert_row(resource.clone(), content_type, &content);
            let scope = scope.push_resource(&resource);

            match content_type {
                models::ContentType::Catalog => self.load_catalog(scope, content.as_ref()).await,
                models::ContentType::JsonSchema => {
                    self.load_schema_document(scope, content.as_ref()).await
                }
                _ => None,
            };
            ()
        }
        .boxed_local()
    }

    async fn load_schema_document<'s>(&'s self, scope: Scope<'s>, content: &[u8]) -> Option<()> {
        let dom: serde_json::Value = self.fallible(scope, serde_yaml::from_slice(&content))?;
        // We don't allow YAML aliases in schema documents as they're redundant
        // with JSON Schema's $ref mechanism.
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
                Keyword::Anchor(anchor_uri) => {
                    // Does this anchor meet our definition of a named schema?
                    if let Some((_, anchor)) = anchor_uri
                        .as_str()
                        .split_once('#')
                        .filter(|(_, s)| NAMED_SCHEMA_RE.is_match(s))
                    {
                        self.tables.borrow_mut().named_schemas.insert_row(
                            scope.flatten(),
                            anchor_uri,
                            anchor.to_string(),
                        );
                    }
                }
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
                                self.load_import(scope, &uri, models::ContentType::JsonSchema)
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
            let mut import = self.fallible(scope, scope.resource().join(import.as_ref()))?;

            // Temporarily strip schema fragment to import base document.
            let fragment = import.fragment().map(str::to_string);
            import.set_fragment(None);

            self.load_import(scope, &import, models::ContentType::JsonSchema)
                .await;

            import.set_fragment(fragment.as_deref());
            Some(import)
        } else {
            // Schema is in-line. Create a synthetic resource URL by extending the parent
            // with a `ptr` query parameter, encoding the json pointer path of the schema.
            let mut import = scope.resource().clone();
            import.set_query(Some(&format!("ptr={}", scope.location.url_escaped())));

            self.load_resource_content(
                scope,
                &import,
                serde_json::to_vec(&schema).unwrap().into(),
                models::ContentType::JsonSchema,
            )
            .await;

            self.tables
                .borrow_mut()
                .imports
                .insert_row(scope.flatten(), scope.resource(), &import);
            Some(import)
        }
    }

    // Load an import to another resource, recursively fetching if not yet visited.
    async fn load_import<'s>(
        &'s self,
        scope: Scope<'s>,
        import: &'s Url,
        content_type: models::ContentType,
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
    async fn load_catalog<'s>(&'s self, scope: Scope<'s>, content: &[u8]) -> Option<()> {
        let dom: serde_yaml::Value = self.fallible(scope, serde_yaml::from_slice(&content))?;
        // We allow and support YAML aliases in catalog documents.
        let dom: serde_yaml::Value =
            self.fallible(scope, yaml_merge_keys::merge_keys_serde(dom))?;

        let models::Catalog {
            _schema,
            resources,
            import,
            npm_dependencies,
            collections,
            materializations,
            captures,
            tests,
            storage_mappings,
        } = self.fallible(scope, serde_yaml::from_value(dom))?;

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

        // Collect NPM dependencies.
        for (package, version) in npm_dependencies {
            let scope = scope
                .push_prop("npmDependencies")
                .push_prop(&package)
                .flatten();

            self.tables
                .borrow_mut()
                .npm_dependencies
                .insert_row(scope, package, version);
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
                    self.load_import(scope, &url, import.content_type()).await;
                }
            }
        });
        let import = futures::future::join_all(import);

        // Start a task which projects this catalog to a sibling TypeScript module,
        // and then optimistically loads this optional resource.
        let typescript_module = async move {
            let mut module = scope.resource().clone();
            let mut path = std::path::PathBuf::from(module.path());
            path.set_extension("ts");

            module.set_path(path.to_str().expect("should still be valid utf8"));
            self.load_import(scope, &module, models::ContentType::TypescriptModule)
                .await;
        };

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

        // Collect captures.
        for (name, capture) in captures {
            let scope = scope.push_prop("captures");
            let scope = scope.push_prop(&name);
            let models::CaptureDef {
                endpoint,
                bindings,
                interval,
                shards,
            } = capture;
            let endpoint_type = endpoint.endpoint_type();

            if let Some(endpoint_spec) = self.load_capture_endpoint(scope, endpoint) {
                self.tables.borrow_mut().captures.insert_row(
                    scope.flatten(),
                    &name,
                    endpoint_type,
                    endpoint_spec,
                    interval.as_secs() as u32,
                    shards,
                );
            }

            for (index, binding) in bindings.into_iter().enumerate() {
                let scope = scope.push_prop("bindings");
                let scope = scope.push_item(index);
                let models::CaptureBinding { resource, target } = binding;

                self.tables.borrow_mut().capture_bindings.insert_row(
                    scope.flatten(),
                    &name,
                    index as u32,
                    serde_json::Value::Object(resource),
                    target,
                );
            }
        }

        // Collect materializations.
        for (name, materialization) in materializations {
            let scope = scope.push_prop("materializations");
            let scope = scope.push_prop(&name);
            let models::MaterializationDef {
                endpoint,
                bindings,
                shards,
            } = materialization;
            let endpoint_type = endpoint.endpoint_type();

            if let Some(endpoint_spec) = self.load_materialization_endpoint(scope, endpoint) {
                self.tables.borrow_mut().materializations.insert_row(
                    scope.flatten(),
                    &name,
                    endpoint_type,
                    endpoint_spec,
                    shards,
                );
            }

            for (index, binding) in bindings.into_iter().enumerate() {
                let scope = scope.push_prop("bindings");
                let scope = scope.push_item(index);

                let models::MaterializationBinding {
                    resource,
                    source,
                    partitions,
                    fields:
                        models::MaterializationFields {
                            include: fields_include,
                            exclude: fields_exclude,
                            recommended: fields_recommended,
                        },
                } = binding;

                self.tables
                    .borrow_mut()
                    .materialization_bindings
                    .insert_row(
                        scope.flatten(),
                        &name,
                        index as u32,
                        serde_json::Value::Object(resource),
                        source,
                        fields_exclude,
                        fields_include,
                        fields_recommended,
                        partitions,
                    );
            }
        }

        // Collect tests.
        for (test, step_specs) in tests {
            for (step_index, spec) in step_specs.into_iter().enumerate() {
                let scope = scope
                    .push_prop("tests")
                    .push_prop(&test)
                    .push_item(step_index)
                    .flatten();
                let test = test.clone();

                let (collection, documents, partitions, description, step_type) = match spec {
                    models::TestStep::Ingest(models::TestStepIngest {
                        collection,
                        documents,
                        description,
                    }) => (
                        collection,
                        documents,
                        None,
                        description,
                        TestStepType::Ingest,
                    ),

                    models::TestStep::Verify(models::TestStepVerify {
                        collection,
                        documents,
                        partitions,
                        description,
                    }) => (
                        collection,
                        documents,
                        partitions,
                        description,
                        TestStepType::Verify,
                    ),
                };

                self.tables.borrow_mut().test_steps.insert_row(
                    scope,
                    test,
                    description,
                    collection,
                    documents,
                    partitions,
                    step_index as u32,
                    step_type,
                );
            }
        }

        let (_, _, _): (Vec<()>, (), Vec<()>) =
            futures::join!(import, typescript_module, collections);
        Some(())
    }

    async fn load_collection<'s>(
        &'s self,
        scope: Scope<'s>,
        collection_name: &'s models::Collection,
        collection: models::CollectionDef,
    ) {
        let models::CollectionDef {
            schema,
            key,
            projections,
            derivation,
            journals,
        } = collection;

        // Visit all collection projections.
        for (field, projection) in projections.iter() {
            let (location, partition) = match projection {
                models::Projection::Pointer(location) => (location, false),
                models::Projection::Extended {
                    location,
                    partition,
                } => (location, *partition),
            };

            self.tables.borrow_mut().projections.insert_row(
                scope.push_prop("projections").push_prop(field).flatten(),
                collection_name,
                field,
                location,
                partition,
                true, // User-provided.
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
                schema,
                key,
                journals,
            );
        }
    }

    async fn load_derivation<'s>(
        &'s self,
        scope: Scope<'s>,
        derivation_name: &'s models::Collection,
        derivation: models::Derivation,
    ) {
        let models::Derivation {
            register:
                models::Register {
                    schema: register_schema,
                    initial: register_initial,
                },
            transform,
            shards,
        } = derivation;

        // Task which loads & maps register schema => URL.
        let register_schema = async move {
            self.load_schema_reference(
                scope.push_prop("register").push_prop("schema"),
                register_schema,
            )
            .await
        };

        // Task which loads each derivation transform.
        let transforms = transform.into_iter().map(|(name, transform)| async move {
            self.load_transform(
                scope.push_prop("transform").push_prop(name.as_ref()),
                &name,
                derivation_name,
                transform,
            )
            .await
        });
        let transforms = futures::future::join_all(transforms);

        let (register_schema, _): (_, Vec<()>) = futures::join!(register_schema, transforms);

        if let Some(register_schema) = register_schema {
            self.tables.borrow_mut().derivations.insert_row(
                scope.flatten(),
                derivation_name,
                register_schema,
                register_initial,
                shards,
            );
        }
    }

    async fn load_transform<'s>(
        &'s self,
        scope: Scope<'s>,
        transform_name: &'s models::Transform,
        derivation: &'s models::Collection,
        transform: models::TransformDef,
    ) {
        let models::TransformDef {
            source:
                models::TransformSource {
                    name: source,
                    schema: source_schema,
                    partitions: source_partitions,
                },
            read_delay,
            priority,
            shuffle,
            update,
            publish,
        } = transform;

        let (shuffle_key, shuffle_lambda) = match shuffle {
            Some(models::Shuffle::Key(key)) => (Some(key), None),
            Some(models::Shuffle::Lambda(lambda)) => (None, Some(lambda)),
            None => (None, None),
        };
        let update_lambda = match update {
            Some(models::Update { lambda }) => Some(lambda),
            None => None,
        };
        let publish_lambda = match publish {
            Some(models::Publish { lambda }) => Some(lambda),
            None => None,
        };

        // Map optional source schema => URL.
        let source_schema = match source_schema {
            Some(url) => {
                self.load_schema_reference(scope.push_prop("source").push_prop("schema"), url)
                    .await
            }
            None => None,
        };

        self.tables.borrow_mut().transforms.insert_row(
            scope.flatten(),
            derivation,
            priority,
            publish_lambda,
            read_delay.map(|d| d.as_secs() as u32),
            shuffle_key,
            shuffle_lambda,
            source,
            source_partitions,
            source_schema,
            transform_name,
            update_lambda,
        );
    }

    fn load_capture_endpoint<'s>(
        &'s self,
        _scope: Scope<'s>,
        endpoint: models::CaptureEndpoint,
    ) -> Option<serde_json::Value> {
        use models::CaptureEndpoint::*;
        match endpoint {
            AirbyteSource(spec) => Some(serde_json::to_value(spec).unwrap()),
            Ingest(spec) => Some(serde_json::to_value(spec).unwrap()),
        }
    }

    fn load_materialization_endpoint<'s>(
        &'s self,
        scope: Scope<'s>,
        endpoint: models::MaterializationEndpoint,
    ) -> Option<serde_json::Value> {
        use models::MaterializationEndpoint::*;
        match endpoint {
            FlowSink(spec) => Some(serde_json::to_value(spec).unwrap()),
            Sqlite(mut spec) => {
                if spec.path.starts_with(":memory:") {
                    Some(serde_json::to_value(spec).unwrap()) // Already absolute.
                } else if let Some(path) =
                    self.fallible(scope, scope.resource().join(spec.path.as_ref()))
                {
                    // Resolve relative database path relative to current scope.
                    spec.path = models::RelativeUrl::new(path.to_string());
                    Some(serde_json::to_value(spec).unwrap())
                } else {
                    None // We reported a join() error.
                }
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

lazy_static::lazy_static! {
    // The set of allowed characters in a schema `$anchor` is quite limited,
    // by Sec 8.2.3.
    //
    // To identify named schemas, we further restrict to anchors which start
    // with a capital letter and include only '_' as punctuation.
    // See: https://json-schema.org/draft/2019-09/json-schema-core.html#anchor
    static ref NAMED_SCHEMA_RE: Regex = Regex::new("^[A-Z][\\w_]+$").unwrap();
}
