use super::{specs, Scope};
use doc::Schema as CompiledSchema;
use futures::future::{FutureExt, LocalBoxFuture};
use json::schema::{build::build_schema, Application, Keyword};
use models::{names, tables};
use protocol::flow::{
    shuffle::Hash as ShuffleHash, test_spec::step::Type as TestStepType, ContentType,
};
use regex::Regex;
use std::cell::RefCell;
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
}

#[derive(Default, Debug)]
pub struct Tables {
    pub captures: tables::Captures,
    pub collections: tables::Collections,
    pub derivations: tables::Derivations,
    pub endpoints: tables::Endpoints,
    pub errors: tables::Errors,
    pub fetches: tables::Fetches,
    pub imports: tables::Imports,
    pub journal_rules: tables::JournalRules,
    pub materializations: tables::Materializations,
    pub named_schemas: tables::NamedSchemas,
    pub npm_dependencies: tables::NPMDependencies,
    pub projections: tables::Projections,
    pub resources: tables::Resources,
    pub schema_docs: tables::SchemaDocs,
    pub test_steps: tables::TestSteps,
    pub transforms: tables::Transforms,
}

/// Fetcher provides a capability for resolving resources URLs to contents.
pub trait Fetcher {
    fn fetch<'a>(
        &'a self,
        resource: &'a Url,
        content_type: &'a ContentType,
    ) -> LocalBoxFuture<'a, Result<bytes::Bytes, anyhow::Error>>;
}

/// Loader provides a stack-based driver for traversing catalog source
/// models, with dispatch to a Visitor trait and having fine-grained
/// tracking of location context.
pub struct Loader<F: Fetcher> {
    // Tables loaded by the build process.
    tables: RefCell<Tables>,
    // Fetcher for retrieving discovered, unvisited resources.
    fetcher: F,
}

impl<F: Fetcher> Loader<F> {
    /// Build and return a new Loader.
    pub fn new(tables: Tables, fetcher: F) -> Loader<F> {
        Loader {
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
        content_type: ContentType,
    ) {
        // Mark as visited, so that recursively-loaded imports don't re-visit.
        self.tables.borrow_mut().fetches.push_row(resource);

        let content = self.fetcher.fetch(&resource, &content_type).await;

        match content {
            Ok(content) => {
                self.load_resource_content(scope, resource, content, content_type)
                    .await
            }
            Err(err) if matches!(content_type, ContentType::TypescriptModule) => {
                // Not every catalog spec need have an accompanying TypescriptModule.
                // We optimistically load them, but do not consider it an error if
                // it doesn't exist. We'll do more handling of this condition within
                // Typescript building, including surfacing compiler errors of missing
                // files and potentially stubbing an implementation for the user.
                tracing::debug!(?err, %resource, "did not fetch typescript module");
            }
            Err(err) => {
                self.tables.borrow_mut().errors.push_row(
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
        content_type: ContentType,
    ) -> LocalBoxFuture<'a, ()> {
        async move {
            self.tables
                .borrow_mut()
                .resources
                .push_row(resource.clone(), &content_type, &content);
            let scope = scope.push_resource(&resource);

            match content_type {
                ContentType::CatalogSpec => self.load_catalog(scope, content.as_ref()).await,
                ContentType::JsonSchema => self.load_schema_document(scope, content.as_ref()).await,
                _ => None,
            };
            ()
        }
        .boxed_local()
    }

    async fn load_schema_document<'s>(&'s self, scope: Scope<'s>, content: &[u8]) -> Option<()> {
        let dom: serde_json::Value = self.fallible(scope, serde_yaml::from_slice(&content))?;
        let doc: CompiledSchema =
            self.fallible(scope, build_schema(scope.resource().clone(), &dom))?;

        let mut index = doc::SchemaIndex::new();
        self.fallible(scope, index.add(&doc))?;

        self.load_schema_node(scope, &index, &doc).await;

        self.tables
            .borrow_mut()
            .schema_docs
            .push_row(scope.flatten(), dom);

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
                    if let Some(anchor) = anchor_uri
                        .as_str()
                        .split('#')
                        .next_back()
                        .filter(|s| NAMED_SCHEMA_RE.is_match(s))
                    {
                        self.tables.borrow_mut().named_schemas.push_row(
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
                                self.load_import(scope, &uri, ContentType::JsonSchema, true)
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
        schema: specs::Schema,
    ) -> Option<Url> {
        // If schema is a relative URL, then import it.
        if let specs::Schema::Url(import) = schema {
            let mut import = self.fallible(scope, scope.resource().join(import.as_ref()))?;

            // Temporarily strip schema fragment to import base document.
            let fragment = import.fragment().map(str::to_string);
            import.set_fragment(None);

            self.load_import(scope, &import, ContentType::JsonSchema, true)
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
                ContentType::JsonSchema,
            )
            .await;

            self.tables.borrow_mut().imports.push_row(
                scope.flatten(),
                scope.resource(),
                &import,
                true,
            );
            Some(import)
        }
    }

    // Load an import to another resource, recursively fetching if not yet visited.
    async fn load_import<'s>(
        &'s self,
        scope: Scope<'s>,
        import: &'s Url,
        content_type: ContentType,
        include: bool,
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

        self.tables.borrow_mut().imports.push_row(
            scope.flatten(),
            scope.resource(),
            import,
            include,
        );
    }

    // Load a top-level catalog specification.
    async fn load_catalog<'s>(&'s self, scope: Scope<'s>, content: &[u8]) -> Option<()> {
        let dom: serde_yaml::Value = self.fallible(scope, serde_yaml::from_slice(&content))?;
        let dom: serde_yaml::Value =
            self.fallible(scope, yaml_merge_keys::merge_keys_serde(dom))?;

        let specs::Catalog {
            _schema,
            include,
            npm_dependencies,
            journal_rules,
            collections,
            endpoints,
            materializations,
            captures,
            tests,
        } = self.fallible(scope, serde_yaml::from_value(dom))?;

        // Collect NPM dependencies.
        for (package, version) in npm_dependencies {
            let scope = scope
                .push_prop("npmDependencies")
                .push_prop(&package)
                .flatten();

            self.tables
                .borrow_mut()
                .npm_dependencies
                .push_row(scope, package, version);
        }

        // Collect journal rules.
        for (name, mut rule) in journal_rules {
            let scope = scope.push_prop("journal_rules").push_prop(&name).flatten();

            rule.rule = name.to_string();
            self.tables
                .borrow_mut()
                .journal_rules
                .push_row(scope, name, rule.into_proto());
        }

        // Task which loads all imports.
        let import = include.into_iter().enumerate().map(|(index, import)| {
            async move {
                let scope = scope.push_prop("include");
                let scope = scope.push_item(index);

                // Map from relative to absolute URL.
                if let Some(import) = self.fallible(scope, scope.resource().join(import.as_ref())) {
                    self.load_import(scope, &import, ContentType::CatalogSpec, true)
                        .await;
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
            self.load_import(scope, &module, ContentType::TypescriptModule, true)
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

        // Collect endpoints.
        for (name, endpoint) in endpoints {
            let scope = scope.push_prop("endpoints");
            let scope = scope.push_prop(name.as_ref());
            let endpoint_type = endpoint.endpoint_type();

            if let Some(base_config) = self.load_endpoint_config(scope, endpoint) {
                self.tables.borrow_mut().endpoints.push_row(
                    scope.flatten(),
                    name,
                    endpoint_type,
                    base_config,
                );
            }
        }

        // Collect captures.
        for (name, capture) in captures {
            let scope = scope
                .push_prop("captures")
                .push_prop(name.as_ref())
                .flatten();

            let (allow_push, endpoint, patch_config) = match capture.inner {
                specs::CaptureType::PushAPI(config) => (true, None, config),
                specs::CaptureType::Endpoint(specs::EndpointRef { name, config }) => {
                    (false, Some(name), config)
                }
            };

            self.tables.borrow_mut().captures.push_row(
                scope,
                name,
                &capture.target.name,
                allow_push,
                endpoint,
                serde_json::Value::Object(patch_config),
            );
        }

        // Collect materializations.
        for (materialization, spec) in materializations {
            let scope = scope
                .push_prop("materializations")
                .push_prop(materialization.as_ref())
                .flatten();

            let specs::MaterializationDef {
                source: specs::MaterializationSource { name: collection },
                endpoint:
                    specs::EndpointRef {
                        name: endpoint,
                        config: patch_config,
                    },
                fields:
                    specs::MaterializationFields {
                        include: fields_include,
                        exclude: fields_exclude,
                        recommended: fields_recommended,
                    },
            } = spec;

            self.tables.borrow_mut().materializations.push_row(
                scope,
                collection,
                endpoint,
                fields_exclude,
                fields_include,
                fields_recommended,
                materialization,
                serde_json::Value::Object(patch_config),
            );
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

                let (collection, documents, partitions, step_type) = match spec {
                    specs::TestStep::Ingest(specs::TestStepIngest {
                        collection,
                        documents,
                    }) => (collection, documents, None, TestStepType::Ingest),

                    specs::TestStep::Verify(specs::TestStepVerify {
                        collection,
                        documents,
                        partitions,
                    }) => (collection, documents, partitions, TestStepType::Verify),
                };

                self.tables.borrow_mut().test_steps.push_row(
                    scope,
                    collection,
                    documents,
                    partitions,
                    step_index as u32,
                    step_type,
                    test,
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
        collection_name: &'s names::Collection,
        collection: specs::CollectionDef,
    ) {
        let specs::CollectionDef {
            schema,
            key,
            projections,
            derivation,
        } = collection;

        // Visit all collection projections.
        for (field, projection) in projections.iter() {
            let (location, partition) = match projection {
                specs::Projection::Pointer(location) => (location, false),
                specs::Projection::Object {
                    location,
                    partition,
                } => (location, *partition),
            };

            self.tables.borrow_mut().projections.push_row(
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
            self.tables.borrow_mut().collections.push_row(
                scope.flatten(),
                collection_name,
                schema,
                key,
            );
        }
    }

    async fn load_derivation<'s>(
        &'s self,
        scope: Scope<'s>,
        derivation_name: &'s names::Collection,
        derivation: specs::Derivation,
    ) {
        let specs::Derivation {
            register:
                specs::Register {
                    schema: register_schema,
                    initial: register_initial,
                },
            transform,
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
            self.tables.borrow_mut().derivations.push_row(
                scope.flatten(),
                derivation_name,
                register_schema,
                register_initial,
            );
        }
    }

    async fn load_transform<'s>(
        &'s self,
        scope: Scope<'s>,
        transform_name: &'s names::Transform,
        derivation: &'s names::Collection,
        transform: specs::Transform,
    ) {
        let specs::Transform {
            source:
                specs::TransformSource {
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

        let (shuffle_key, shuffle_lambda, shuffle_hash) = match shuffle {
            Some(specs::Shuffle::Key(key)) => (Some(key), None, ShuffleHash::None),
            Some(specs::Shuffle::Md5(key)) => (Some(key), None, ShuffleHash::Md5),
            Some(specs::Shuffle::Lambda(lambda)) => (None, Some(lambda), ShuffleHash::None),
            None => (None, None, ShuffleHash::None),
        };
        let (rollback_on_register_conflict, update_lambda) = match update {
            Some(specs::Update {
                rollback_on_conflict,
                lambda,
            }) => (rollback_on_conflict, Some(lambda)),
            None => (false, None),
        };
        let publish_lambda = match publish {
            Some(specs::Publish { lambda }) => Some(lambda),
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

        self.tables.borrow_mut().transforms.push_row(
            scope.flatten(),
            derivation,
            priority,
            publish_lambda,
            read_delay.map(|d| d.as_secs() as u32),
            rollback_on_register_conflict,
            shuffle_hash,
            shuffle_key,
            shuffle_lambda,
            source,
            source_partitions,
            source_schema,
            transform_name,
            update_lambda,
        );
    }

    fn load_endpoint_config<'s>(
        &'s self,
        scope: Scope<'s>,
        endpoint: specs::EndpointDef,
    ) -> Option<serde_json::Value> {
        match endpoint {
            specs::EndpointDef::GS(cfg) => Some(serde_json::to_value(cfg).unwrap()),
            specs::EndpointDef::Postgres(cfg) => Some(serde_json::to_value(cfg).unwrap()),
            specs::EndpointDef::Remote(cfg) => Some(serde_json::to_value(cfg).unwrap()),
            specs::EndpointDef::S3(cfg) => Some(serde_json::to_value(cfg).unwrap()),
            specs::EndpointDef::Sqlite(mut cfg) => {
                // Resolve relative database path relative to current scope.
                if let Some(path) = self.fallible(scope, scope.resource().join(cfg.path.as_ref())) {
                    cfg.path = specs::RelativeUrl(path.to_string());
                    Some(serde_json::to_value(cfg).unwrap())
                } else {
                    None
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
                    .push_row(scope.flatten(), anyhow::anyhow!(err.into()));
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
