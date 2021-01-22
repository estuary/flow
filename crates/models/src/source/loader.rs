use super::wrappers::{CollectionName, ContentType, ShuffleHash, TransformName};
use super::{specs, Scope, Tables};

use doc::Schema as CompiledSchema;
use futures::future::{FutureExt, LocalBoxFuture};
use json::schema::{build::build_schema, Application, Keyword};
use std::cell::RefCell;
use std::future::Future;
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum LoadError {
    #[error("failed to parse URL")]
    URLParse(#[from] url::ParseError),
    #[error("failed to fetch resource {uri}")]
    Fetch {
        uri: String,
        #[source]
        detail: Box<dyn std::error::Error + Send + Sync>,
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

pub type FetchResult = Result<Box<[u8]>, Box<dyn std::error::Error + Send + Sync>>;

/// Loader provides a stack-based driver for traversing catalog source
/// models, with dispatch to a Visitor trait and having fine-grained
/// tracking of location context.
pub struct Loader<F, FF>
where
    F: FnMut(&Url) -> FF,
    FF: Future<Output = FetchResult>,
{
    // Tables loaded by the build process.
    tables: RefCell<Tables>,
    // Dynamic fetch function for retrieving discovered, unvisited resources.
    fetch: RefCell<F>,
}

impl<F, FF> Loader<F, FF>
where
    F: FnMut(&Url) -> FF,
    FF: Future<Output = FetchResult>,
{
    /// Build and return a new Loader.
    pub fn new(tables: Tables, fetch: F) -> Loader<F, FF> {
        Loader {
            tables: RefCell::new(tables),
            fetch: RefCell::new(fetch),
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
        self.tables.borrow_mut().fetches.push_row(resource.clone());

        let content = (self.fetch.borrow_mut())(&resource);
        let content = content.await.map_err(|e| LoadError::Fetch {
            uri: resource.to_string(),
            detail: e,
        });

        if let Some(content) = self.fallible(scope, content) {
            self.load_resource_content(scope, resource, &content, content_type)
                .await;
        }
    }

    // Resources are loaded recursively, and Rust requires that recursive
    // async calls be made through a boxed future. Otherwise, the generated
    // state machine would have infinite size!
    fn load_resource_content<'a>(
        &'a self,
        scope: Scope<'a>,
        resource: &'a Url,
        content: &'a [u8],
        content_type: ContentType,
    ) -> LocalBoxFuture<'a, ()> {
        async move {
            self.tables.borrow_mut().resources.push_row(
                resource.clone(),
                content_type,
                content.to_vec(),
            );
            let scope = scope.push_resource(&resource);

            match content_type {
                ContentType::CatalogSpec => self.load_catalog(scope, content).await,
                ContentType::JsonSchema => self.load_schema_document(scope, content).await,
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
        // Build an iterator that returns a Future for each Application keyword.
        // That future in turn:
        // * Builds a recursive future A from it's contained schema, and
        // * Builds a future B which fetches a referenced external resource, if present.
        // Then futures A & B are concurrently joined.
        let it = schema.kw.iter().filter_map(move |kw| match kw {
            Keyword::Application(app, child) => Some(async move {
                // Add Application keywords to the Scope's Location.
                let location = app.push_keyword(&scope.location);
                let scope = Scope {
                    location: app.push_keyword_target(&location),
                    ..scope
                };

                // Map to an external URL that's not contained by this CompiledSchema.
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

                // Recursive call to walk the schema.
                let recurse = self.load_schema_node(scope, index, child);

                if let Some(uri) = uri {
                    // Concurrently fetch |uri| while continuing to walk the schema.
                    let ((), ()) = futures::join!(
                        recurse,
                        self.load_import(scope, &uri, ContentType::JsonSchema)
                    );
                } else {
                    let () = recurse.await;
                }
            }),
            _ => None,
        });

        // Join all futures of the iterator.
        futures::future::join_all(it)
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

            self.load_import(scope, &import, ContentType::JsonSchema)
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
                &serde_json::to_vec(&schema).unwrap(),
                ContentType::JsonSchema,
            )
            .await;

            self.tables
                .borrow_mut()
                .imports
                .push_row(scope.flatten(), scope.resource(), &import);
            Some(import)
        }
    }

    // Load an import to another resource, recursively fetching if not yet visited.
    async fn load_import<'s>(
        &'s self,
        scope: Scope<'s>,
        import: &'s Url,
        content_type: ContentType,
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
            .push_row(scope.flatten(), scope.resource(), import);
    }

    // Load a top-level catalog specification.
    async fn load_catalog<'s>(&'s self, scope: Scope<'s>, content: &[u8]) -> Option<()> {
        let dom: serde_yaml::Value = self.fallible(scope, serde_yaml::from_slice(&content))?;
        let dom: serde_yaml::Value =
            self.fallible(scope, yaml_merge_keys::merge_keys_serde(dom))?;

        let specs::Catalog {
            _schema,
            import,
            node_dependencies,
            collections,
            endpoints,
            materializations,
            captures,
            tests,
        } = self.fallible(scope, serde_yaml::from_value(dom))?;

        // Collect NodeJS dependencies.
        for (package, version) in node_dependencies {
            let scope = scope
                .push_prop("nodeDependencies")
                .push_prop(&package)
                .flatten();

            self.tables
                .borrow_mut()
                .nodejs_dependencies
                .push_row(scope, package, version);
        }

        // Task which loads all imports.
        let import = import.into_iter().enumerate().map(|(index, import)| {
            async move {
                let scope = scope.push_prop("import");
                let scope = scope.push_item(index);

                // Map from relative to absolute URL.
                if let Some(import) = self.fallible(scope, scope.resource().join(import.as_ref())) {
                    self.load_import(scope, &import, ContentType::CatalogSpec)
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

        // Collect endpoints.
        for (name, endpoint) in endpoints {
            let scope = scope
                .push_prop("endpoints")
                .push_prop(name.as_ref())
                .flatten();

            self.tables.borrow_mut().endpoints.push_row(
                scope,
                name,
                endpoint.endpoint_type(),
                endpoint.base_config(),
            );
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
        for (name, materialization) in materializations {
            let scope = scope
                .push_prop("materializations")
                .push_prop(name.as_ref())
                .flatten();

            let specs::Materialization {
                source: specs::MaterializationSource { name: source },
                endpoint:
                    specs::EndpointRef {
                        name: endpoint,
                        config: patch_config,
                    },
                fields,
            } = materialization;

            self.tables.borrow_mut().materializations.push_row(
                scope,
                name,
                source,
                endpoint,
                serde_json::Value::Object(patch_config),
                fields,
            );
        }

        // Collect tests.
        for (name, steps) in tests {
            for (index, step) in steps.into_iter().enumerate() {
                let scope = scope
                    .push_prop("tests")
                    .push_prop(&name)
                    .push_item(index)
                    .flatten();
                let name = name.clone();

                self.tables
                    .borrow_mut()
                    .test_steps
                    .push_row(scope, name, index as u32, step);
            }
        }

        let (_, _): (Vec<()>, Vec<()>) = futures::join!(import, collections);
        Some(())
    }

    async fn load_collection<'s>(
        &'s self,
        scope: Scope<'s>,
        collection_name: &'s CollectionName,
        collection: specs::Collection,
    ) {
        let specs::Collection {
            schema,
            key,
            store:
                specs::EndpointRef {
                    name: store,
                    config: store_patch_config,
                },
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
                store,
                serde_json::Value::Object(store_patch_config),
            );
        }
    }

    async fn load_derivation<'s>(
        &'s self,
        scope: Scope<'s>,
        derivation_name: &'s CollectionName,
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
        transform_name: &'s TransformName,
        derivation: &'s CollectionName,
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
            Some(specs::Shuffle::MD5(key)) => (Some(key), None, ShuffleHash::Md5),
            Some(specs::Shuffle::Lambda(lambda)) => (None, Some(lambda), ShuffleHash::None),
            None => (None, None, ShuffleHash::None),
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
            transform_name,
            derivation,
            source,
            source_partitions,
            source_schema,
            shuffle_key,
            shuffle_lambda,
            shuffle_hash,
            read_delay.map(|d| d.as_secs() as u32),
            priority,
            update,
            publish,
        );
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
