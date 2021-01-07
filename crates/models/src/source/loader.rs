use super::wrappers::{
    CaptureName, CollectionName, ContentType, EndpointName, EndpointType, JsonPointer,
    MaterializationName, ShuffleHash, TestName, TransformName,
};
use super::{specs, Scope};

use doc::Schema as CompiledSchema;
use futures::future::{FutureExt, LocalBoxFuture};
use futures::TryFutureExt;
use json::schema::{build::build_schema, Application, Keyword};
use std::cell::RefCell;
use std::collections::HashSet;
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

pub trait Visitor {
    type Error: std::error::Error + 'static;

    /// Notification that a resource fetch has begun.
    fn visit_fetch<'a>(&mut self, scope: Scope<'a>, resource: &Url) -> Result<(), Self::Error>;

    /// Notification that a resource of the given content-type and content has been loaded.
    /// visit_resource is called immediately before entities of the resource are visited,
    /// so Visitor implementations may want to clear out any retained prior entries from a
    /// previous visitation of this resource.
    fn visit_resource<'a>(
        &mut self,
        scope: Scope<'a>,
        resource: &Url,
        content_type: ContentType,
        content: &[u8],
    ) -> Result<(), Self::Error>;

    fn visit_import<'a>(
        &mut self,
        scope: Scope<'a>,
        parent_uri: &Url,
        child_uri: &Url,
    ) -> Result<(), Self::Error>;

    fn visit_nodejs_dependency<'a>(
        &mut self,
        scope: Scope<'a>,
        package: &str,
        version: &str,
    ) -> Result<(), Self::Error>;

    fn visit_collection<'a>(
        &mut self,
        scope: Scope<'a>,
        collection: &CollectionName,
        schema: &Url,
        key: &specs::CompositeKey,
        store: &EndpointName,
        patch_config: &serde_json::Value,
    ) -> Result<(), Self::Error>;

    fn visit_projection<'a>(
        &mut self,
        scope: Scope<'a>,
        collection: &CollectionName,
        field: &str,
        location: &JsonPointer,
        partition: bool,
        user_provided: bool,
    ) -> Result<(), Self::Error>;

    fn visit_derivation<'a>(
        &mut self,
        scope: Scope<'a>,
        derivation: &CollectionName,
        register_schema: &Url,
        register_initial: &serde_json::Value,
    ) -> Result<(), Self::Error>;

    fn visit_transform<'a>(
        &mut self,
        scope: Scope<'a>,
        transform: &TransformName,
        derivation: &CollectionName,
        source: &CollectionName,
        source_partitions: Option<&specs::PartitionSelector>,
        source_schema: Option<&Url>,
        shuffle_key: Option<&specs::CompositeKey>,
        shuffle_lambda: Option<&specs::Lambda>,
        shuffle_hash: ShuffleHash,
        read_delay: Option<std::time::Duration>,
        update: Option<&specs::Lambda>,
        publish: Option<&specs::Lambda>,
    ) -> Result<(), Self::Error>;

    fn visit_endpoint<'a>(
        &mut self,
        scope: Scope<'a>,
        endpoint: &EndpointName,
        endpoint_type: EndpointType,
        base_config: &serde_json::Value,
    ) -> Result<(), Self::Error>;

    fn visit_materialization<'a>(
        &mut self,
        scope: Scope<'a>,
        materialization: &MaterializationName,
        source: &CollectionName,
        source_schema: Option<&Url>,
        endpoint: &EndpointName,
        patch_config: &serde_json::Value,
        fields: &specs::FieldSelector,
    ) -> Result<(), Self::Error>;

    fn visit_capture<'a>(
        &mut self,
        scope: Scope<'a>,
        capture: &CaptureName,
        target: &CollectionName,
        allow_push: bool,
        endpoint: Option<&EndpointName>,
        patch_config: Option<&serde_json::Value>,
    ) -> Result<(), Self::Error>;

    fn visit_test<'a>(
        &mut self,
        scope: Scope<'a>,
        test: &TestName,
        total_steps: usize,
    ) -> Result<(), Self::Error>;

    fn visit_test_step_ingest<'a>(
        &mut self,
        scope: Scope<'a>,
        test: &TestName,
        step_index: usize,
        collection: &CollectionName,
        documents: &[serde_json::Value],
    ) -> Result<(), Self::Error>;

    fn visit_test_step_verify<'a>(
        &mut self,
        scope: Scope<'a>,
        test: &TestName,
        step_index: usize,
        collection: &CollectionName,
        documents: &[serde_json::Value],
        partitions: Option<&specs::PartitionSelector>,
    ) -> Result<(), Self::Error>;

    fn visit_schema_document<'a>(
        &mut self,
        scope: Scope<'a>,
        dom: &serde_json::Value,
    ) -> Result<(), Self::Error>;

    fn visit_catalog<'a>(&mut self, scope: Scope<'a>) -> Result<(), Self::Error>;

    fn visit_error<'a>(&mut self, scope: Scope<'a>, err: LoadError) -> Result<(), Self::Error>;
}

pub type FetchResult = Result<Box<[u8]>, Box<dyn std::error::Error + Send + Sync>>;

/// Loader provides a stack-based driver for traversing catalog source
/// models, with dispatch to a Visitor trait and having fine-grained
/// tracking of location context.
pub struct Loader<V, F, FF>
where
    V: Visitor,
    F: FnMut(&Url) -> FF,
    FF: Future<Output = FetchResult>,
{
    /// The database connection to use during the build process.
    visitor: RefCell<V>,
    /// Dynamic fetch function for retrieving discovered, unvisited resources.
    fetch: RefCell<F>,
    // Marked resources which are currently being loaded with senders
    // for each waiting task (if Some), or have completed loaded (None).
    visited: RefCell<HashSet<Url>>,
}

impl<V, F, FF> Loader<V, F, FF>
where
    V: Visitor,
    F: FnMut(&Url) -> FF,
    FF: Future<Output = FetchResult>,
{
    /// Build and return a new Loader.
    pub fn new(visitor: V, fetch: F) -> Loader<V, F, FF> {
        Loader {
            visitor: RefCell::new(visitor),
            fetch: RefCell::new(fetch),
            visited: RefCell::new(HashSet::new()),
        }
    }

    /// Consume this Loader, returning its wrapped Visitor.
    pub fn into_visitor(self) -> V {
        let Loader { visitor, .. } = self;
        visitor.into_inner()
    }

    /// Load (or re-load) a resource of the given ContentType.
    pub async fn load_resource<'a>(
        &'a self,
        scope: Scope<'a>,
        resource: &'a Url,
        content_type: ContentType,
    ) -> Result<(), V::Error> {
        // Mark as visited, so that recursively-loaded imports don't re-visit.
        self.visited.borrow_mut().insert(resource.clone());
        self.visitor.borrow_mut().visit_fetch(scope, resource)?;

        let content = (self.fetch.borrow_mut())(&resource);
        let content = content.await.map_err(|e| LoadError::Fetch {
            uri: resource.to_string(),
            detail: e,
        });

        if let Some(content) = opt_err_to_ok(self.fallible(scope, content))? {
            self.load_resource_content(scope, resource, &content, content_type)
                .await?;
        }
        Ok(())
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
    ) -> LocalBoxFuture<'a, Result<(), V::Error>> {
        async move {
            self.visitor
                .borrow_mut()
                .visit_resource(scope, &resource, content_type, &content)?;

            let scope = scope.push_resource(&resource);

            match content_type {
                ContentType::CatalogSpec => self.load_catalog(scope, content).await?,
                ContentType::JsonSchema => self.load_schema_document(scope, content).await?,
                _ => (),
            }

            Ok(())
        }
        .map(|r| r.or_else(ok_if_none))
        .boxed_local()
    }

    async fn load_schema_document<'s>(
        &'s self,
        scope: Scope<'s>,
        content: &[u8],
    ) -> Result<(), Option<V::Error>> {
        let dom: serde_json::Value = self.fallible(scope, serde_yaml::from_slice(&content))?;
        let root: CompiledSchema =
            self.fallible(scope, build_schema(scope.resource().clone(), &dom))?;

        let mut index = doc::SchemaIndex::new();
        self.fallible(scope, index.add(&root))?;

        self.load_schema_node(scope, &index, &root).await?;

        self.visitor
            .borrow_mut()
            .visit_schema_document(scope, &dom)?;

        Ok(())
    }

    fn load_schema_node<'s>(
        &'s self,
        scope: Scope<'s>,
        index: &'s doc::SchemaIndex<'s>,
        schema: &'s CompiledSchema,
    ) -> LocalBoxFuture<'s, Result<(), V::Error>> {
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
                    let ((), ()) = futures::try_join!(
                        recurse,
                        self.load_import(scope, &uri, ContentType::JsonSchema)
                    )?;
                } else {
                    let () = recurse.await?;
                }

                Ok(())
            }),
            _ => None,
        });

        // Join all futures of the iterator.
        futures::future::try_join_all(it)
            .map_ok(|_: Vec<()>| ())
            .boxed_local()
    }

    /// Load a schema reference, which may be an inline schema.
    async fn load_schema_reference<'s>(
        &'s self,
        scope: Scope<'s>,
        schema: specs::Schema,
    ) -> Result<Url, Option<V::Error>> {
        // If schema is a relative URL, then import it.
        if let specs::Schema::Url(import) = schema {
            let mut import = self.fallible(scope, scope.resource().join(import.as_ref()))?;

            // Temporarily strip schema fragment to import base document.
            let fragment = import.fragment().map(str::to_string);
            import.set_fragment(None);

            self.load_import(scope, &import, ContentType::JsonSchema)
                .await?;

            import.set_fragment(fragment.as_deref());
            Ok(import)
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
            .await?;

            self.visitor
                .borrow_mut()
                .visit_import(scope, scope.resource(), &import)?;
            Ok(import)
        }
    }

    // Load an import to another resource, recursively fetching if not yet visited.
    async fn load_import<'s>(
        &'s self,
        scope: Scope<'s>,
        import: &'s Url,
        content_type: ContentType,
    ) -> Result<(), V::Error> {
        // Recursively process the import if it's not already visited.
        if !self.visited.borrow().contains(&import) {
            self.load_resource(scope, &import, content_type).await?;
        }

        self.visitor
            .borrow_mut()
            .visit_import(scope, scope.resource(), &import)?;

        Ok(())
    }

    // Load a top-level catalog specification.
    async fn load_catalog<'s>(
        &'s self,
        scope: Scope<'s>,
        content: &[u8],
    ) -> Result<(), Option<V::Error>> {
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

        // Visit NPM dependencies.
        for (package, version) in node_dependencies.iter() {
            self.visitor.borrow_mut().visit_nodejs_dependency(
                scope.push_prop("nodeDependencies").push_prop(package),
                package,
                version,
            )?;
        }

        // Task which loads all imports.
        let import = import.into_iter().enumerate().map(|(index, import)| {
            async move {
                let scope = scope.push_prop("import");
                let scope = scope.push_item(index);

                // Map from relative to absolute URL.
                let import = self.fallible(scope, scope.resource().join(import.as_ref()))?;

                self.load_import(scope, &import, ContentType::CatalogSpec)
                    .await?;
                Ok(())
            }
            .map(|r| r.or_else(ok_if_none))
        });
        let import = futures::future::try_join_all(import);

        // Task which loads all collections.
        let collections = collections
            .into_iter()
            .map(|(name, collection)| async move {
                self.load_collection(
                    scope.push_prop("collections").push_prop(name.as_ref()),
                    &name,
                    collection,
                )
                .await
            });
        let collections = futures::future::try_join_all(collections);

        // Visit endpoints.
        for (name, endpoint) in endpoints {
            self.visitor.borrow_mut().visit_endpoint(
                scope.push_prop("endpoints").push_prop(name.as_ref()),
                &name,
                endpoint.endpoint_type(),
                &endpoint.base_config(),
            )?;
        }

        // Visit captures.
        for (name, capture) in captures {
            self.load_capture(
                scope.push_prop("captures").push_prop(name.as_ref()),
                &name,
                capture,
            )?;
        }

        // Task which loads all materializations.
        let materializations =
            materializations
                .into_iter()
                .map(|(name, materialization)| async move {
                    self.load_materialization(
                        scope.push_prop("materializations").push_prop(name.as_ref()),
                        &name,
                        materialization,
                    )
                    .await
                });
        let materializations = futures::future::try_join_all(materializations);

        // Visit tests.
        for (name, steps) in tests {
            self.load_test_case(
                scope.push_prop("tests").push_prop(name.as_ref()),
                &name,
                steps,
            )?;
        }

        let (_, _, _): (Vec<()>, Vec<()>, Vec<()>) =
            futures::try_join!(import, collections, materializations)?;

        self.visitor.borrow_mut().visit_catalog(scope)?;
        Ok(())
    }

    async fn load_collection<'s>(
        &'s self,
        scope: Scope<'s>,
        collection_name: &'s CollectionName,
        collection: specs::Collection,
    ) -> Result<(), V::Error> {
        let specs::Collection {
            schema,
            key,
            store:
                specs::EndpointRef {
                    name: store,
                    config: patch_config,
                },
            projections,
            derivation,
        } = collection;

        // Visit all collection projections.
        for (field, projection) in projections.iter() {
            match projection {
                specs::Projection::Pointer(location) => {
                    self.visitor.borrow_mut().visit_projection(
                        scope.push_prop("projections").push_prop(field),
                        collection_name,
                        field,
                        &location,
                        false,
                        true,
                    )?;
                }
                specs::Projection::Object {
                    location,
                    partition,
                } => {
                    self.visitor.borrow_mut().visit_projection(
                        scope.push_prop("projections").push_prop(field),
                        collection_name,
                        field,
                        &location,
                        *partition,
                        true,
                    )?;
                }
            }
        }

        // Task which loads & maps collection schema => URL.
        // Recoverable failures project to Ok(None).
        let schema = self
            .load_schema_reference(scope.push_prop("schema"), schema)
            .map(opt_err_to_ok);

        // If this collection is a derivation, concurrently
        // load the collection's schema and its derivation.
        let schema = match derivation {
            Some(derivation) => {
                let derivation = self.load_derivation(
                    scope.push_prop("derivation"),
                    collection_name,
                    derivation,
                );

                let (schema, ()) = futures::try_join!(schema, derivation)?;
                schema
            }
            None => schema.await?,
        };

        if let Some(schema) = schema {
            self.visitor.borrow_mut().visit_collection(
                scope,
                collection_name,
                &schema,
                &key,
                &store,
                &patch_config,
            )?;
        }
        Ok(())
    }

    async fn load_derivation<'s>(
        &'s self,
        scope: Scope<'s>,
        derivation_name: &'s CollectionName,
        derivation: specs::Derivation,
    ) -> Result<(), V::Error> {
        let specs::Derivation {
            register:
                specs::Register {
                    schema: register_schema,
                    initial: register_initial,
                },
            transform,
        } = derivation;

        // Task which loads & maps register schema => URL.
        // Recoverable failures project to Ok(None).
        let register_schema = async move {
            self.load_schema_reference(
                scope.push_prop("register").push_prop("schema"),
                register_schema,
            )
            .await
        }
        .map(opt_err_to_ok);

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
        let transforms = futures::future::try_join_all(transforms);

        let (register_schema, _): (_, Vec<()>) = futures::try_join!(register_schema, transforms)?;

        if let Some(register_schema) = register_schema {
            self.visitor.borrow_mut().visit_derivation(
                scope,
                derivation_name,
                &register_schema,
                &register_initial,
            )?;
        }
        Ok(())
    }

    async fn load_transform<'s>(
        &'s self,
        scope: Scope<'s>,
        transform_name: &'s TransformName,
        derivation: &'s CollectionName,
        transform: specs::Transform,
    ) -> Result<(), V::Error> {
        let specs::Transform {
            source:
                specs::TransformSource {
                    name: source,
                    schema: source_schema,
                    partitions: source_partitions,
                },
            read_delay,
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
            Some(url) => match self
                .load_schema_reference(scope.push_prop("source").push_prop("schema"), url)
                .map(opt_err_to_ok)
                .await?
            {
                Some(url) => Some(url),
                None => return Ok(()), // Error while loading.
            },
            None => None,
        };

        self.visitor.borrow_mut().visit_transform(
            scope,
            transform_name,
            derivation,
            &source,
            source_partitions.as_ref(),
            source_schema.as_ref(),
            shuffle_key.as_ref(),
            shuffle_lambda.as_ref(),
            shuffle_hash,
            read_delay,
            update.as_ref(),
            publish.as_ref(),
        )?;

        Ok(())
    }

    async fn load_materialization<'s>(
        &'s self,
        scope: Scope<'s>,
        materialization_name: &'s MaterializationName,
        materialization: specs::Materialization,
    ) -> Result<(), V::Error> {
        let specs::Materialization {
            source:
                specs::MaterializationSource {
                    name: source,
                    schema: source_schema,
                },
            endpoint:
                specs::EndpointRef {
                    name: endpoint,
                    config: patch_config,
                },
            fields,
        } = materialization;

        // Map optional source schema => URL.
        let source_schema = match source_schema {
            Some(url) => match self
                .load_schema_reference(scope.push_prop("source").push_prop("schema"), url)
                .map(opt_err_to_ok)
                .await?
            {
                Some(url) => Some(url),
                None => return Ok(()), // Error while loading.
            },
            None => None,
        };

        self.visitor.borrow_mut().visit_materialization(
            scope,
            materialization_name,
            &source,
            source_schema.as_ref(),
            &endpoint,
            &patch_config,
            &fields,
        )?;

        Ok(())
    }

    fn load_capture<'s>(
        &'s self,
        scope: Scope<'s>,
        name: &'s CaptureName,
        capture: specs::Capture,
    ) -> Result<(), V::Error> {
        let (allow_push, endpoint, patch_config) = match capture.inner {
            specs::CaptureType::PushAPI => (true, None, None),
            specs::CaptureType::Endpoint(specs::EndpointRef { name, config }) => {
                (false, Some(name), Some(config))
            }
        };
        self.visitor.borrow_mut().visit_capture(
            scope,
            &name,
            &capture.target.name,
            allow_push,
            endpoint.as_ref(),
            patch_config.as_ref(),
        )?;
        Ok(())
    }

    fn load_test_case<'s>(
        &'s self,
        scope: Scope<'s>,
        name: &'s TestName,
        steps: Vec<specs::TestStep>,
    ) -> Result<(), V::Error> {
        let total_steps = steps.len();

        for (index, step) in steps.into_iter().enumerate() {
            let scope = scope.push_item(index);

            match step {
                specs::TestStep::Ingest(specs::TestStepIngest {
                    collection,
                    documents,
                }) => {
                    self.visitor.borrow_mut().visit_test_step_ingest(
                        scope,
                        &name,
                        index,
                        &collection,
                        &documents,
                    )?;
                }
                specs::TestStep::Verify(specs::TestStepVerify {
                    collection,
                    documents,
                    partitions,
                }) => {
                    self.visitor.borrow_mut().visit_test_step_verify(
                        scope,
                        &name,
                        index,
                        &collection,
                        &documents,
                        partitions.as_ref(),
                    )?;
                }
            }
        }

        self.visitor
            .borrow_mut()
            .visit_test(scope, &name, total_steps)?;

        Ok(())
    }

    // Consume a result capable of producing a LoadError, reporting any error to the visitor.
    // * If the input Result is Ok, its value is returned directly.
    // * If the input Result is Err and the visitor consumed it without error,
    //   Err(None) is returned. This provides an opportunity to abort out of local
    //   control flows which require this result to succeed. Macro control-flow
    //   can recover and continue from a local error by using the
    //   Result.or_else(ok_if_none) combinator.
    // * If the input Result is Err and the visitor produced on error on consumption,
    //   Err(Some(V::Error)) is returned.
    fn fallible<'s, T, E>(&self, scope: Scope<'s>, r: Result<T, E>) -> Result<T, Option<V::Error>>
    where
        E: Into<LoadError>,
    {
        match r {
            Ok(t) => Ok(t),
            Err(err) => match self.visitor.borrow_mut().visit_error(scope, err.into()) {
                Ok(()) => Err(None),
                Err(err) => Err(Some(err)),
            },
        }
    }
}

// ok_if_none maps a None option of the Error to Ok,
// and a Some(err) to an Err(Some(err)).
// Use with Result.or_else() to "hoist" a None error into
// an Ok(()).
fn ok_if_none<E>(e: Option<E>) -> Result<(), E> {
    match e {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn opt_err_to_ok<T, E>(r: Result<T, Option<E>>) -> Result<Option<T>, E> {
    match r {
        Ok(t) => Ok(Some(t)),
        Err(None) => Ok(None),
        Err(Some(e)) => Err(e),
    }
}
