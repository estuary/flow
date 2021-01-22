use crate::{collate, source, tables};

use doc::inference;
use doc::Schema as CompiledSchema;
use futures::future::LocalBoxFuture;
use futures::FutureExt;
use itertools::{EitherOrBoth, Itertools};
use json::schema::build::build_schema;
use json::schema::types;
use lazy_static::lazy_static;
use protocol::protocol::{journal_spec::Fragment as FragmentSpec, JournalSpec};
use protocol::{flow, materialize};
use regex::Regex;
use std::collections::HashMap;
use superslice::Ext;
use url::Url;

#[derive(Default, Debug)]
pub struct Tables {
    pub built_collections: tables::BuiltCollections,
    pub built_materializations: tables::BuiltMaterializations,
    pub errors: tables::Errors,
    pub implicit_projections: tables::Projections,
    pub inferences: tables::Inferences,
}

impl Tables {
    pub fn as_tables(&self) -> Vec<&dyn tables::TableObj> {
        // This de-structure ensures we can't fail to update if fields change.
        let Tables {
            built_collections,
            built_materializations,
            errors,
            implicit_projections,
            inferences,
        } = self;

        vec![
            built_collections,
            built_materializations,
            errors,
            implicit_projections,
            inferences,
        ]
    }

    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn tables::TableObj> {
        let Tables {
            built_collections,
            built_materializations,
            errors,
            implicit_projections,
            inferences,
        } = self;

        vec![
            built_collections,
            built_materializations,
            errors,
            implicit_projections,
            inferences,
        ]
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{entity} name cannot be empty")]
    NameEmpty { entity: &'static str },
    #[error("{name} cannot be used as name for {entity} ({unmatched:?} is invalid)")]
    NameRegex {
        entity: &'static str,
        name: String,
        unmatched: String,
    },
    #[error("{entity} {lhs} has a duplicated definition at {rhs_scope}")]
    Duplicate {
        entity: &'static str,
        lhs: String,
        rhs_scope: Url,
    },
    #[error("{entity} {lhs} is a prohibited prefix of {rhs}, defined at {rhs_scope}")]
    Prefix {
        entity: &'static str,
        lhs: String,
        rhs: String,
        rhs_scope: Url,
    },
    #[error("{ref_entity} {ref_name}, referenced by {this_entity} {this_name}, is not defined")]
    NoSuchEntity {
        this_entity: &'static str,
        this_name: String,
        ref_entity: &'static str,
        ref_name: String,
    },
    #[error("{ref_entity} {ref_name}, referenced by {this_entity} {this_name}, is not defined; did you mean {suggest_name} defined at {suggest_scope}?")]
    NoSuchEntitySuggest {
        this_entity: &'static str,
        this_name: String,
        ref_entity: &'static str,
        ref_name: String,
        suggest_name: String,
        suggest_scope: Url,
    },
    #[error("{this_entity} {this_name} references {ref_entity} {ref_name}, defined at {ref_scope}, without importing it or being imported by it")]
    MissingImport {
        this_entity: &'static str,
        this_name: String,
        ref_entity: &'static str,
        ref_name: String,
        ref_scope: Url,
    },
    #[error("referenced schema fragment location {schema} does not exist")]
    NoSuchSchema { schema: Url },
    #[error(
        "keyed location {ptr} (having type {type_:?}) must be required to exist by schema {schema}"
    )]
    KeyMayNotExist {
        ptr: String,
        type_: types::Set,
        schema: Url,
    },
    #[error("location {ptr} accepts {type_:?} in schema {schema}, but {disallowed:?} is disallowed in locations used as keys")]
    KeyWrongType {
        ptr: String,
        type_: types::Set,
        disallowed: types::Set,
        schema: Url,
    },
    #[error("location {ptr} is unknown in schema {schema}")]
    NoSuchPointer { ptr: String, schema: Url },
    #[error("transform {lhs_name} shuffled key types {lhs_types:?} don't align with transform {rhs_name} types {rhs_types:?}")]
    ShuffleKeyMismatch {
        lhs_name: String,
        lhs_types: Vec<types::Set>,
        rhs_name: String,
        rhs_types: Vec<types::Set>,
    },
    #[error("{category} projection {field} does not exist in collection {collection}")]
    NoSuchProjection {
        category: String,
        field: String,
        collection: String,
    },
    #[error("{category} projection {field} of collection {collection} is not a partition")]
    ProjectionNotPartitioned {
        category: String,
        field: String,
        collection: String,
    },
    #[error("projection {field} is the canonical field name of location {canonical_ptr}, and cannot re-map it to {wrong_ptr}")]
    ProjectionRemapsCanonicalField {
        field: String,
        canonical_ptr: String,
        wrong_ptr: String,
    },
    #[error("{category} partition selector field {field} value {value} is incompatible with the projections type, {type_:?}")]
    SelectorTypeMismatch {
        category: String,
        field: String,
        value: String,
        type_: types::Set,
    },
    #[error("{category} partition selector field {field} cannot be an empty string")]
    SelectorEmptyString { category: String, field: String },
    #[error(
        "source schema {schema} is already the schema of {collection} and should be omitted here"
    )]
    SourceSchemaNotDifferent { schema: Url, collection: String },
    #[error("transform {transform} shuffle key is already the key of {collection} and should be omitted here")]
    ShuffleKeyNotDifferent {
        transform: String,
        collection: String,
    },
    #[error("transform {transform} shuffle key cannot be empty")]
    ShuffleKeyEmpty { transform: String },
    #[error("{type_:?} is not a supported endpoint type for a collection store")]
    StoreEndpointType { type_: source::EndpointType },
    #[error("{type_:?} is not a supported endpoint type for a capture")]
    CaptureEndpointType { type_: source::EndpointType },
    #[error("{type_:?} is not a supported endpoint type for a materialization")]
    MaterializationEndpointType { type_: source::EndpointType },
    #[error("must set at least one of 'update' or 'publish' lambdas")]
    NoUpdateOrPublish { transform: String },
    #[error("capture {capture} cannot capture into derived collection {derivation}")]
    CaptureOfDerivation { capture: String, derivation: String },
    #[error("captures {lhs_name} and {rhs_name} (at {rhs_scope}) both pull from endpoint {endpoint} into collection {target}")]
    CaptureMultiplePulls {
        lhs_name: String,
        rhs_name: String,
        rhs_scope: Url,
        endpoint: String,
        target: String,
    },
    #[error("driver error while validating materialization {name}")]
    MaterializationDriver {
        name: String,
        #[source]
        detail: BoxError,
    },
    #[error("materialization {materialization} field {field} is not satisfiable ({reason})")]
    FieldUnsatisfiable {
        materialization: String,
        field: String,
        reason: String,
    },
    #[error(
        "materialization {materialization} has no acceptable field that satisfies required location {location}"
    )]
    LocationUnsatisfiable {
        materialization: String,
        location: String,
    },
    #[error("documents to verify are not in collection key order")]
    TestVerifyOrder,

    #[error("derivation's initial register is invalid against its schema: {}", serde_json::to_string_pretty(.0).unwrap())]
    RegisterInitialInvalid(doc::FailedValidation),
    #[error("test ingest document is invalid against the collection schema: {}", serde_json::to_string_pretty(.0).unwrap())]
    IngestDocInvalid(doc::FailedValidation),
    #[error("failed to parse merged bucket configuration")]
    ParseBucketConfig(#[source] serde_json::Error),

    #[error(transparent)]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error(transparent)]
    SchemaShape(#[from] doc::inference::Error),
}

pub struct SchemaShape {
    // Schema URL, including fragment pointer.
    pub schema: Url,
    // Inferred schema shape.
    pub shape: inference::Shape,
    // Canonical field names and corresponding locations, sorted on field.
    // This combines implicit, discovered locations with explicit projected locations.
    pub fields: Vec<(String, source::JsonPointer)>,
}

pub struct SchemaRef<'a> {
    // Scope referencing the schema.
    pub scope: &'a Url,
    // Schema which is referenced, including fragment pointer.
    pub schema: &'a Url,
    // Collection having this schema, for which this SchemaRef was created.
    // None if this reference is not a collection schema.
    pub collection: Option<&'a source::CollectionName>,
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub trait Drivers {
    fn validate_materialization(
        &self,
        endpoint_type: source::EndpointType,
        endpoint_config: serde_json::Value,
        request: materialize::ValidateRequest,
    ) -> LocalBoxFuture<Result<materialize::ValidateResponse, BoxError>>;
}

pub fn validate<D: Drivers>(drivers: &D, catalog: &source::Tables) -> Tables {
    let source::Tables {
        captures,
        collections,
        derivations,
        endpoints,
        errors: _,
        fetches: _,
        imports,
        materializations,
        nodejs_dependencies: _, // TODO verify there aren't conflicts.
        projections,
        resources,
        schema_docs,
        test_steps,
        transforms,
    } = catalog;

    let mut errors = tables::Errors::new();

    // We binary-search while exploring the import graph.
    let imports = imports
        .iter()
        .sorted_by_key(|i| (&i.from_resource, &i.to_resource))
        .collect::<Vec<_>>();

    let compiled_schemas = schema_docs
        .iter()
        .map(|s| build_schema(s.schema.clone(), &s.dom).unwrap())
        .collect::<Vec<CompiledSchema>>();
    let schema_index =
        index_compiled_schemas(&compiled_schemas, &resources[0].resource, &mut errors);

    let schema_refs = gather_schema_refs(collections, derivations, transforms);
    let (schema_shapes, inferences) =
        walk_all_schema_refs(&schema_index, projections, &schema_refs, &mut errors);

    let (built_collections, implicit_projections) = walk_all_collections(
        collections,
        endpoints,
        &imports,
        projections,
        &schema_shapes,
        &mut errors,
    );

    walk_all_derivations(
        collections,
        derivations,
        &imports,
        &schema_index,
        projections,
        &schema_shapes,
        transforms,
        &mut errors,
    );

    walk_all_endpoints(endpoints, &mut errors);

    walk_all_captures(
        captures,
        collections,
        derivations,
        endpoints,
        &imports,
        &mut errors,
    );

    let built_materializations = walk_all_materializations(
        drivers,
        &built_collections,
        collections,
        endpoints,
        &imports,
        materializations,
        &mut errors,
    );

    walk_all_test_steps(
        collections,
        &imports,
        projections,
        &schema_index,
        &schema_shapes,
        test_steps,
        &mut errors,
    );

    Tables {
        built_collections,
        built_materializations,
        errors,
        implicit_projections,
        inferences,
    }
}

fn gather_schema_refs<'a>(
    collections: &'a [tables::Collection],
    derivations: &'a [tables::Derivation],
    transforms: &'a [tables::Transform],
) -> Vec<SchemaRef<'a>> {
    collections
        .iter()
        .map(|c| SchemaRef {
            scope: &c.scope,
            schema: &c.schema,
            collection: Some(&c.collection),
        })
        .chain(derivations.iter().map(|d| SchemaRef {
            scope: &d.scope,
            schema: &d.register_schema,
            collection: None,
        }))
        .chain(transforms.iter().filter_map(|t| {
            t.source_schema.as_ref().map(|schema| SchemaRef {
                scope: &t.scope,
                schema,
                collection: None,
            })
        }))
        .collect()
}

fn index_compiled_schemas<'a>(
    compiled: &'a [CompiledSchema],
    root_scope: &Url,
    errors: &mut tables::Errors,
) -> doc::SchemaIndex<'a> {
    let mut index = doc::SchemaIndex::new();

    for compiled in compiled {
        if let Err(err) = index.add(compiled) {
            errors.push_validation(&compiled.curi, err.into());
        }
    }

    // TODO(johnny): report multiple errors and visit each,
    // rather than stopping at the first.
    if let Err(err) = index.verify_references() {
        errors.push_validation(root_scope, err.into());
    }

    index
}

fn walk_all_schema_refs(
    index: &doc::SchemaIndex<'_>,
    projections: &[tables::Projection],
    schema_refs: &[SchemaRef<'_>],
    errors: &mut tables::Errors,
) -> (Vec<SchemaShape>, tables::Inferences) {
    let mut schema_shapes: Vec<SchemaShape> = Vec::new();
    let mut inferences = tables::Inferences::new();

    // Walk schema URLs (*with* fragment pointers) with their grouped references.
    for (schema, references) in schema_refs
        .iter()
        .sorted_by_key(|r| r.schema)
        .group_by(|r| r.schema)
        .into_iter()
    {
        // Infer the schema shape, and report any inspected errors.
        let shape = match index.fetch(schema) {
            Some(s) => inference::Shape::infer(s, &index),
            None => {
                for reference in references {
                    errors.push_validation(
                        reference.scope,
                        Error::NoSuchSchema {
                            schema: schema.clone(),
                        },
                    );
                }

                schema_shapes.push(SchemaShape {
                    schema: schema.clone(),
                    shape: Default::default(),
                    fields: Default::default(),
                });
                continue;
            }
        };
        for err in shape.inspect() {
            errors.push_validation(schema, err.into());
        }

        // Map references through collections having the schema as source,
        // and from there to collection projections. Walk projections to
        // identify all unique named location pointers of the schema.
        // These locations may include entries which aren't statically
        // know-able, e.x. due to additionalProperties or patternProperties.
        let explicit: Vec<&str> = references
            .filter_map(|r| r.collection)
            .map(|collection| {
                projections.iter().filter_map(move |p| {
                    if *collection == p.collection {
                        Some(p.location.as_ref())
                    } else {
                        None
                    }
                })
            })
            .flatten()
            .sorted()
            .dedup()
            .collect::<Vec<_>>();

        // Now identify all implicit, statically known-able schema locations.
        // These may overlap with explicit.
        let implicit = shape
            .locations()
            .into_iter()
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .collect::<Vec<_>>();

        // Merge explicit & implicit into a unified sequence of schema locations.
        let merged = explicit
            .into_iter()
            .merge_join_by(implicit.into_iter(), |lhs, rhs| (*lhs).cmp(rhs.0.as_ref()))
            .filter_map(|eob| match eob {
                EitherOrBoth::Left(ptr) => shape
                    .locate(&doc::Pointer::from_str(ptr))
                    // We'll skip entries from projections that can't be located in the shape.
                    // These will generate a proper error later.
                    .map(|(shape, must_exist)| (ptr.to_string(), shape, must_exist)),
                EitherOrBoth::Both(_, (ptr, shape, must_exist))
                | EitherOrBoth::Right((ptr, shape, must_exist)) => Some((ptr, shape, must_exist)),
            })
            // Generate a canonical projection field for each location.
            .map(|(ptr, shape, must_exist)| {
                let field = if ptr.is_empty() {
                    "flow_document".to_string()
                } else {
                    // Canonical projection field is the JSON pointer
                    // stripped of its leading '/'.
                    ptr.chars().skip(1).collect::<String>()
                };

                (field, source::JsonPointer::new(ptr), shape, must_exist)
            })
            // Re-order to walk in ascending field name order.
            .sorted_by(|a, b| a.0.cmp(&b.0));

        // Now collect |fields| in ascending field order,
        // and record all inferences.
        let mut fields = Vec::with_capacity(merged.len());

        for (field, ptr, shape, must_exist) in merged {
            inferences.push_row(schema, &ptr, shape_to_inference(shape, must_exist));
            fields.push((field, ptr)); // Note we're already ordered on |field|.
        }

        schema_shapes.push(SchemaShape {
            schema: schema.clone(),
            shape,
            fields,
        });
    }

    (schema_shapes, inferences)
}

fn shape_to_inference(shape: &inference::Shape, must_exist: bool) -> protocol::flow::Inference {
    protocol::flow::Inference {
        types: shape.type_.to_vec(),
        must_exist,
        title: shape.title.clone().unwrap_or_default(),
        description: shape.description.clone().unwrap_or_default(),
        string: if shape.type_.overlaps(types::STRING) {
            Some(flow::inference::String {
                content_type: shape.string.content_type.clone().unwrap_or_default(),
                format: shape.string.format.clone().unwrap_or_default(),
                is_base64: shape.string.is_base64.unwrap_or_default(),
                max_length: shape.string.max_length.unwrap_or_default() as u32,
            })
        } else {
            None
        },
    }
}

fn walk_all_collections(
    collections: &[tables::Collection],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    projections: &[tables::Projection],
    schema_shapes: &[SchemaShape],
    errors: &mut tables::Errors,
) -> (tables::BuiltCollections, tables::Projections) {
    let mut implicit_projections = tables::Projections::new();
    let mut built_collections = tables::BuiltCollections::new();

    for collection in collections {
        let projections = projections
            .iter()
            .filter(|p| p.collection == collection.collection)
            .collect::<Vec<_>>();

        built_collections.push_row(
            &collection.scope,
            &collection.collection,
            walk_collection(
                collection,
                endpoints,
                imports,
                &projections,
                schema_shapes,
                errors,
                &mut implicit_projections,
            ),
        );
    }

    walk_duplicates(
        "collection",
        collections.iter().map(|c| (&c.collection, &c.scope)),
        errors,
    );

    (built_collections, implicit_projections)
}

fn walk_collection(
    collection: &tables::Collection,
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    projections: &[&tables::Projection],
    schema_shapes: &[SchemaShape],
    errors: &mut tables::Errors,
    implicit_projections: &mut tables::Projections,
) -> flow::CollectionSpec {
    let tables::Collection {
        collection: name,
        scope,
        schema,
        key,
        store_endpoint: store,
        store_patch_config,
    } = collection;

    walk_name(scope, "collection", name.as_ref(), &COLLECTION_RE, errors);

    let schema = schema_shapes.iter().find(|s| s.schema == *schema).unwrap();
    let _ = walk_composite_key(scope, key, schema, errors);

    // Dereference the collection's endpoint.
    let endpoint = walk_reference(
        scope,
        "collection",
        name,
        "endpoint",
        store,
        endpoints,
        |e| (&e.endpoint, &e.scope),
        imports,
        errors,
    );
    let fragment_store = walk_collection_store(scope, endpoint, store_patch_config, errors);

    let projections = walk_collection_projections(
        collection,
        projections,
        schema,
        errors,
        implicit_projections,
    );

    let partition_fields = projections
        .iter()
        .filter_map(|p| {
            if p.is_partition_key {
                Some(p.field.clone())
            } else {
                None
            }
        })
        .collect();

    let journal_spec = walk_collection_journal_spec(scope, collection, fragment_store, errors);

    flow::CollectionSpec {
        name: name.to_string(),
        schema_uri: schema.schema.to_string(),
        key_ptrs: key.iter().map(|p| p.to_string()).collect(),
        journal_spec: Some(journal_spec),
        projections,
        partition_fields,

        uuid_ptr: "/_meta/uuid".to_string(),
        ack_json_template: serde_json::json!({
                "_meta": {"uuid": "DocUUIDPlaceholder-329Bb50aa48EAa9ef",
                "ack": true,
            } })
        .to_string()
        .into(),
    }
}

fn walk_collection_store(
    scope: &Url,
    endpoint: Option<&tables::Endpoint>,
    store_patch_config: &serde_json::Value,
    errors: &mut tables::Errors,
) -> Option<String> {
    match endpoint {
        Some(tables::Endpoint {
            endpoint_type,
            base_config,
            ..
        }) if matches!(
            endpoint_type,
            source::EndpointType::S3 | source::EndpointType::GS
        ) =>
        {
            let mut cfg = base_config.clone();
            json_patch::merge(&mut cfg, store_patch_config);

            match serde_json::from_value::<source::BucketConfig>(cfg) {
                Ok(cfg) => Some(format!(
                    "{}://{}/{}",
                    endpoint_type.as_scheme(),
                    cfg.bucket,
                    cfg.prefix
                )),
                Err(err) => {
                    errors.push_validation(scope, Error::ParseBucketConfig(err));
                    None
                }
            }
        }
        Some(tables::Endpoint { endpoint_type, .. }) => {
            errors.push_validation(
                scope,
                Error::StoreEndpointType {
                    type_: *endpoint_type,
                },
            );
            None
        }
        None => None,
    }
}

fn walk_collection_journal_spec(
    _scope: &Url,
    _collection: &tables::Collection,
    fragment_store: Option<String>,
    _errors: &mut tables::Errors,
) -> JournalSpec {
    // TODO: We'll need more principled & source-driven mechanisms for these.
    // For now, we hard-code!
    let journal_spec = JournalSpec {
        replication: 3,
        fragment: Some(FragmentSpec {
            length: 1 << 29, // 512MB.
            stores: fragment_store.into_iter().collect(),
            compression_codec: protocol::protocol::CompressionCodec::GzipOffloadDecompression
                as i32,
            refresh_interval: Some(std::time::Duration::from_secs(5 * 60).into()),
            path_postfix_template: r#"utc_date={{.Spool.FirstAppendTime.Format "2006-01-02"}}/utc_hour={{.Spool.FirstAppendTime.Format "15"}}"#.to_string(),
            flush_interval: Some(std::time::Duration::from_secs(60 * 60).into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    journal_spec
}

fn walk_collection_projections(
    collection: &tables::Collection,
    projections: &[&tables::Projection],
    schema_shape: &SchemaShape,
    errors: &mut tables::Errors,
    implicit_projections: &mut tables::Projections,
) -> Vec<flow::Projection> {
    // Require that projection fields have no duplicates under our collation.
    // This restricts *manually* specified projections, but not canonical ones.
    // Most importantly, this ensures there are no collation-duplicated partitions.
    walk_duplicates(
        "projection",
        projections.iter().map(|p| (&p.field, &p.scope)),
        errors,
    );

    let mut specs = Vec::new();
    for eob in projections
        .iter()
        .sorted_by_key(|p| &p.field)
        .merge_join_by(schema_shape.fields.iter(), |projection, (field, _)| {
            projection.field.cmp(field)
        })
    {
        let (spec, implicit) =
            walk_projection_with_inference(collection, eob, schema_shape, errors);

        if let Some(spec) = spec {
            specs.push(spec);
        }
        if let Some(implicit) = implicit {
            implicit_projections.push(implicit);
        }
    }

    specs
}

fn walk_projection_with_inference(
    collection: &tables::Collection,
    eob: EitherOrBoth<&&tables::Projection, &(String, source::JsonPointer)>,
    schema_shape: &SchemaShape,
    errors: &mut tables::Errors,
) -> (Option<flow::Projection>, Option<tables::Projection>) {
    let (scope, field, location, projection) = match eob {
        EitherOrBoth::Both(projection, (field, location)) => {
            if projection.location != *location {
                errors.push_validation(
                    &projection.scope,
                    Error::ProjectionRemapsCanonicalField {
                        field: field.clone(),
                        canonical_ptr: location.to_string(),
                        wrong_ptr: projection.location.to_string(),
                    },
                );
            }
            (
                &projection.scope,
                &projection.field,
                &projection.location,
                Some(projection),
            )
        }
        EitherOrBoth::Left(projection) => (
            &projection.scope,
            &projection.field,
            &projection.location,
            Some(projection),
        ),
        EitherOrBoth::Right((field, location)) => (&collection.scope, field, location, None),
    };

    let (shape, must_exist) = match schema_shape.shape.locate(&doc::Pointer::from_str(location)) {
        Some(t) => t,
        None => {
            errors.push_validation(
                scope,
                Error::NoSuchPointer {
                    ptr: location.to_string(),
                    schema: schema_shape.schema.clone(),
                },
            );
            return (None, None);
        }
    };

    let mut spec = flow::Projection {
        ptr: location.to_string(),
        field: field.clone(),
        user_provided: false,
        is_primary_key: collection.key.iter().any(|k| k == location),
        is_partition_key: false,
        inference: Some(shape_to_inference(shape, must_exist)),
    };

    if let Some(projection) = projection {
        if projection.partition {
            walk_name(scope, "partition", field, &PARTITION_RE, errors);
            walk_keyed_location(
                &projection.scope,
                &schema_shape.schema,
                location,
                shape,
                must_exist,
                errors,
            );
        }

        spec.user_provided = true;
        spec.is_partition_key = projection.partition;

        (Some(spec), None)
    } else {
        // This is a discovered projection not provided by the user.
        let implicit = tables::Projection {
            scope: collection.scope.clone(),
            collection: collection.collection.clone(),
            field: field.clone(),
            location: location.clone(),
            partition: false,
            user_provided: false,
        };
        (Some(spec), Some(implicit))
    }
}

fn walk_all_endpoints(endpoints: &[tables::Endpoint], errors: &mut tables::Errors) {
    for tables::Endpoint {
        scope,
        endpoint: name,
        endpoint_type: _,
        base_config: _,
    } in endpoints
    {
        walk_name(scope, "endpoint", name, &ENDPOINT_RE, errors);
    }

    walk_duplicates(
        "endpoint",
        endpoints.iter().map(|ep| (&ep.endpoint, &ep.scope)),
        errors,
    );
}

fn walk_all_materializations<D: Drivers>(
    drivers: &D,
    built_collections: &[tables::BuiltCollection],
    collections: &[tables::Collection],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    materializations: &[tables::Materialization],
    errors: &mut tables::Errors,
) -> tables::BuiltMaterializations {
    let mut validations = Vec::new();

    for materialization in materializations {
        validations.extend(
            walk_materialization_request(
                built_collections,
                collections,
                endpoints,
                imports,
                materialization,
                errors,
            )
            .into_iter(),
        );
    }

    walk_duplicates(
        "materialization",
        materializations
            .iter()
            .map(|m| (&m.materialization, &m.scope)),
        errors,
    );

    // Run all validations concurrently.
    let validations = validations.into_iter().map(
        |(endpoint_type, endpoint_config, request, built_collection, materialization)| async move {
            drivers
                .validate_materialization(endpoint_type, endpoint_config.clone(), request)
                // Pass-through the materialization & CollectionSpec for future verification.
                .map(|response| (built_collection, materialization, endpoint_config, response))
                .await
        },
    );
    let validations = futures::executor::block_on(futures::future::join_all(validations));

    let mut built_materializations = tables::BuiltMaterializations::new();

    for (built_collection, materialization, endpoint_config, response) in validations {
        match response {
            Ok(response) => {
                let fields = walk_materialization_response(
                    built_collection,
                    materialization,
                    response,
                    errors,
                );

                built_materializations.push_row(
                    &materialization.scope,
                    &materialization.materialization,
                    &materialization.collection,
                    endpoint_config,
                    fields,
                );
            }
            Err(err) => {
                errors.push_validation(
                    &materialization.scope,
                    Error::MaterializationDriver {
                        name: materialization.materialization.to_string(),
                        detail: err,
                    },
                );
            }
        }
    }

    built_materializations
}

fn walk_materialization_request<'a>(
    built_collections: &'a [tables::BuiltCollection],
    collections: &[tables::Collection],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    materialization: &'a tables::Materialization,
    errors: &mut tables::Errors,
) -> Option<(
    source::EndpointType,
    serde_json::Value,
    materialize::ValidateRequest,
    &'a tables::BuiltCollection,
    &'a tables::Materialization,
)> {
    let tables::Materialization {
        scope,
        materialization: name,
        collection: source,
        endpoint,
        patch_config,
        field_selector: fields,
    } = materialization;

    walk_name(
        scope,
        "materialization",
        name.as_ref(),
        &MATERIALIZATION_RE,
        errors,
    );

    let source = walk_reference(
        scope,
        "materialization",
        name,
        "collection",
        source,
        collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    );

    let endpoint = walk_reference(
        scope,
        "materialization",
        name,
        "endpoint",
        endpoint,
        endpoints,
        |e| (&e.endpoint, &e.scope),
        imports,
        errors,
    );

    // We must resolve both |source| and |endpoint| to continue.
    let (source, endpoint) = match (source, endpoint) {
        (Some(s), Some(e)) => (s, e),
        _ => return None,
    };

    let built_collection = built_collections
        .iter()
        .find(|c| c.collection == source.collection)
        .unwrap();

    let mut endpoint_config = endpoint.base_config.clone();
    json_patch::merge(&mut endpoint_config, &patch_config);

    let field_config = walk_materialization_fields(scope, built_collection, fields, errors);

    let request = materialize::ValidateRequest {
        handle: Vec::new(),
        collection: Some(built_collection.spec.clone()),
        field_config: field_config.into_iter().collect(),
    };

    Some((
        endpoint.endpoint_type,
        endpoint_config,
        request,
        built_collection,
        materialization,
    ))
}

fn walk_materialization_fields<'a>(
    scope: &Url,
    built_collection: &tables::BuiltCollection,
    fields: &source::MaterializationFields,
    errors: &mut tables::Errors,
) -> Vec<(String, String)> {
    let source::MaterializationFields {
        include,
        exclude,
        recommended: _,
    } = fields;

    let flow::CollectionSpec {
        name: collection,
        projections,
        ..
    } = &built_collection.spec;

    let mut bag = Vec::new();

    for (field, config) in include {
        if projections.iter().any(|p| p.field == *field) {
            bag.push((field.clone(), serde_json::to_string(config).unwrap()));
        } else {
            errors.push_validation(
                scope,
                Error::NoSuchProjection {
                    category: "include".to_string(),
                    field: field.clone(),
                    collection: collection.clone(),
                },
            );
        }
    }

    for field in exclude {
        if !projections.iter().any(|p| p.field == *field) {
            errors.push_validation(
                scope,
                Error::NoSuchProjection {
                    category: "exclude".to_string(),
                    field: field.clone(),
                    collection: collection.clone(),
                },
            );
        }
    }

    bag
}

fn walk_materialization_response(
    built_collection: &tables::BuiltCollection,
    materialization: &tables::Materialization,
    response: materialize::ValidateResponse,
    errors: &mut tables::Errors,
) -> materialize::FieldSelection {
    let tables::Materialization {
        scope,
        materialization: name,
        field_selector:
            source::MaterializationFields {
                include,
                exclude,
                recommended,
            },
        ..
    } = materialization;

    let flow::CollectionSpec {
        projections,
        key_ptrs,
        ..
    } = &built_collection.spec;

    let materialize::ValidateResponse { mut constraints } = response;

    // |keys| and |document| are initialized with placeholder None,
    // that we'll revisit as we walk projections & constraints.
    let mut keys = key_ptrs
        .iter()
        .map(|_| Option::<String>::None)
        .collect::<Vec<_>>();
    let mut document = String::new();
    // Projections *not* key parts or the root document spill to |values|.
    let mut values = Vec::new();
    // Required locations (as JSON pointers), and an indication of whether each has been found.
    let mut locations: HashMap<String, bool> = HashMap::new();
    // Encoded field configuration, passed through from |include| to the driver.
    let mut field_config = HashMap::new();

    use materialize::constraint::Type;

    // Sort projections so that we walk, in order:
    // * Fields which *must* be included.
    // * Fields which are user-defined, and should be selected preferentially
    //   for locations where we need only one field.
    // * Everything else.
    let projections = projections
        .iter()
        .sorted_by_key(|p| {
            let must_include = include.get(&p.field).is_some()
                || constraints
                    .get(&p.field)
                    .map(|c| c.r#type == Type::FieldRequired as i32)
                    .unwrap_or_default();

            (!must_include, !p.user_provided) // Negate to order before.
        })
        .collect::<Vec<_>>();

    for projection in projections {
        let flow::Projection { ptr, field, .. } = projection;

        let constraint = constraints
            .remove(field)
            .unwrap_or(materialize::Constraint {
                r#type: Type::FieldForbidden as i32,
                reason: String::new(),
            });

        let type_ = match Type::from_i32(constraint.r#type) {
            Some(t) => t,
            None => {
                errors.push_validation(
                    scope,
                    Error::MaterializationDriver {
                        name: name.to_string(),
                        detail: format!("unknown constraint type {}", constraint.r#type).into(),
                    },
                );
                Type::FieldForbidden
            }
        };
        let reason = constraint.reason.as_str();

        if matches!(type_, Type::LocationRequired) {
            // Mark that this location must be selected.
            locations.entry(ptr.clone()).or_insert(false);
        }

        // Has this pointer been selected already, via another projection?
        let is_selected_ptr = locations.get(ptr).cloned().unwrap_or_default();
        // What's the index of this pointer in the composite key (if any)?
        let key_index = key_ptrs.iter().enumerate().find(|(_, k)| *k == ptr);

        let resolution = match (
            include.get(field).is_some(),
            exclude.iter().any(|f| f == field),
            type_,
        ) {
            // Selector / driver constraints conflict internally:
            (true, true, _) => Err(format!("field is both included and excluded by selector")),
            (_, _, Type::Unsatisfiable) => Err(format!(
                "driver reports as unsatisfiable with reason: {}",
                reason
            )),
            // Selector / driver constraints conflict with each other:
            (true, false, Type::FieldForbidden) => Err(format!(
                "selector includes field, but driver forbids it with reason reason: {}",
                reason
            )),
            (false, true, Type::FieldRequired) => Err(format!(
                "selector excludes field, but driver requires it with reason: {}",
                reason
            )),

            // Field is required by selector or driver.
            (true, false, _) | (false, false, Type::FieldRequired) => Ok(true),
            // Field is forbidden by selector or driver.
            (false, true, _) | (false, false, Type::FieldForbidden) => Ok(false),
            // Location is required and is not yet selected.
            (false, false, Type::LocationRequired) if !is_selected_ptr => Ok(true),
            // We desire recommended fields, and this location is unseen & recommended.
            // (Note we'll visit a user-provided projection of the location before an inferred one).
            (false, false, Type::LocationRecommended) if !is_selected_ptr && *recommended => {
                Ok(true)
            }

            // Cases where we don't include the field.
            (false, false, Type::FieldOptional) => Ok(false),
            (false, false, Type::LocationRequired) => {
                assert!(is_selected_ptr);
                Ok(false)
            }
            (false, false, Type::LocationRecommended) => {
                assert!(is_selected_ptr || !*recommended);
                Ok(false)
            }
        };

        match resolution {
            Err(reason) => {
                errors.push_validation(
                    scope,
                    Error::FieldUnsatisfiable {
                        materialization: name.to_string(),
                        field: field.to_string(),
                        reason,
                    },
                );
            }
            Ok(false) => { /* No action. */ }
            Ok(true) => {
                let key_slot = key_index.and_then(|(i, _)| keys.get_mut(i));

                // Add to one of |keys|, |document| or |values|.
                if let Some(slot @ None) = key_slot {
                    *slot = Some(field.clone());
                } else if ptr == "" && document == "" {
                    document = field.clone();
                } else {
                    values.push(field.clone());
                }

                // Pass-through JSON-encoded field configuration.
                if let Some(cfg) = include.get(field) {
                    field_config.insert(field.clone(), serde_json::to_string(cfg).unwrap());
                }
                // Mark location as having been selected.
                locations.insert(ptr.clone(), true);
            }
        }
    }

    // Any left-over constraints were unexpectedly not in |projections|.
    for (field, _) in constraints {
        errors.push_validation(
            scope,
            Error::MaterializationDriver {
                name: name.to_string(),
                detail: format!("driver sent constraint for unknown field {}", field).into(),
            },
        );
    }
    // Any required but unmatched locations are an error.
    for (location, found) in locations {
        if !found {
            errors.push_validation(
                scope,
                Error::LocationUnsatisfiable {
                    materialization: name.to_string(),
                    location,
                },
            );
        }
    }

    materialize::FieldSelection {
        keys: keys.into_iter().filter_map(|k| k).collect(),
        values,
        document,
        field_config,
    }
}

fn walk_all_captures(
    captures: &[tables::Capture],
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    errors: &mut tables::Errors,
) {
    for capture in captures {
        walk_capture(
            capture,
            collections,
            derivations,
            endpoints,
            imports,
            errors,
        );
    }
    walk_duplicates(
        "capture",
        captures.iter().map(|c| (&c.capture, &c.scope)),
        errors,
    );

    // Require that tuples of (target, endpoint) are globally unique.
    // TODO: This is a bit wrong -- we need the endpoint to *give* us
    // an ID to de-duplicate on, such as a table name.
    // That requires that we first implement a capture driver concept,
    // which can generate these IDs. Leaving this behavior in for now,
    // until we do this.
    for ((l_tgt, l_ep, l_name, l_scope), (r_tgt, r_ep, r_name, r_scope)) in captures
        .iter()
        .filter_map(|c| {
            c.endpoint
                .as_ref()
                .map(|endpoint| (&c.collection, endpoint, &c.capture, &c.scope))
        })
        .sorted()
        .tuple_windows()
    {
        if (l_tgt, l_ep) == (r_tgt, r_ep) {
            errors.push_validation(
                l_scope,
                Error::CaptureMultiplePulls {
                    lhs_name: l_name.to_string(),
                    rhs_name: r_name.to_string(),
                    rhs_scope: r_scope.clone(),
                    endpoint: l_ep.to_string(),
                    target: l_tgt.to_string(),
                },
            );
        }
    }
}

fn walk_capture(
    capture: &tables::Capture,
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    errors: &mut tables::Errors,
) {
    let tables::Capture {
        scope,
        capture: name,
        collection: target,
        endpoint,
        allow_push: _,
        patch_config: _,
    } = capture;

    walk_name(scope, "capture", name.as_ref(), &CAPTURE_RE, errors);

    // Ensure we can dereference the capture's target.
    let _ = walk_reference(
        scope,
        "capture",
        name.as_ref(),
        "collection",
        target,
        collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    );

    // But it must not be a derivation.
    if let Some(_) = derivations.iter().find(|d| d.derivation == *target) {
        errors.push_validation(
            scope,
            Error::CaptureOfDerivation {
                capture: name.to_string(),
                derivation: target.to_string(),
            },
        );
    }

    if let Some(endpoint) = endpoint {
        // Dereference the captures's endpoint.
        if let Some(endpoint) = walk_reference(
            scope,
            "capture",
            name.as_ref(),
            "endpoint",
            endpoint,
            endpoints,
            |e| (&e.endpoint, &e.scope),
            imports,
            errors,
        ) {
            // Ensure it's of a compatible endpoint type.
            if !matches!(endpoint.endpoint_type, source::EndpointType::S3) {
                errors.push_validation(
                    scope,
                    Error::CaptureEndpointType {
                        type_: endpoint.endpoint_type,
                    },
                );
            }
        }
    }
}

fn walk_all_derivations(
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    imports: &[&tables::Import],
    index: &doc::SchemaIndex<'_>,
    projections: &[tables::Projection],
    schema_shapes: &[SchemaShape],
    transforms: &[tables::Transform],
    errors: &mut tables::Errors,
) {
    for derivation in derivations {
        let transforms = transforms
            .iter()
            .filter(|t| t.derivation == derivation.derivation)
            .collect::<Vec<_>>();

        walk_derivation(
            collections,
            derivation,
            imports,
            index,
            projections,
            schema_shapes,
            &transforms,
            errors,
        );
    }
}

fn walk_derivation(
    collections: &[tables::Collection],
    derivation: &tables::Derivation,
    imports: &[&tables::Import],
    index: &doc::SchemaIndex<'_>,
    projections: &[tables::Projection],
    schema_shapes: &[SchemaShape],
    transforms: &[&tables::Transform],
    errors: &mut tables::Errors,
) {
    let tables::Derivation {
        scope,
        register_schema,
        register_initial,
        ..
    } = derivation;

    // Verify that the register's initial value conforms to its schema.
    if let Err(err) = doc::validate(
        &mut doc::Validator::<doc::FullContext>::new(index),
        register_schema,
        register_initial,
    ) {
        errors.push_validation(scope, Error::RegisterInitialInvalid(err));
    }

    // We'll collect types of each transform's shuffle key.
    let mut shuffle_types: Vec<(Vec<types::Set>, &tables::Transform)> = Vec::new();

    // Walk transforms of this derivation.
    for transform in transforms {
        if let Some(s) = walk_transform(
            collections,
            imports,
            projections,
            schema_shapes,
            transform,
            errors,
        ) {
            shuffle_types.push((s, transform));
        }
    }

    walk_duplicates(
        "transform",
        transforms.iter().map(|t| (&t.transform, &t.scope)),
        errors,
    );

    // Verify that shuffle key types & lengths align.
    for ((l_types, l_transform), (r_types, r_transform)) in shuffle_types.iter().tuple_windows() {
        if l_types != r_types {
            errors.push_validation(
                &l_transform.scope,
                Error::ShuffleKeyMismatch {
                    lhs_name: l_transform.transform.to_string(),
                    lhs_types: l_types.clone(),
                    rhs_name: r_transform.transform.to_string(),
                    rhs_types: r_types.clone(),
                },
            );
        }
    }
}

pub fn walk_transform(
    collections: &[tables::Collection],
    imports: &[&tables::Import],
    projections: &[tables::Projection],
    schema_shapes: &[SchemaShape],
    transform: &tables::Transform,
    errors: &mut tables::Errors,
) -> Option<Vec<types::Set>> {
    let tables::Transform {
        scope,
        derivation: _,
        transform: name,
        source_collection: source,
        source_partitions,
        source_schema,
        shuffle_key,
        shuffle_lambda,
        shuffle_hash: _,
        read_delay_seconds: _,
        priority: _,
        publish_lambda: publish,
        update_lambda: update,
    } = transform;

    walk_name(scope, "transform", name.as_ref(), &TRANSFORM_RE, errors);

    if update.is_none() && publish.is_none() {
        errors.push_validation(
            scope,
            Error::NoUpdateOrPublish {
                transform: name.to_string(),
            },
        );
    }

    // Dereference the transform's source. We can't continue without it.
    let source = match walk_reference(
        scope,
        "transform",
        name,
        "collection",
        source,
        collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    ) {
        Some(s) => s,
        None => return None,
    };

    if let Some(selector) = source_partitions {
        // Note that the selector is deliberately checked against the
        // collection's schema shape, and not our own transform source schema.
        let source_shape = schema_shapes
            .iter()
            .find(|s| s.schema == source.schema)
            .unwrap();

        let source_projections = projections
            .iter()
            .filter(|p| p.collection == source.collection)
            .collect::<Vec<_>>();

        walk_selector(
            scope,
            &source.collection,
            &source_projections,
            source_shape,
            &selector,
            errors,
        );
    }

    // Map to an effective source schema & shape.
    let source_schema = match source_schema {
        Some(url) => {
            if url == &source.schema {
                errors.push_validation(
                    scope,
                    Error::SourceSchemaNotDifferent {
                        schema: url.clone(),
                        collection: source.collection.to_string(),
                    },
                );
            }
            url
        }
        None => &source.schema,
    };

    let shuffle_types = if shuffle_lambda.is_none() {
        // Map to an effective shuffle key.
        let shuffle_key = match shuffle_key {
            Some(key) => {
                if key == &source.key {
                    errors.push_validation(
                        scope,
                        Error::ShuffleKeyNotDifferent {
                            transform: name.to_string(),
                            collection: source.collection.to_string(),
                        },
                    );
                }
                if key.iter().next().is_none() {
                    errors.push_validation(
                        scope,
                        Error::ShuffleKeyEmpty {
                            transform: name.to_string(),
                        },
                    );
                }
                key
            }
            None => &source.key,
        };
        // Walk and collect key value types, so we can compare
        // with other transforms of this derivation later.
        let source_shape = schema_shapes
            .iter()
            .find(|s| s.schema == *source_schema)
            .unwrap();
        walk_composite_key(scope, shuffle_key, source_shape, errors)
    } else {
        None
    };

    shuffle_types
}

pub fn walk_all_test_steps(
    collections: &[tables::Collection],
    imports: &[&tables::Import],
    projections: &[tables::Projection],
    schema_index: &doc::SchemaIndex<'_>,
    schemas: &[SchemaShape],
    test_steps: &[tables::TestStep],
    errors: &mut tables::Errors,
) {
    for test_step in test_steps {
        walk_test_step(
            collections,
            imports,
            projections,
            schema_index,
            schemas,
            test_step,
            errors,
        );
    }

    walk_duplicates(
        "test",
        test_steps
            .iter()
            .filter(|s| s.step_index == 0)
            .map(|s| (&s.test, &s.scope)),
        errors,
    );
}

pub fn walk_test_step(
    collections: &[tables::Collection],
    imports: &[&tables::Import],
    projections: &[tables::Projection],
    schema_index: &doc::SchemaIndex<'_>,
    schema_shapes: &[SchemaShape],
    test_step: &tables::TestStep,
    errors: &mut tables::Errors,
) {
    let tables::TestStep {
        scope,
        test: name,
        step,
        step_index: _,
    } = test_step;

    let (collection, ingest, verify, partitions) = match step {
        source::TestStep::Verify(v) => (
            &v.collection,
            &[] as &[_],
            v.documents.as_slice(),
            v.partitions.as_ref(),
        ),
        source::TestStep::Ingest(i) => (&i.collection, i.documents.as_slice(), &[] as &[_], None),
    };

    // Dereference test collection, returning early if not found.
    let collection = match walk_reference(
        scope,
        "test step",
        name,
        "collection",
        collection,
        collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    ) {
        Some(s) => s,
        None => return,
    };

    // Verify that any ingest documents conform to the collection schema.
    let mut validator = doc::Validator::<doc::FullContext>::new(schema_index);
    for doc in ingest {
        if schema_index.fetch(&collection.schema).is_none() {
            // Referential integrity error, which we've already reported.
            continue;
        } else if let Err(err) = doc::validate(&mut validator, &collection.schema, doc) {
            errors.push_validation(scope, Error::IngestDocInvalid(err));
        }
    }

    // Verify that any verified documents are ordered correctly w.r.t.
    // the collection's key.
    if verify
        .iter()
        .tuple_windows()
        .map(|(lhs, rhs)| json::json_cmp_at(&collection.key, lhs, rhs))
        .any(|ord| ord == std::cmp::Ordering::Greater)
    {
        errors.push_validation(scope, Error::TestVerifyOrder);
    }

    if let Some(selector) = partitions {
        let schema_shape = schema_shapes
            .iter()
            .find(|s| s.schema == collection.schema)
            .unwrap();

        let projections = projections
            .iter()
            .filter(|p| p.collection == collection.collection)
            .collect::<Vec<_>>();

        walk_selector(
            scope,
            &collection.collection,
            &projections,
            schema_shape,
            selector,
            errors,
        );
    }
}

pub fn walk_selector(
    scope: &Url,
    collection: &source::CollectionName,
    projections: &[&tables::Projection],
    schema_shape: &SchemaShape,
    selector: &source::PartitionSelector,
    errors: &mut tables::Errors,
) {
    let source::PartitionSelector { include, exclude } = selector;

    for (category, labels) in &[("include", include), ("exclude", exclude)] {
        for (field, values) in labels.iter() {
            let partition = match projections.iter().find(|p| p.field == *field) {
                Some(projection) => {
                    if !projection.partition {
                        errors.push_validation(
                            scope,
                            Error::ProjectionNotPartitioned {
                                category: category.to_string(),
                                field: field.clone(),
                                collection: collection.to_string(),
                            },
                        );
                    }
                    projection
                }
                None => {
                    errors.push_validation(
                        scope,
                        Error::NoSuchProjection {
                            category: category.to_string(),
                            field: field.clone(),
                            collection: collection.to_string(),
                        },
                    );
                    continue;
                }
            };

            // Map partition to its accepted value type.
            // We'll error elsewhere if it's not found.
            let type_ = schema_shape
                .shape
                .locate(&doc::Pointer::from_str(&partition.location))
                .map(|(shape, _)| shape.type_)
                .unwrap_or(types::ANY);

            for value in values {
                if !type_.overlaps(types::Set::for_value(value)) {
                    errors.push_validation(
                        scope,
                        Error::SelectorTypeMismatch {
                            category: category.to_string(),
                            field: field.clone(),
                            value: value.to_string(),
                            type_,
                        },
                    );
                }

                if value.as_str() == Some("") {
                    errors.push_validation(
                        scope,
                        Error::SelectorEmptyString {
                            category: category.to_string(),
                            field: field.clone(),
                        },
                    );
                }
            }
        }
    }
}

pub fn walk_keyed_location(
    scope: &Url,
    schema: &Url,
    ptr: &source::JsonPointer,
    shape: &inference::Shape,
    must_exist: bool,
    errors: &mut tables::Errors,
) {
    if !must_exist {
        errors.push_validation(
            scope,
            Error::KeyMayNotExist {
                ptr: ptr.to_string(),
                type_: shape.type_,
                schema: scope.clone(),
            },
        );
    }

    // Prohibit types not suited to being keys.
    let disallowed = shape.type_ & (types::OBJECT | types::ARRAY | types::FRACTIONAL);

    if disallowed != types::INVALID {
        errors.push_validation(
            scope,
            Error::KeyWrongType {
                ptr: ptr.to_string(),
                type_: shape.type_,
                disallowed,
                schema: schema.clone(),
            },
        );
    }
}

pub fn walk_composite_key(
    scope: &Url,
    key: &source::CompositeKey,
    schema: &SchemaShape,
    errors: &mut tables::Errors,
) -> Option<Vec<types::Set>> {
    let mut out = Some(Vec::new());

    for ptr in key.iter() {
        match schema.shape.locate(&doc::Pointer::from_str(ptr)) {
            Some((shape, must_exist)) => {
                walk_keyed_location(scope, &schema.schema, ptr, shape, must_exist, errors);

                out = out.map(|mut out| {
                    out.push(shape.type_);
                    out
                });
            }
            None => {
                errors.push_validation(
                    scope,
                    Error::NoSuchPointer {
                        ptr: ptr.to_string(),
                        schema: schema.schema.clone(),
                    },
                );
                out = None;
            }
        }
    }

    out
}

pub fn walk_reference<'a, T, F, N>(
    this_scope: &Url,
    this_entity: &'static str,
    this_name: &str,
    ref_entity: &'static str,
    ref_name: &N,
    entities: &'a [T],
    entity_fn: F,
    imports: &'a [&'a tables::Import],
    errors: &mut tables::Errors,
) -> Option<&'a T>
where
    F: Fn(&'a T) -> (&'a N, &'a Url),
    N: std::ops::Deref<Target = str> + Eq + 'static,
{
    if let Some(entity) = entities.iter().find(|t| entity_fn(t).0 == ref_name) {
        let ref_scope = entity_fn(entity).1;
        if !import_path(imports, this_scope, ref_scope) {
            errors.push_validation(
                this_scope,
                Error::MissingImport {
                    this_entity,
                    this_name: this_name.to_string(),
                    ref_entity,
                    ref_name: ref_name.to_string(),
                    ref_scope: ref_scope.clone(),
                },
            );
        }
        return Some(entity);
    }

    let closest = entities
        .iter()
        .filter_map(|t| {
            let (name, scope) = entity_fn(t);
            let dist = strsim::osa_distance(&ref_name, &name);

            if dist <= 4 {
                Some((dist, name.deref(), scope))
            } else {
                None
            }
        })
        .min();

    if let Some((_, suggest_name, suggest_scope)) = closest {
        errors.push_validation(
            this_scope,
            Error::NoSuchEntitySuggest {
                this_entity,
                this_name: this_name.to_string(),
                ref_entity,
                ref_name: ref_name.to_string(),
                suggest_name: suggest_name.to_string(),
                suggest_scope: suggest_scope.clone(),
            },
        );
    } else {
        errors.push_validation(
            this_scope,
            Error::NoSuchEntity {
                this_entity,
                this_name: this_name.to_string(),
                ref_entity,
                ref_name: ref_name.to_string(),
            },
        );
    }

    None
}

fn walk_name(
    scope: &Url,
    entity: &'static str,
    name: &str,
    re: &Regex,
    errors: &mut tables::Errors,
) {
    if name.len() == 0 {
        errors.push_validation(scope, Error::NameEmpty { entity });
    }

    let (start, stop) = re
        .find(name)
        .map(|m| (m.start(), m.end()))
        .unwrap_or((0, 0));
    let unmatched = [&name[..start], &name[stop..]].concat();

    if !unmatched.is_empty() {
        errors.push_validation(
            scope,
            Error::NameRegex {
                entity,
                name: name.to_string(),
                unmatched,
            },
        );
    }
}

pub fn walk_duplicates<'a, I, N>(entity: &'static str, i: I, errors: &mut tables::Errors)
where
    I: Iterator<Item = (&'a N, &'a Url)> + 'a,
    N: std::ops::Deref<Target = str> + Clone + 'static,
{
    // Sort entity iterator by increasing, collated name.
    let i = i.sorted_by(|(lhs, _), (rhs, _)| collate(lhs.chars()).cmp(collate(rhs.chars())));

    // Walk ordered 2-tuples of names & their scopes,
    // looking for duplicates or prefixes.
    for ((lhs, lhs_scope), (rhs, rhs_scope)) in i.tuple_windows() {
        // This loop is walking zipped characters of each name, and doing two things:
        // 1) Identifying an exact match (iterator drains with no different characters).
        // 2) Identifying hierarchical prefixes:
        //     "foo/bar" is a prefix of "foo/bar/baz"
        //     "foo/bar" is *not* a prefix of "foo/bark".
        let l = collate(lhs.chars());
        let r = collate(rhs.chars());
        let mut it = l.zip_longest(r);

        loop {
            match it.next() {
                Some(EitherOrBoth::Both(l, r)) if l == r => continue,
                Some(EitherOrBoth::Both(_, _)) => {
                    break; // Neither equal nor a prefix.
                }
                Some(EitherOrBoth::Left(_)) => unreachable!("prevented by sorting"),
                Some(EitherOrBoth::Right(r)) if r == '/' => {
                    // LHS finished *just* as we reached a '/',
                    // as in "foo/bar" vs "foo/bar/".
                    errors.push_validation(
                        lhs_scope,
                        Error::Prefix {
                            entity,
                            lhs: lhs.to_string(),
                            rhs: rhs.to_string(),
                            rhs_scope: rhs_scope.clone(),
                        },
                    );
                }
                Some(EitherOrBoth::Right(_)) => {
                    // E.x. "foo/bar" vs "foo/bark". A prefix, but not a hierarchical one.
                    break;
                }
                None => {
                    // Iterator finished with no different characters.
                    errors.push_validation(
                        lhs_scope,
                        Error::Duplicate {
                            entity,
                            lhs: lhs.to_string(),
                            rhs_scope: rhs_scope.clone(),
                        },
                    );
                    break;
                }
            }
        }
    }
}

pub fn import_path(imports: &[&tables::Import], src_scope: &Url, tgt_scope: &Url) -> bool {
    let edges = |from: &Url| {
        let range = imports.equal_range_by_key(&from, |import| &import.from_resource);
        imports[range].iter().map(|import| &import.to_resource)
    };

    // Trim any fragment suffix of each scope to obtain the base resource.
    let (mut src, mut tgt) = (src_scope.clone(), tgt_scope.clone());
    src.set_fragment(None);
    tgt.set_fragment(None);

    // Search forward paths.
    if let Some(_) = pathfinding::directed::bfs::bfs(&&src, |f| edges(f), |s| s == &&tgt) {
        true
    } else if let Some(_) =
        // Search backward paths.
        pathfinding::directed::bfs::bfs(&&tgt, |f| edges(f), |s| s == &&src)
    {
        true
    } else {
        false
    }
}

const TOKEN: &'static str = r"[\pL\pN\-_.]+";

lazy_static! {
    static ref CAPTURE_RE: Regex = Regex::new(&[TOKEN, "(:?/", TOKEN, ")*"].concat()).unwrap();
    static ref COLLECTION_RE: Regex = Regex::new(&[TOKEN, "(:?/", TOKEN, ")*"].concat()).unwrap();
    static ref MATERIALIZATION_RE: Regex =
        Regex::new(&[TOKEN, "(:?/", TOKEN, ")*"].concat()).unwrap();
    static ref ENDPOINT_RE: Regex = Regex::new(TOKEN).unwrap();
    static ref PARTITION_RE: Regex = Regex::new(TOKEN).unwrap();
    static ref TRANSFORM_RE: Regex = Regex::new(TOKEN).unwrap();
}

#[cfg(test)]
mod tests;
