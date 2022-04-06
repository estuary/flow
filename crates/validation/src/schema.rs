use super::Error;
use doc::{
    inference::{self, Exists},
    Schema as CompiledSchema,
};
use itertools::{EitherOrBoth, Itertools};
use json::schema::types;
use superslice::Ext;
use url::Url;

pub struct Shape {
    // Schema URL, including fragment pointer.
    pub schema: Url,
    // Inferred schema shape.
    pub shape: inference::Shape,
    // Canonical field names and corresponding locations, sorted on field.
    // This combines implicit, discovered locations with explicit projected locations.
    pub fields: Vec<(String, models::JsonPointer)>,
    // Schema document with bundled dependencies.
    pub bundle: serde_json::Value,
}

/// Ref is a reference to a schema.
pub enum Ref<'a> {
    // Root resource of the catalog is a schema.
    Root(&'a url::Url),
    // Schema has an anchored, explicit name.
    Named(&'a tables::NamedSchema),
    // Schema of a collection.
    Collection {
        collection: &'a tables::Collection,
        // Projections of this collection.
        projections: &'a [tables::Projection],
    },
    // Schema of a derivation register.
    Register(&'a tables::Derivation),
    // Schema being read by a transform.
    Source {
        transform: &'a tables::Transform,
        // Schema resolved from either transform.source_schema,
        // or the schema of the referenced collection.
        schema: &'a url::Url,
    },
    // Schema of a foreign collection.
    ForeignCollection {
        foreign_collection: &'a tables::BuiltCollection,
        // |foreign_collection.spec.projections|, as JsonPointers.
        projections: Vec<models::JsonPointer>,
    },
    // Foreign collection sourced by a transform using its original schema.
    // Note a transform using an alternative source schema is a Self::Source
    // even if the sourced collection is foreign.
    ForeignSource {
        transform: &'a tables::Transform,
        foreign_collection: &'a tables::BuiltCollection,
    },
}

impl<'a> Ref<'a> {
    fn scope(&'a self) -> &'a url::Url {
        match self {
            Ref::Root(schema) => schema,
            Ref::Named(named) => &named.scope,
            Ref::Collection { collection, .. } => &collection.scope,
            Ref::Register(derivation) => &derivation.scope,
            Ref::Source { transform, .. } => &transform.scope,
            Ref::ForeignCollection {
                foreign_collection, ..
            } => &foreign_collection.scope,
            Ref::ForeignSource {
                foreign_collection, ..
            } => &foreign_collection.scope,
        }
    }

    fn schema(&'a self) -> &'a url::Url {
        match self {
            Ref::Root(schema) => schema,
            Ref::Named(named) => &named.anchor,
            Ref::Collection { collection, .. } => &collection.schema,
            Ref::Register(derivation) => &derivation.register_schema,
            Ref::Source { schema, .. } => schema,

            // Self::Foreign* use their unique scopes as their schema URLs,
            // and can never group with a non-foreign schema URL.
            Ref::ForeignCollection {
                foreign_collection, ..
            } => &foreign_collection.scope,
            Ref::ForeignSource {
                foreign_collection, ..
            } => &foreign_collection.scope,
        }
    }

    fn explicit_locations(&'a self) -> impl Iterator<Item = &'a models::JsonPointer> {
        let b: Box<dyn Iterator<Item = &'a models::JsonPointer>> = match self {
            Ref::Root(_) => Box::new(std::iter::empty()),
            Ref::Named(_) => Box::new(std::iter::empty()),
            Ref::Collection {
                collection,
                projections,
            } => Box::new(
                // Locations of explicit projections of the collection are explicit
                // schema locations, as are the components of the collection key itself.
                projections
                    .iter()
                    .map(|p| &p.location)
                    .chain(collection.key.iter()),
            ),
            Ref::Register(_) => Box::new(std::iter::empty()),
            Ref::Source { transform, .. } | Ref::ForeignSource { transform, .. } => Box::new(
                // Shuffle keys of the transform are explicit schema locations.
                transform
                    .shuffle_key
                    .iter()
                    .flat_map(|composite| composite.iter()),
            ),
            Ref::ForeignCollection { projections, .. } => Box::new(projections.iter()),
        };
        b
    }

    pub fn from_tables(
        collections: &'a [tables::Collection],
        derivations: &'a [tables::Derivation],
        foreign_collections: &'a [tables::BuiltCollection],
        named_schemas: &'a [tables::NamedSchema],
        projections: &'a [tables::Projection],
        resources: &'a [tables::Resource],
        root_scope: &'a url::Url,
        transforms: &'a [tables::Transform],
    ) -> Vec<Ref<'a>> {
        let mut refs = Vec::new();

        // If the root resource is a JSON schema then treat it as an implicit reference.
        let root = &resources[resources.equal_range_by_key(&root_scope, |r| &r.resource)];
        match root.first() {
            Some(tables::Resource {
                content_type: models::ContentType::JsonSchema,
                resource,
                ..
            }) => {
                refs.push(Ref::Root(resource));
            }
            _ => (),
        };

        for collection in collections.iter() {
            let projections = &projections
                [projections.equal_range_by_key(&&collection.collection, |p| &p.collection)];

            refs.push(Ref::Collection {
                collection,
                projections,
            });
        }

        for derivation in derivations.iter() {
            refs.push(Ref::Register(derivation));
        }

        for collection in foreign_collections.iter() {
            assert!(
                collection.foreign_build_id.is_some(),
                "collection {:?} is not foreign",
                &collection.collection
            );

            refs.push(Ref::ForeignCollection {
                foreign_collection: collection,
                projections: collection
                    .spec
                    .projections
                    .iter()
                    .map(|p| models::JsonPointer::new(&p.ptr))
                    .collect(),
            });
        }

        for named in named_schemas {
            refs.push(Ref::Named(named));
        }

        // We track schema references from transforms for two reasons:
        // * Transforms may use alternate source schemas,
        //   which are distinguished from the collection's declared schema.
        // * Transforms may have shuffle keys which contribute to the explicitly
        //   inferred locations of a schema.
        for transform in transforms.iter() {
            let local = collections
                [collections.equal_range_by_key(&&transform.source_collection, |c| &c.collection)]
            .first();
            let foreign = foreign_collections[foreign_collections
                .equal_range_by_key(&&transform.source_collection, |c| &c.collection)]
            .first();

            match (&transform.source_schema, local, foreign) {
                (_, Some(_), Some(_)) => panic!(
                    "collection {:?} is both local and foreign",
                    transform.source_collection
                ),
                (Some(schema), _, _) => {
                    refs.push(Ref::Source { schema, transform });
                }
                (None, Some(local), None) => {
                    refs.push(Ref::Source {
                        schema: &local.schema,
                        transform,
                    });
                }
                (None, None, Some(foreign_collection)) => {
                    refs.push(Ref::ForeignSource {
                        transform,
                        foreign_collection,
                    });
                }
                (None, None, None) => {
                    // Referential error that we'll report later.
                }
            }
        }

        refs
    }
}

pub fn walk_all_named_schemas<'a>(
    named_schemas: &'a [tables::NamedSchema],
    errors: &mut tables::Errors,
) {
    for (lhs, rhs) in named_schemas.iter().tuple_windows() {
        if lhs.anchor_name == rhs.anchor_name {
            Error::NameCollision {
                error_class: "duplicates",
                lhs_entity: "named schema",
                lhs_name: lhs.anchor_name.clone(),
                rhs_entity: "named schema",
                rhs_name: rhs.anchor_name.clone(),
                rhs_scope: rhs.scope.clone(),
            }
            .push(&lhs.scope, errors);
        }
    }
}

pub fn index_compiled_schemas<'a>(
    compiled: &'a [CompiledSchema],
    root_scope: &Url,
    errors: &mut tables::Errors,
) -> doc::SchemaIndex<'a> {
    let mut index = doc::SchemaIndexBuilder::new();

    for compiled in compiled {
        if let Err(err) = index.add(compiled) {
            Error::from(err).push(&compiled.curi, errors);
        }
    }

    // TODO(johnny): report multiple errors and visit each,
    // rather than stopping at the first.
    if let Err(err) = index.verify_references() {
        Error::from(err).push(root_scope, errors);
    }

    index.into_index()
}

pub fn walk_all_schema_refs(
    imports: &[tables::Import],
    schema_docs: &[tables::SchemaDoc],
    schema_index: &doc::SchemaIndex<'_>,
    schema_refs: &[Ref<'_>],
    errors: &mut tables::Errors,
) -> (Vec<Shape>, tables::Inferences) {
    let mut schema_shapes: Vec<Shape> = Vec::new();
    let mut inferences = tables::Inferences::new();

    // Walk schema URLs (*with* fragment pointers) with their grouped references.
    for (schema, references) in schema_refs
        .iter()
        .sorted_by_key(|r| r.schema())
        .group_by(|r| r.schema())
        .into_iter()
    {
        let references = references.collect::<Vec<_>>();

        let (shape, bundle) = match (schema_index.fetch(schema), references[0]) {
            // Is this an indexed schema of the current build?
            (Some(compiled), _) => (
                inference::Shape::infer(compiled, &schema_index),
                assemble::bundled_schema(schema, imports, &schema_docs),
            ),
            // Is this a foreign collection?
            // Infer from its inline schema instead of the build's schema index.
            (
                _,
                Ref::ForeignCollection {
                    foreign_collection, ..
                }
                | Ref::ForeignSource {
                    foreign_collection, ..
                },
            ) => match foreign_shape(foreign_collection) {
                Ok(ok) => ok,
                Err(err) => {
                    err.push(references[0].scope(), errors);
                    (inference::Shape::default(), serde_json::Value::Bool(true))
                }
            },
            (None, _) => {
                // Schema is local, and was not found in our schema index.
                // Report error against each reference of the schema.
                for reference in references.iter() {
                    let schema = schema.clone();
                    Error::NoSuchSchema { schema }.push(reference.scope(), errors);
                }
                (inference::Shape::default(), serde_json::Value::Bool(true))
            }
        };

        for err in shape.inspect() {
            Error::from(err).push(schema, errors);
        }

        // Map through reference to the explicit locations of the schema which they name.
        // These locations may include entries which aren't statically
        // know-able, e.x. due to additionalItems, additionalProperties or patternProperties.
        let explicit: Vec<&str> = references
            .iter()
            .flat_map(|r| r.explicit_locations())
            .map(AsRef::as_ref)
            .sorted()
            .dedup()
            .collect();

        // Now identify all implicit, statically known-able schema locations
        // which are not patterns. These may overlap with explicit.
        let implicit = shape
            .locations()
            .into_iter()
            .filter_map(|(ptr, pattern, shape, exists)| {
                if !pattern {
                    Some((ptr, shape, exists))
                } else {
                    None // Filter locations which are patterns.
                }
            })
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .collect::<Vec<_>>();

        // Merge explicit & implicit into a unified sequence of schema locations.
        let merged = explicit
            .into_iter()
            .merge_join_by(implicit.into_iter(), |lhs, (rhs, _, _)| (*lhs).cmp(rhs))
            .map(|eob| match eob {
                EitherOrBoth::Left(ptr) => {
                    let (shape, exists) = shape.locate(&doc::Pointer::from_str(ptr));
                    (ptr.to_string(), shape, exists)
                }
                EitherOrBoth::Both(_, (ptr, shape, exists))
                | EitherOrBoth::Right((ptr, shape, exists)) => (ptr, shape, exists),
            })
            // Generate a canonical projection field for each location.
            .map(|(ptr, shape, exists)| {
                let field = if ptr.is_empty() {
                    "flow_document".to_string()
                } else {
                    // Canonical projection field is the JSON pointer
                    // stripped of its leading '/'.
                    ptr.chars().skip(1).collect::<String>()
                };

                (field, models::JsonPointer::new(ptr), shape, exists)
            })
            // Re-order to walk in ascending field name order.
            .sorted_by(|a, b| a.0.cmp(&b.0));

        // Now collect |fields| in ascending field order,
        // and record all inferences.
        let mut fields = Vec::with_capacity(merged.len());

        for (field, ptr, shape, exists) in merged {
            // Note we're already ordered on |field|.
            inferences.insert_row(schema, &ptr, assemble::inference(shape, exists));
            fields.push((field, ptr));
        }

        schema_shapes.push(Shape {
            schema: schema.clone(),
            shape,
            fields,
            bundle,
        });
    }

    (schema_shapes, inferences)
}

pub fn gather_key_types(key: &models::CompositeKey, schema: &Shape) -> Option<Vec<types::Set>> {
    let mut out = Vec::new();

    for ptr in key.iter() {
        let (shape, exists) = schema.shape.locate(&doc::Pointer::from_str(ptr));

        // We already error'd if the key could not exist or isn't key-able.
        // Don't produce further errors about conflicting key types.
        if !matches!(
            (exists, unkeyable(shape.type_)),
            (Exists::Must, types::INVALID)
        ) {
            return None;
        }

        out.push(shape.type_);
    }
    Some(out)
}

pub fn walk_composite_key(
    scope: &Url,
    key: &models::CompositeKey,
    schema: &Shape,
    errors: &mut tables::Errors,
) {
    for ptr in key.iter() {
        // An explicit field should be attached to the schema shape
        // for every composite key component pointer we encounter.
        assert!(
            schema.fields.iter().find(|(_, p)| p == ptr).is_some(),
            "scope {} key {} not found in schema {}",
            scope.to_string(),
            ptr.as_str(),
            schema.schema.to_string()
        );

        let (shape, exists) = schema.shape.locate(&doc::Pointer::from_str(ptr));
        walk_explicit_location(scope, &schema.schema, ptr, true, shape, exists, errors);
    }
}

// Walk a JSON pointer which was explicitly provided by the user.
// Examples include collection key components, shuffle key components,
// and collection projections.
// A location which is serving as a key has additional restrictions
// on its required existence and applicable types.
pub fn walk_explicit_location(
    scope: &Url,
    schema: &Url,
    ptr: &models::JsonPointer,
    is_key: bool,
    shape: &inference::Shape,
    exists: Exists,
    errors: &mut tables::Errors,
) {
    if exists == Exists::Implicit {
        Error::KeyIsImplicit {
            ptr: ptr.to_string(),
            schema: schema.clone(),
        }
        .push(scope, errors);
        return; // Further errors are likely spurious.
    } else if exists == Exists::Cannot {
        Error::KeyCannotExist {
            ptr: ptr.to_string(),
            schema: schema.clone(),
        }
        .push(scope, errors);
        return;
    }

    // Remaining validations apply only to key locations.
    if !is_key {
        return;
    }

    if exists == Exists::May {
        Error::KeyMayNotExist {
            ptr: ptr.to_string(),
            schema: schema.clone(),
        }
        .push(scope, errors);
    }

    if unkeyable(shape.type_) != types::INVALID {
        Error::KeyWrongType {
            ptr: ptr.to_string(),
            type_: shape.type_,
            disallowed: unkeyable(shape.type_),
            schema: schema.clone(),
        }
        .push(scope, errors);
    }

    if !matches!(
        shape.reduction,
        inference::Reduction::Unset | inference::Reduction::LastWriteWins,
    ) {
        Error::KeyHasReduction {
            ptr: ptr.to_string(),
            schema: schema.clone(),
            strategy: shape.reduction.clone(),
        }
        .push(scope, errors);
    }
}

// unkeyable returns the types of |t| which cannot participate in a Flow key.
fn unkeyable(t: types::Set) -> types::Set {
    t & (types::OBJECT | types::ARRAY | types::FRACTIONAL | types::NULL)
}

// foreign_shape maps a ForeignCollection to an inference::Shape using
// its inlined schema, also returned. We'll surface an encountered error
// but generally never expect that there might be one, since the
// ForeignCollection is a product of a prior (successful) build process.
pub fn foreign_shape(
    f: &tables::BuiltCollection,
) -> Result<(inference::Shape, serde_json::Value), Error> {
    let bundle = serde_json::from_str(&f.spec.schema_json)?;
    let schema = json::schema::build::build_schema(f.scope.clone(), &bundle)?;

    let mut index = doc::SchemaIndexBuilder::new();
    index.add(&schema)?;
    index.verify_references()?;
    let index = index.into_index();

    Ok((inference::Shape::infer(&schema, &index), bundle))
}
