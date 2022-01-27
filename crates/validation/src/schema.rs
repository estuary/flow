use super::Error;
use doc::{inference, Schema as CompiledSchema};
use itertools::{EitherOrBoth, Itertools};
use json::schema::types;
use models::{self, build, tables};
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
}

impl<'a> Ref<'a> {
    pub fn scope(&'a self) -> &'a url::Url {
        match self {
            Ref::Root(schema) => schema,
            Ref::Named(named) => &named.scope,
            Ref::Collection { collection, .. } => &collection.scope,
            Ref::Register(derivation) => &derivation.scope,
            Ref::Source { transform, .. } => &transform.scope,
        }
    }

    pub fn schema(&'a self) -> &'a url::Url {
        match self {
            Ref::Root(schema) => schema,
            Ref::Named(named) => &named.anchor,
            Ref::Collection { collection, .. } => &collection.schema,
            Ref::Register(derivation) => &derivation.register_schema,
            Ref::Source { schema, .. } => schema,
        }
    }

    pub fn explicit_locations(&'a self) -> impl Iterator<Item = &'a models::JsonPointer> {
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
            Ref::Source { transform, .. } => Box::new(
                // Shuffle keys of the transform are explicit schema locations.
                transform
                    .shuffle_key
                    .iter()
                    .flat_map(|composite| composite.iter()),
            ),
        };
        b
    }

    pub fn from_tables(
        collections: &'a [tables::Collection],
        derivations: &'a [tables::Derivation],
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

        for named in named_schemas {
            refs.push(Ref::Named(named));
        }
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
        for transform in transforms.iter() {
            if let Some(schema) = &transform.source_schema {
                refs.push(Ref::Source { schema, transform });
            } else if let Some(c) = collections
                [collections.equal_range_by_key(&&transform.source_collection, |c| &c.collection)]
            .first()
            {
                refs.push(Ref::Source {
                    schema: &c.schema,
                    transform,
                });
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
        // Infer the schema shape, and report any inspected errors.
        let shape = match schema_index.fetch(schema) {
            Some(s) => inference::Shape::infer(s, &schema_index),
            None => {
                for reference in references {
                    Error::NoSuchSchema {
                        schema: schema.clone(),
                    }
                    .push(reference.scope(), errors);
                }

                schema_shapes.push(Shape {
                    schema: schema.clone(),
                    shape: Default::default(),
                    fields: Default::default(),
                    bundle: Default::default(),
                });
                continue;
            }
        };
        for err in shape.inspect() {
            Error::from(err).push(schema, errors);
        }

        // Map through reference to the explicit locations of the schema which they name.
        // These locations may include entries which aren't statically
        // know-able, e.x. due to additionalItems, additionalProperties or patternProperties.
        let explicit: Vec<&str> = references
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
            .filter_map(|eob| match eob {
                EitherOrBoth::Left(ptr) => shape
                    .locate(&doc::Pointer::from_str(ptr))
                    // We'll skip entries from projections that can't be located in the shape.
                    // These will generate a proper error later.
                    .map(|(shape, exists)| (ptr.to_string(), shape, exists)),
                EitherOrBoth::Both(_, (ptr, shape, exists))
                | EitherOrBoth::Right((ptr, shape, exists)) => Some((ptr, shape, exists)),
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
            inferences.insert_row(schema, &ptr, build::inference(shape, exists));

            if !exists.cannot() {
                fields.push((field, ptr)); // Note we're already ordered on |field|.
            }
        }

        schema_shapes.push(Shape {
            schema: schema.clone(),
            shape,
            fields,
            bundle: build::bundled_schema(schema, imports, &schema_docs),
        });
    }

    (schema_shapes, inferences)
}

pub fn walk_composite_key(
    scope: &Url,
    key: &models::CompositeKey,
    schema: &Shape,
    errors: &mut tables::Errors,
) -> Option<Vec<types::Set>> {
    let mut out = Some(Vec::new());

    for ptr in key.iter() {
        match schema.shape.locate(&doc::Pointer::from_str(ptr)) {
            Some((shape, exists)) => {
                walk_keyed_location(scope, &schema.schema, ptr, shape, exists, errors);

                out = out.map(|mut out| {
                    out.push(shape.type_);
                    out
                });
            }
            None => {
                Error::NoSuchPointer {
                    ptr: ptr.to_string(),
                    schema: schema.schema.clone(),
                }
                .push(scope, errors);
                out = None;
            }
        }
    }

    out
}

pub fn walk_keyed_location(
    scope: &Url,
    schema: &Url,
    ptr: &models::JsonPointer,
    shape: &inference::Shape,
    exists: inference::Exists,
    errors: &mut tables::Errors,
) {
    if !exists.must() {
        Error::KeyMayNotExist {
            ptr: ptr.to_string(),
            type_: shape.type_,
            schema: scope.clone(),
        }
        .push(scope, errors);
    }

    // Prohibit types not suited to being keys.
    let disallowed = shape.type_ & (types::OBJECT | types::ARRAY | types::FRACTIONAL | types::NULL);

    if disallowed != types::INVALID {
        Error::KeyWrongType {
            ptr: ptr.to_string(),
            type_: shape.type_,
            disallowed,
            schema: schema.clone(),
        }
        .push(scope, errors);
    }
}
