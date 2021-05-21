use super::Error;
use doc::{inference, Schema as CompiledSchema};
use itertools::{EitherOrBoth, Itertools};
use json::schema::types;
use models::{build, names, tables};
use url::Url;

pub struct Shape {
    // Schema URL, including fragment pointer.
    pub schema: Url,
    // Inferred schema shape.
    pub shape: inference::Shape,
    // Canonical field names and corresponding locations, sorted on field.
    // This combines implicit, discovered locations with explicit projected locations.
    pub fields: Vec<(String, names::JsonPointer)>,
}

/// Ref is a reference to a schema.
pub struct Ref<'a> {
    // Scope referencing the schema.
    pub scope: &'a url::Url,
    // Schema which is referenced, including fragment pointer.
    pub schema: &'a url::Url,
    // Collection having this schema, for which this SchemaRef was created.
    // None if this reference is not a collection schema.
    pub collection: Option<&'a names::Collection>,
}

impl<'a> Ref<'a> {
    pub fn from_tables(
        resources: &'a [tables::Resource],
        named_schemas: &'a [tables::NamedSchema],
        collections: &'a [tables::Collection],
        derivations: &'a [tables::Derivation],
        transforms: &'a [tables::Transform],
    ) -> Vec<Ref<'a>> {
        let mut refs = Vec::new();

        // If the root resource is a JSON schema then treat it as an implicit reference.
        match resources.first() {
            Some(tables::Resource {
                content_type: protocol::flow::ContentType::JsonSchema,
                resource,
                ..
            }) => {
                refs.push(Ref {
                    scope: resource,
                    schema: resource,
                    collection: None,
                });
            }
            _ => (),
        };

        for n in named_schemas {
            refs.push(Ref {
                scope: &n.scope,
                schema: &n.anchor,
                collection: None,
            });
        }
        for c in collections.iter() {
            refs.push(Ref {
                scope: &c.scope,
                schema: &c.schema,
                collection: Some(&c.collection),
            })
        }
        for d in derivations.iter() {
            refs.push(Ref {
                scope: &d.scope,
                schema: &d.register_schema,
                collection: None,
            })
        }
        for t in transforms.iter() {
            if let Some(schema) = &t.source_schema {
                refs.push(Ref {
                    scope: &t.scope,
                    schema,
                    collection: None,
                })
            }
        }

        refs
    }
}

pub fn walk_all_named_schemas<'a>(
    named_schemas: &'a [tables::NamedSchema],
    errors: &mut tables::Errors,
) {
    for (lhs, rhs) in named_schemas
        .iter()
        .sorted_by_key(|n| &n.anchor_name)
        .tuple_windows()
    {
        if lhs.anchor_name == rhs.anchor_name {
            Error::Duplicate {
                entity: "named schema",
                lhs: lhs.anchor_name.clone(),
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
    let mut index = doc::SchemaIndex::new();

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

    index
}

pub fn walk_all_schema_refs(
    index: &doc::SchemaIndex<'_>,
    projections: &[tables::Projection],
    schema_refs: &[Ref<'_>],
    errors: &mut tables::Errors,
) -> (Vec<Shape>, tables::Inferences) {
    let mut schema_shapes: Vec<Shape> = Vec::new();
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
                    Error::NoSuchSchema {
                        schema: schema.clone(),
                    }
                    .push(reference.scope, errors);
                }

                schema_shapes.push(Shape {
                    schema: schema.clone(),
                    shape: Default::default(),
                    fields: Default::default(),
                });
                continue;
            }
        };
        for err in shape.inspect() {
            Error::from(err).push(schema, errors);
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

                (field, names::JsonPointer::new(ptr), shape, exists)
            })
            // Re-order to walk in ascending field name order.
            .sorted_by(|a, b| a.0.cmp(&b.0));

        // Now collect |fields| in ascending field order,
        // and record all inferences.
        let mut fields = Vec::with_capacity(merged.len());

        for (field, ptr, shape, exists) in merged {
            inferences.push_row(schema, &ptr, build::inference(shape, exists));

            if !exists.cannot() {
                fields.push((field, ptr)); // Note we're already ordered on |field|.
            }
        }

        schema_shapes.push(Shape {
            schema: schema.clone(),
            shape,
            fields,
        });
    }

    (schema_shapes, inferences)
}

pub fn walk_composite_key(
    scope: &Url,
    key: &names::CompositeKey,
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
    ptr: &names::JsonPointer,
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
    let disallowed = shape.type_ & (types::OBJECT | types::ARRAY | types::FRACTIONAL);

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
