use super::Error;
use doc::inference::{self, Exists};
use itertools::{EitherOrBoth, Itertools};
use json::schema::types;
use proto_flow::flow;
use superslice::Ext;
use url::Url;

pub struct Shape<'c> {
    // Schema URL, including fragment pointer.
    pub schema: Url,
    // Inferred schema shape.
    pub shape: inference::Shape,
    // Canonical field names and corresponding locations, sorted on field.
    // This combines implicit, discovered locations with explicit projected locations.
    pub fields: Vec<(String, models::JsonPointer)>,
    // Schema document with bundled dependencies.
    pub bundle: serde_json::Value,
    // Schema index for validations with this shape.
    pub index: doc::SchemaIndex<'c>,
}

/// Ref is a reference to a schema.
pub enum Ref<'a> {
    // Root resource of the catalog is a schema.
    Root(&'a url::Url),
    // Schema of a collection.
    Collection {
        collection: &'a tables::Collection,
        // Projections of this collection.
        projections: &'a [tables::Projection],
        // Is this a reference to a distinct collection write schema?
        // As opposed to its read schema, or a shared write & read schema.
        write_only: bool,
    },
    // Schema of a derivation register.
    Register(&'a tables::Derivation),
    // Schema being read by a transform.
    Source {
        transform: &'a tables::Transform,
        // `read_schema` of the referenced source collection.
        read_schema: &'a url::Url,
    },
}

impl<'a> Ref<'a> {
    fn scope(&'a self) -> &'a url::Url {
        match self {
            Ref::Root(schema) => schema,
            Ref::Collection { collection, .. } => &collection.scope,
            Ref::Register(derivation) => &derivation.scope,
            Ref::Source { transform, .. } => &transform.scope,
        }
    }

    fn schema(&'a self) -> &'a url::Url {
        match self {
            Ref::Root(schema) => schema,
            Ref::Collection {
                collection,
                projections: _,
                write_only,
            } => {
                if *write_only {
                    &collection.write_schema
                } else {
                    &collection.read_schema
                }
            }
            Ref::Register(derivation) => &derivation.register_schema,
            Ref::Source {
                read_schema: schema,
                ..
            } => schema,
        }
    }

    fn explicit_locations(&'a self) -> impl Iterator<Item = &'a models::JsonPointer> {
        let b: Box<dyn Iterator<Item = &'a models::JsonPointer>> = match self {
            Ref::Root(_) => Box::new(std::iter::empty()),
            Ref::Collection {
                collection,
                projections,
                write_only,
            } => Box::new(
                // Locations of explicit projections of the collection are explicit
                // schema locations, as are the components of the collection key itself.
                // If a schema is used only for writes, we skip projections that aren't logical partitions.
                projections
                    .iter()
                    .filter_map(|projection| {
                        let (location, partition) = projection.spec.as_parts();

                        if partition || !*write_only {
                            Some(location)
                        } else {
                            None
                        }
                    })
                    .chain(collection.spec.key.iter()),
            ),
            Ref::Register(_) => Box::new(std::iter::empty()),
            Ref::Source { transform, .. } => {
                if let Some(models::Shuffle::Key(key)) = &transform.spec.shuffle {
                    // Shuffle keys of the transform are explicit schema locations.
                    Box::new(key.iter())
                } else {
                    Box::new(std::iter::empty())
                }
            }
        };
        b
    }

    pub fn from_tables(
        collections: &'a [tables::Collection],
        derivations: &'a [tables::Derivation],
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
                content_type: flow::ContentType::JsonSchema,
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
                write_only: false,
            });
            // A distinct write schema creates a separate schema reference for the collection.
            if collection.write_schema != collection.read_schema {
                refs.push(Ref::Collection {
                    collection,
                    projections,
                    write_only: true,
                });
            }
        }

        for derivation in derivations.iter() {
            refs.push(Ref::Register(derivation));
        }

        // We track schema references from transforms as they may have shuffle keys
        // which contribute to the explicitly inferred locations of a schema.
        for transform in transforms.iter() {
            let source = collections
                [collections.equal_range_by_key(&&transform.spec.source.name, |c| &c.collection)]
            .first();

            // If source is not Some, it's a referential error that we'll report later.
            if let Some(source) = source {
                refs.push(Ref::Source {
                    read_schema: &source.read_schema,
                    transform,
                });
            }
        }

        refs
    }
}

pub fn index_compiled_schemas<'a>(
    compiled: &'a [(url::Url, doc::Schema)],
    imports: &[tables::Import],
    schema: &'a Url,
    errors: &mut tables::Errors,
) -> doc::SchemaIndex<'a> {
    let mut schema_no_fragment = schema.clone();
    schema_no_fragment.set_fragment(None);

    // Collect all dependencies of |schema|, with |schema| as the first item.
    let mut dependencies = tables::Import::transitive_imports(imports, &schema_no_fragment)
        .filter_map(|url| {
            compiled
                .binary_search_by_key(&url, |(resource, _)| resource)
                .ok()
                .and_then(|ind| compiled.get(ind).map(|c| &c.1))
        })
        .peekable();

    let mut index = doc::SchemaIndexBuilder::new();

    // A root |schema| reference (no fragment) by which the schema was fetched may
    // differ from the canonical URI under which it's indexed. Add an alias.
    if let (None, Some(compiled)) = (schema.fragment(), dependencies.peek()) {
        let _ = index.add_alias(compiled, schema);
    }

    for compiled in dependencies {
        if let Err(err) = index.add(compiled) {
            Error::from(err).push(&compiled.curi, errors);
        }
    }

    if let Err(err) = index.verify_references() {
        Error::from(err).push(schema, errors);
    }

    index.into_index()
}

pub fn walk_all_schema_refs<'a>(
    compiled_schemas: &'a [(url::Url, doc::Schema)],
    imports: &[tables::Import],
    resources: &[tables::Resource],
    schema_refs: &'a [Ref<'_>],
    errors: &mut tables::Errors,
) -> (Vec<Shape<'a>>, tables::Inferences) {
    let mut schema_shapes: Vec<Shape> = Vec::new();
    let mut inferences = tables::Inferences::new();

    // Walk schema URLs (*with* fragment pointers) with their grouped references.
    for (schema, references) in schema_refs
        .iter()
        .sorted_by_key(|r| r.schema())
        .group_by(|r| r.schema())
        .into_iter()
    {
        let bundle = assemble::bundled_schema(schema, imports, resources);
        let index = index_compiled_schemas(compiled_schemas, imports, schema, errors);
        let references = references.collect::<Vec<_>>();

        let shape = match index.fetch(schema) {
            Some(schema) => inference::Shape::infer(schema, &index),
            None => {
                // Schema was not found in our schema index.
                // Report error against each reference of the schema.
                for reference in references.iter() {
                    let schema = schema.clone();
                    Error::NoSuchSchema { schema }.push(reference.scope(), errors);
                }
                inference::Shape::default()
            }
        };

        for err in shape.inspect() {
            Error::from(err).push(schema, errors);
        }

        // Map through reference to the explicit locations of the schema which they name.
        // These locations may include entries which aren't statically
        // know-able, e.x. due to additionalItems, additionalProperties or patternProperties.
        let explicit: Vec<&models::JsonPointer> = references
            .iter()
            .flat_map(|r| r.explicit_locations())
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
            .merge_join_by(implicit.into_iter(), |lhs, (rhs, _, _)| {
                lhs.as_str().cmp(rhs)
            })
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
                // Canonical projection field is the JSON pointer stripped of its leading '/'.
                let field = ptr.chars().skip(1).collect::<String>();
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

            if !field.is_empty() {
                fields.push((field, ptr));
            }
        }

        schema_shapes.push(Shape {
            schema: schema.clone(),
            shape,
            fields,
            bundle,
            index,
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
        // for every composite key component pointer we encounter...
        // with the exception of the document root.
        assert!(
            schema.fields.iter().find(|(_, p)| p == ptr).is_some() || ptr.is_empty(),
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
    let (start, stop) = models::JsonPointer::regex()
        .find(ptr)
        .map(|m| (m.start(), m.end()))
        .unwrap_or((0, 0));
    let unmatched = [&ptr[..start], &ptr[stop..]].concat();

    // These checks return early if matched because
    // further errors are likely spurious.
    if !ptr.is_empty() && !ptr.starts_with("/") {
        Error::KeyMissingLeadingSlash {
            ptr: ptr.to_string(),
        }
        .push(scope, errors);
        return;
    } else if !unmatched.is_empty() {
        Error::KeyRegex {
            ptr: ptr.to_string(),
            unmatched,
        }
        .push(scope, errors);
        return;
    } else if exists == Exists::Implicit {
        Error::KeyIsImplicit {
            ptr: ptr.to_string(),
            schema: schema.clone(),
        }
        .push(scope, errors);
        return;
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
