use crate::connector::DeletionMode;
use avro::{located_shape_to_avro, shape_to_avro};
use doc::shape::location;
use itertools::Itertools;
use lazy_static::lazy_static;
use proto_flow::flow::{self, MaterializationSpec};
use std::{borrow::Cow, iter};

lazy_static! {
    static ref META_OP_PTR: doc::Pointer = doc::Pointer::from_str("/_meta/op");
    static ref META_IS_DELETED_PTR: doc::Pointer = doc::Pointer::from_str("/_meta/is_deleted");
}

#[derive(Debug, Clone)]
pub enum CustomizableExtractor {
    Extractor(doc::Extractor),
    RootExtractorWithIsDeleted,
    IsDeleted,
}

// This lets us add our own "virtual" fields to Dekaf without having to add them to
// doc::Extractor and all of the other platform machinery.
impl CustomizableExtractor {
    pub fn extract<'s, 'n, N: doc::AsNode>(
        &'s self,
        doc: &'n N,
    ) -> Result<&'n N, Cow<'s, serde_json::Value>> {
        match self {
            CustomizableExtractor::Extractor(e) => e.query(doc),
            CustomizableExtractor::IsDeleted => {
                let deletion = match META_OP_PTR.query(doc) {
                    Some(n) => match n.as_node() {
                        doc::Node::String(s) if s == "d" => 1,
                        _ => 0,
                    },
                    None => 0,
                };

                Err(Cow::Owned(serde_json::json!(deletion)))
            }
            CustomizableExtractor::RootExtractorWithIsDeleted => {
                let deletion = match META_OP_PTR.query(doc) {
                    Some(n) => match n.as_node() {
                        doc::Node::String(s) if s == "d" => 1,
                        _ => 0,
                    },
                    None => 0,
                };

                let mut full_doc = serde_json::to_value(&doc::SerPolicy::noop().on(doc)).unwrap();

                if let Some(meta_is_deleted) = META_IS_DELETED_PTR.create_value(&mut full_doc) {
                    *meta_is_deleted = serde_json::json!(deletion);

                    Err(Cow::Owned(full_doc))
                } else {
                    Ok(doc)
                }
            }
        }
    }
}

impl From<doc::Extractor> for CustomizableExtractor {
    fn from(value: doc::Extractor) -> Self {
        Self::Extractor(value)
    }
}

pub fn build_field_extractors(
    source_shape: doc::Shape,
    fields: flow::FieldSelection,
    projections: Vec<flow::Projection>,
    deletions: DeletionMode,
) -> anyhow::Result<(avro::Schema, Vec<(avro::Schema, CustomizableExtractor)>)> {
    let policy = doc::SerPolicy::noop();

    let mut extractor_schemas = fields
        .keys
        .into_iter()
        .chain(fields.values.into_iter())
        .chain(iter::once(fields.document))
        .filter(|f| f.len() > 0)
        .enumerate()
        .map(|(idx, field)| {
            let projection = projections.iter().find(|proj| proj.field == *field);
            if let Some(proj) = projection {
                // Turn the projection into a (avro::Schema, doc::Extractor) pair
                let source_ptr = doc::Pointer::from_str(&proj.ptr);
                let (source_shape, exists) = source_shape.locate(&source_ptr);

                let required = match exists {
                    location::Exists::May | location::Exists::Implicit => false,
                    _ => true,
                };

                let extractor = extractors::for_projection(&proj, &policy)?;

                let default = source_shape.default.as_ref().map(|d| d.0.clone());

                let avro_field = avro::RecordField {
                    schema: located_shape_to_avro(
                        json::Location::Root.push_prop(proj.field.as_str()),
                        source_shape.to_owned(),
                        required,
                    ),
                    name: dekaf_connector::field_fold(&proj.field),
                    doc: None,
                    aliases: None,
                    default,
                    order: apache_avro::schema::RecordFieldOrder::Ascending,
                    position: idx,
                    custom_attributes: Default::default(),
                };

                Ok::<_, anyhow::Error>((avro_field, CustomizableExtractor::Extractor(extractor)))
            } else {
                anyhow::bail!(
                    "Missing projection for field on materialization built spec: {field:?}"
                );
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    if matches!(deletions, DeletionMode::CDC) {
        let mut shape = doc::Shape::nothing();
        shape.type_ = json::schema::types::INTEGER;

        let avro_field = avro::RecordField {
            schema: shape_to_avro(shape),
            name: "_is_deleted".to_string(),
            doc: None,
            aliases: None,
            default: None,
            order: apache_avro::schema::RecordFieldOrder::Ascending,
            position: extractor_schemas.len(),
            custom_attributes: Default::default(),
        };

        extractor_schemas.push((avro_field, CustomizableExtractor::IsDeleted));
    }

    let schema = avro::Schema::Record(avro::RecordSchema {
        name: "root".into(),
        aliases: None,
        doc: None,
        fields: extractor_schemas
            .iter()
            .map(|(field, _)| field.clone())
            .collect_vec(),
        lookup: Default::default(),
        attributes: Default::default(),
    });

    Ok((
        schema,
        extractor_schemas
            .into_iter()
            .map(|(field, extractor)| (field.schema, extractor))
            .collect_vec(),
    ))
}


pub fn fetch_all_collection_names(spec: &MaterializationSpec) -> anyhow::Result<Vec<String>> {
    spec.bindings
        .iter()
        .map(|b| {
            b.resource_path
                .first()
                .cloned()
                .ok_or(anyhow::anyhow!("missing resource path"))
        })
        .collect::<Result<Vec<_>, _>>()
}

pub fn get_binding_for_topic(
    spec: &MaterializationSpec,
    topic_name: &str,
) -> anyhow::Result<Option<proto_flow::flow::materialization_spec::Binding>> {
    Ok(spec
        .bindings
        .iter()
        .find(|binding| {
            binding
                .resource_path
                .first()
                .is_some_and(|path| path == topic_name)
        })
        .map(|b| b.clone()))
}
