use crate::connector::DeletionMode;
use avro::{located_shape_to_avro, shape_to_avro};
use doc::shape::location;
use lazy_static::lazy_static;
use proto_flow::flow;
use std::{borrow::Cow, iter};

lazy_static! {
    static ref META_OP_PTR: doc::Pointer = doc::Pointer::from_str("/_meta/op");
}

#[derive(Debug, Clone)]
pub enum CustomizableExtractor {
    Extractor(doc::Extractor),
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

                Err(Cow::Owned(serde_json::json!({"is_deleted": deletion})))
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
) -> anyhow::Result<(avro::Schema, Vec<CustomizableExtractor>)> {
    let policy = doc::SerPolicy::noop();

    let (mut fields, mut extractors) = fields
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
                    name: proj.field.to_owned(),
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
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .unzip::<_, _, Vec<_>, Vec<_>>();

    if matches!(deletions, DeletionMode::CDC) {
        let mut shape = doc::Shape::nothing();
        shape.type_ = json::schema::types::INTEGER;

        // In order to maintain backwards compatibility, when CDC deletions mode is
        // enabled we should emit {"_meta": {"is_deleted": 1}} instead of a root-level field
        let avro_field = avro::RecordField {
            schema: shape_to_avro(shape),
            name: "is_deleted".to_string(),
            doc: None,
            aliases: None,
            default: None,
            order: apache_avro::schema::RecordFieldOrder::Ascending,
            position: 0,
            custom_attributes: Default::default(),
        };

        let meta_field = avro::RecordField {
            name: "_meta".to_string(),
            schema: avro::Schema::Record(avro::RecordSchema {
                name: "root._meta.is_deleted".into(),
                aliases: None,
                doc: None,
                fields: vec![avro_field],
                lookup: Default::default(),
                attributes: Default::default(),
            }),
            doc: None,
            aliases: None,
            default: None,
            order: apache_avro::schema::RecordFieldOrder::Ascending,
            position: fields.len(),
            custom_attributes: Default::default(),
        };

        fields.push(meta_field);
        extractors.push(CustomizableExtractor::IsDeleted);
    }

    let schema = avro::Schema::Record(avro::RecordSchema {
        name: "root".into(),
        aliases: None,
        doc: None,
        fields: fields,
        lookup: Default::default(),
        attributes: Default::default(),
    });

    Ok((schema, extractors))
}
