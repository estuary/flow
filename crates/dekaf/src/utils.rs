use crate::connector::DeletionMode;
use crate::{task_manager::TaskStateListener, App};
use anyhow::Context;
use async_trait::async_trait;
use avro::{located_shape_to_avro, shape_to_avro};
use aws_config::imds::client;
use doc::shape::location;
use flow_client::client::Client as FlowClient;
use gazette::{broker, journal};
use itertools::Itertools;
use lazy_static::lazy_static;
use proto_flow::flow;
use std::{borrow::Cow, iter};
use std::{sync::Arc, time::SystemTime};

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

pub fn build_LEGACY_field_extractors(
    mut schema: doc::Shape,
    deletions: DeletionMode,
) -> anyhow::Result<(avro::Schema, Vec<(avro::Schema, CustomizableExtractor)>)> {
    if matches!(deletions, DeletionMode::CDC) {
        if let Some(meta) = schema
            .object
            .properties
            .iter_mut()
            .find(|prop| prop.name.to_string() == "_meta".to_string())
        {
            if let Err(idx) = meta
                .shape
                .object
                .properties
                .binary_search_by(|prop| prop.name.to_string().cmp(&"is_deleted".to_string()))
            {
                meta.shape.object.properties.insert(
                    idx,
                    doc::shape::ObjProperty {
                        name: "is_deleted".into(),
                        is_required: true,
                        shape: doc::Shape {
                            type_: json::schema::types::INTEGER,
                            ..doc::Shape::nothing()
                        },
                    },
                );
            } else {
                tracing::warn!(
                    "This collection's schema already has a /_meta/is_deleted location!"
                );
            }
        } else {
            return Err(anyhow::anyhow!("Schema missing /_meta"));
        }

        let schema = avro::shape_to_avro(schema.clone());

        Ok((
            schema.clone(),
            vec![(schema, CustomizableExtractor::RootExtractorWithIsDeleted)],
        ))
    } else {
        let schema = avro::shape_to_avro(schema.clone());

        Ok((
            schema.clone(),
            vec![(
                schema,
                CustomizableExtractor::Extractor(doc::Extractor::new(
                    doc::Pointer::empty(),
                    &doc::SerPolicy::noop(),
                )),
            )],
        ))
    }
}

#[async_trait]
pub trait JournalClientProvider: Send + Sync {
    async fn get_journal_client(&self) -> anyhow::Result<(journal::Client, proto_gazette::Claims)>;
}

pub struct TaskAuthClientProvider {
    partition_template_name: String,
    task_state_listener: TaskStateListener,
}

impl TaskAuthClientProvider {
    pub fn new(partition_template_name: String, task_state_listener: TaskStateListener) -> Self {
        Self {
            partition_template_name,
            task_state_listener,
        }
    }
}

#[async_trait]
impl JournalClientProvider for TaskAuthClientProvider {
    async fn get_journal_client(&self) -> anyhow::Result<(journal::Client, proto_gazette::Claims)> {
        let task_state = self
            .task_state_listener
            .get()
            .await
            .context("Failed to get task state from listener")?;

        let (client, claims, _) = task_state
            .partitions
            .iter()
            .find_map(|(k, v)| {
                if *k == self.partition_template_name {
                    Some(v)
                } else {
                    None
                }
            })
            .context(format!(
                "Partition template {} not found in task state",
                self.partition_template_name
            ))?
            .as_ref()
            .map_err(|e| {
                anyhow::anyhow!(
                    "Error accessing client for partition template {}: {}",
                    self.partition_template_name,
                    e
                )
            })?;

        Ok((client.clone(), claims.clone()))
    }
}

pub struct UserAuthClientProvider {
    client: Arc<FlowClient>,
    collection_name: String,
}

impl UserAuthClientProvider {
    pub fn new(client: Arc<FlowClient>, collection_name: String) -> Self {
        Self {
            client,
            collection_name,
        }
    }
}

#[async_trait]
impl JournalClientProvider for UserAuthClientProvider {
    async fn get_journal_client(&self) -> anyhow::Result<(journal::Client, proto_gazette::Claims)> {
        let (_, client, claims) =
            flow_client::fetch_user_collection_authorization(&self.client, &self.collection_name)
                .await?;

        Ok((client, claims))
    }
}
