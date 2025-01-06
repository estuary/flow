use crate::{
    connector::{DekafConfig, DekafResourceConfig, DeletionMode},
    dekaf_shard_template_id, App, SessionAuthentication, TaskAuth, UserAuth,
};
use anyhow::{anyhow, bail, Context};
use flow_client::fetch_task_authorization;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use gazette::{broker, journal, uuid};
use itertools::Itertools;
use models::RawValue;
use proto_flow::flow;
use std::{iter, time::Duration};

impl UserAuth {
    /// Fetch the names of all collections which the current user may read.
    /// Each is mapped into a kafka topic.
    pub async fn fetch_all_collection_names(&mut self) -> anyhow::Result<Vec<String>> {
        let client = self.authenticated_client().await?.pg_client();
        #[derive(serde::Deserialize)]
        struct Row {
            catalog_name: String,
        }
        let rows_builder = client
            .from("live_specs_ext")
            .eq("spec_type", "collection")
            .select("catalog_name");

        let items = flow_client::pagination::into_items::<Row>(rows_builder)
            .map(|res| res.map(|Row { catalog_name }| catalog_name))
            .try_collect()
            .await
            .context("listing current catalog specifications")?;

        Ok(items)
    }
}

impl TaskAuth {
    pub async fn fetch_all_collection_names(&self) -> anyhow::Result<Vec<String>> {
        Ok(self
            .built_spec
            .bindings
            .iter()
            .map(|b| {
                serde_json::from_str::<crate::connector::DekafResourceConfig>(
                    &b.resource_config_json,
                )
            })
            .map_ok(|val| val.topic_name)
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_binding_for_topic(
        &self,
        topic_name: &str,
    ) -> anyhow::Result<
        Option<(
            proto_flow::flow::materialization_spec::Binding,
            DekafResourceConfig,
        )>,
    > {
        Ok(self
            .built_spec
            .bindings
            .iter()
            .map(|b| {
                serde_json::from_str::<crate::connector::DekafResourceConfig>(
                    &b.resource_config_json,
                )
                .map(|parsed| (b, parsed))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .find(|(_, parsed_config)| parsed_config.topic_name == topic_name)
            .map(|(binding, config)| (binding.clone(), config)))
    }
}

impl SessionAuthentication {
    pub async fn fetch_all_collection_names(&mut self) -> anyhow::Result<Vec<String>> {
        match self {
            SessionAuthentication::User(auth) => auth.fetch_all_collection_names().await,
            SessionAuthentication::Task(auth) => auth.fetch_all_collection_names().await,
        }
    }

    pub fn get_collection_for_topic(&self, topic_name: &str) -> anyhow::Result<String> {
        match self {
            SessionAuthentication::User(_) => Ok(topic_name.to_string()),
            SessionAuthentication::Task(auth) => {
                let (binding, _resource_config) = auth
                    .get_binding_for_topic(topic_name)?
                    .ok_or(anyhow::anyhow!("Unrecognized topic {topic_name}"))?;

                Ok(binding
                    .collection
                    .context("missing collection in materialization binding")?
                    .name)
            }
        }
    }
}

/// Collection is the assembled metadata of a collection being accessed as a Kafka topic.
pub struct Collection {
    pub journal_client: journal::Client,
    pub key_ptr: Vec<doc::Pointer>,
    pub key_schema: avro::Schema,
    pub not_before: uuid::Clock,
    pub partitions: Vec<Partition>,
    pub spec: flow::CollectionSpec,
    pub uuid_ptr: doc::Pointer,
    pub value_schema: avro::Schema,
    pub projections: Vec<proto_flow::flow::Projection>,
}

/// Partition is a collection journal which is mapped into a stable Kafka partition order.
#[derive(Debug)]
pub struct Partition {
    pub create_revision: i64,
    pub spec: broker::JournalSpec,
    #[allow(unused)]
    pub mod_revision: i64,
    #[allow(unused)]
    pub route: broker::Route,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct PartitionOffset {
    pub fragment_start: i64,
    pub offset: i64,
    pub mod_time: i64,
}

impl Collection {
    /// Build a Collection by fetching its spec, a authenticated data-plane access token, and its partitions.
    pub async fn new(
        app: &App,
        auth: &SessionAuthentication,
        pg_client: &postgrest::Postgrest,
        topic_name: &str,
    ) -> anyhow::Result<Option<Self>> {
        let not_before = uuid::Clock::default();

        let binding = if let SessionAuthentication::Task(task_auth) = auth {
            if let Some((binding, _)) = task_auth.get_binding_for_topic(topic_name)? {
                Some(binding)
            } else if let Some(suggested_binding) = task_auth.built_spec.bindings.iter().find(|b| {
                b.collection
                    .as_ref()
                    .expect("missing collection in materialization binding")
                    .name
                    == topic_name
            }) {
                let correct_topic_name = serde_json::from_str::<
                    crate::connector::DekafResourceConfig,
                >(&suggested_binding.resource_config_json)?
                .topic_name;
                bail!(
                    "{topic_name} is not a binding of {}. Did you mean {}?",
                    task_auth.task_name,
                    correct_topic_name
                )
            } else {
                bail!("{topic_name} is not a binding of {}", task_auth.task_name)
            }
        } else {
            None
        };

        let collection_name = &auth.get_collection_for_topic(topic_name)?;

        let Some(collection_spec) = Self::fetch_spec(&pg_client, collection_name).await? else {
            return Ok(None);
        };
        let partition_template_name = collection_spec
            .partition_template
            .as_ref()
            .map(|spec| spec.name.to_owned())
            .ok_or(anyhow!("missing partition template"))?;

        let journal_client =
            Self::build_journal_client(app, &auth, collection_name, &partition_template_name)
                .await?;
        let partitions = Self::fetch_partitions(&journal_client, collection_name).await?;

        tracing::debug!(?partitions, "Got partitions");

        let key_ptr: Vec<doc::Pointer> = collection_spec
            .key
            .iter()
            .map(|p| doc::Pointer::from_str(p))
            .collect();
        let uuid_ptr = doc::Pointer::from_str(&collection_spec.uuid_ptr);

        let json_schema = if collection_spec.read_schema_json.is_empty() {
            &collection_spec.write_schema_json
        } else {
            &collection_spec.read_schema_json
        };

        let json_schema = doc::validation::build_bundle(json_schema)?;
        let validator = doc::Validator::new(json_schema)?;
        let collection_schema_shape =
            doc::Shape::infer(&validator.schemas()[0], validator.schema_index());

        // Create value shape by merging all projected fields in the schema
        let (field_selected_shape, projections) = if let Some(binding) = binding {
            let selection = binding
                .field_selection
                .context("missing field selection in materialization binding")?;

            build_field_selection_shape(
                collection_schema_shape.clone(),
                selection
                    .keys
                    .into_iter()
                    .chain(selection.values.into_iter())
                    .collect_vec(),
                collection_spec.projections.clone(),
            )?
        } else {
            (
                collection_schema_shape.clone(),
                collection_spec.projections.clone(),
            )
        };

        let field_selected_shape = if matches!(auth.deletions(), DeletionMode::CDC) {
            let nested_shape = build_shape_at_pointer(
                &doc::Pointer::from_str("/_meta/is_deleted"),
                &doc::Shape {
                    type_: json::schema::types::INTEGER,
                    ..doc::Shape::nothing()
                },
            );
            doc::Shape::intersect(field_selected_shape, nested_shape)
        } else {
            field_selected_shape
        };

        // Need to generate the key schema based on the collection schema,
        // whereas the value schema is the shape of documents after field-selection
        let key_schema = avro::key_to_avro(&key_ptr, collection_schema_shape);
        let value_schema = avro::shape_to_avro(field_selected_shape);

        tracing::debug!(
            collection_name,
            partitions = partitions.len(),
            "built collection"
        );

        Ok(Some(Self {
            journal_client,
            key_ptr,
            key_schema,
            not_before,
            partitions,
            spec: collection_spec,
            uuid_ptr,
            value_schema,
            projections,
        }))
    }

    /// Map the collection's key and value Avro schema into globally unique registry IDs.
    /// This will content-address each schema to fetch a current registry ID if one is available,
    /// or will register a new schema if not.
    pub async fn registered_schema_ids(
        &self,
        client: &postgrest::Postgrest,
    ) -> anyhow::Result<(u32, u32)> {
        let (key_id, value_id) = futures::try_join!(
            Self::registered_schema_id(client, &self.spec.name, &self.key_schema),
            Self::registered_schema_id(client, &self.spec.name, &self.value_schema),
        )?;
        Ok((key_id, value_id))
    }

    /// Fetch the built spec for a collection.
    async fn fetch_spec(
        client: &postgrest::Postgrest,
        collection: &str,
    ) -> anyhow::Result<Option<flow::CollectionSpec>> {
        #[derive(serde::Deserialize)]
        struct Row {
            built_spec: flow::CollectionSpec,
        }

        let mut rows: Vec<Row> = handle_postgrest_response(
            client
                .from("live_specs_ext")
                .eq("spec_type", "collection")
                .eq("catalog_name", collection)
                .select("built_spec"),
        )
        .await
        .context("listing current collection specifications")?;

        if let Some(Row { built_spec }) = rows.pop() {
            Ok(Some(built_spec))
        } else {
            Ok(None)
        }
    }

    /// Fetch the journals of a collection and map into stable-order partitions.
    #[tracing::instrument(skip(journal_client))]
    async fn fetch_partitions(
        journal_client: &journal::Client,
        collection: &str,
    ) -> anyhow::Result<Vec<Partition>> {
        let request = broker::ListRequest {
            selector: Some(broker::LabelSelector {
                include: Some(labels::build_set([(labels::COLLECTION, collection)])),
                exclude: None,
            }),
            ..Default::default()
        };

        let response = journal_client.list(request).await?;

        let mut partitions = Vec::with_capacity(response.journals.len());

        for journal in response.journals {
            partitions.push(Partition {
                create_revision: journal.create_revision,
                spec: journal.spec.context("expected journal Spec")?,
                mod_revision: journal.mod_revision,
                route: journal.route.context("expected journal Route")?,
            })
        }

        // Establish stability of exposed partition indices by ordering journals
        // by their created revision, and _then_ by their name.
        partitions.sort_by(|l, r| {
            (l.create_revision, &l.spec.name).cmp(&(r.create_revision, &r.spec.name))
        });

        Ok(partitions)
    }

    /// Map a partition and timestamp into the newest covering fragment offset.
    pub async fn fetch_partition_offset(
        &self,
        partition_index: usize,
        timestamp_millis: i64,
    ) -> anyhow::Result<Option<PartitionOffset>> {
        let Some(partition) = self.partitions.get(partition_index) else {
            return Ok(None);
        };
        let (not_before_sec, _) = self.not_before.to_unix();

        let begin_mod_time = if timestamp_millis == -1 {
            i64::MAX // Sentinel for "largest available offset",
        } else if timestamp_millis == -2 {
            0 // Sentinel for "first available offset"
        } else {
            let timestamp = timestamp_millis / 1_000;
            if timestamp < not_before_sec as i64 {
                not_before_sec as i64
            } else {
                timestamp as i64
            }
        };

        let request = broker::FragmentsRequest {
            journal: partition.spec.name.clone(),
            begin_mod_time,
            page_limit: 1,
            ..Default::default()
        };
        let response = self.journal_client.list_fragments(request).await?;

        let offset_data = match response.fragments.get(0) {
            Some(broker::fragments_response::Fragment {
                spec: Some(spec), ..
            }) => {
                if timestamp_millis == -1 {
                    PartitionOffset {
                        fragment_start: spec.begin,
                        // Subtract one to reflect the largest fetch-able offset of the fragment.
                        offset: spec.end - 1,
                        mod_time: spec.mod_time,
                    }
                } else {
                    PartitionOffset {
                        fragment_start: spec.begin,
                        offset: spec.begin,
                        mod_time: spec.mod_time,
                    }
                }
            }
            _ => PartitionOffset::default(),
        };

        tracing::debug!(
            collection = self.spec.name,
            ?offset_data,
            partition_index,
            timestamp_millis,
            "fetched offset"
        );

        Ok(Some(offset_data))
    }

    /// Build a journal client by resolving the collections data-plane gateway and an access token.
    async fn build_journal_client(
        app: &App,
        auth: &SessionAuthentication,
        collection_name: &str,
        partition_template_name: &str,
    ) -> anyhow::Result<journal::Client> {
        match auth {
            SessionAuthentication::User(user_auth) => {
                let (_, journal_client) = flow_client::fetch_user_collection_authorization(
                    &user_auth.client,
                    collection_name,
                )
                .await?;

                Ok(journal_client)
            }
            SessionAuthentication::Task(task_auth) => {
                let journal_client = tokio::time::timeout(
                    Duration::from_secs(30),
                    fetch_task_authorization(
                        &app.client_base,
                        &dekaf_shard_template_id(&task_auth.task_name),
                        &app.data_plane_fqdn,
                        &app.data_plane_signer,
                        proto_flow::capability::AUTHORIZE
                            | proto_gazette::capability::LIST
                            | proto_gazette::capability::READ,
                        gazette::broker::LabelSelector {
                            include: Some(labels::build_set([(
                                "name:prefix",
                                format!("{partition_template_name}/").as_str(),
                            )])),
                            exclude: None,
                        },
                    ),
                )
                .map_err(|e| {
                    anyhow::anyhow!("timed out building journal client for {collection_name}: {e}")
                })
                .await??;

                Ok(journal_client)
            }
        }
    }

    async fn registered_schema_id(
        client: &postgrest::Postgrest,
        catalog_name: &str,
        schema: &avro::Schema,
    ) -> anyhow::Result<u32> {
        #[derive(serde::Deserialize)]
        struct Row {
            registry_id: u32,
        }

        // Note the canonical form of the schema strips away some important metadata
        // that we require while encoding, such as default values.
        // It's fully sufficient for readers, though.
        // We map into a serde_json::Value to ensure stability of property order when content-summing.
        let schema: serde_json::Value = serde_json::from_str(&schema.canonical_form()).unwrap();
        let schema_md5 = format!("{:x}", md5::compute(&schema.to_string()));

        let mut rows: Vec<Row> = handle_postgrest_response(
            client
                .from("registered_avro_schemas")
                .eq("avro_schema_md5", &schema_md5)
                .select("registry_id"),
        )
        .await
        .context("querying for an already-registered schema")?;

        if let Some(Row { registry_id }) = rows.pop() {
            return Ok(registry_id);
        }

        let mut rows: Vec<Row> = handle_postgrest_response(
            client.from("registered_avro_schemas").insert(
                serde_json::json!([{
                    "avro_schema": schema,
                    "catalog_name": catalog_name,
                }])
                .to_string(),
            ),
        )
        .await
        .context("inserting new registered schema")?;

        let registry_id = rows.pop().unwrap().registry_id;
        tracing::info!(schema_md5, registry_id, "registered new Avro schema");

        Ok(registry_id)
    }
}

async fn handle_postgrest_response<T: serde::de::DeserializeOwned>(
    builder: postgrest::Builder,
) -> anyhow::Result<T> {
    let resp = builder.execute().await?;
    let status = resp.status();

    if status.is_client_error() || status.is_server_error() {
        bail!(
            "{}: {}",
            status.canonical_reason().unwrap_or(status.as_str()),
            resp.text().await?
        )
    } else {
        Ok(resp.json().await?)
    }
}

// Claims returned by `/authorize/dekaf`
#[derive(Debug, Clone, serde::Deserialize)]
pub struct AccessTokenClaims {
    pub iat: u64,
    pub exp: u64,
}
#[tracing::instrument(skip(client, data_plane_signer), err)]
pub async fn fetch_dekaf_task_auth(
    client: flow_client::Client,
    shard_template_id: &str,
    data_plane_fqdn: &str,
    data_plane_signer: &jsonwebtoken::EncodingKey,
) -> anyhow::Result<(
    flow_client::Client,
    AccessTokenClaims,
    String,
    String,
    proto_flow::flow::MaterializationSpec,
)> {
    let request_token = flow_client::client::build_task_authorization_request_token(
        shard_template_id,
        data_plane_fqdn,
        data_plane_signer,
        proto_flow::capability::AUTHORIZE,
        Default::default(),
    )?;
    let models::authorizations::DekafAuthResponse {
        token,
        ops_logs_journal,
        ops_stats_journal,
        task_spec,
        retry_millis: _,
    } = loop {
        let response: models::authorizations::DekafAuthResponse = client
            .agent_unary(
                "/authorize/dekaf",
                &models::authorizations::TaskAuthorizationRequest {
                    token: request_token.clone(),
                },
            )
            .await?;
        if response.retry_millis != 0 {
            tracing::warn!(
                secs = response.retry_millis as f64 / 1000.0,
                "authorization service tentatively rejected our request, but will retry before failing"
            );
            () = tokio::time::sleep(std::time::Duration::from_millis(response.retry_millis)).await;
            continue;
        }
        break response;
    };
    let claims = flow_client::parse_jwt_claims(token.as_str())?;

    Ok((
        client.with_user_access_token(Some(token)),
        claims,
        ops_logs_journal,
        ops_stats_journal,
        serde_json::from_str(
            task_spec
                .ok_or(anyhow::anyhow!(
                    "task_spec is only None when we need to retry the auth request"
                ))?
                .get(),
        )?,
    ))
}

pub async fn extract_dekaf_config(
    spec: &proto_flow::flow::MaterializationSpec,
) -> anyhow::Result<DekafConfig> {
    let config = serde_json::from_str::<models::DekafConfig>(&spec.config_json)?;

    let decrypted_endpoint_config =
        unseal::decrypt_sops(&RawValue::from_str(&config.config.to_string())?).await?;

    let dekaf_config = serde_json::from_str::<DekafConfig>(&decrypted_endpoint_config.to_string())?;
    Ok(dekaf_config)
}

/// Nests the provided shape under a JSON pointer path by creating the necessary object hierarchy.
/// For example, given pointer "/a/b/c" and a field shape, creates an object structure:
/// { "a": { "b": { "c": field_shape } } }
fn build_shape_at_pointer(ptr: &doc::Pointer, shape: &doc::Shape) -> doc::Shape {
    // Return the original shape if pointer is empty
    if ptr.0.is_empty() {
        return shape.clone();
    }

    tracing::debug!(?ptr, "Building shape");
    let mut current_shape = doc::Shape::nothing();
    let mut current = &mut current_shape;

    // For each component in the pointer path except the last one,
    // create the object structure
    for token in ptr.iter().take(ptr.0.len() - 1) {
        match token {
            doc::ptr::Token::Property(name) => {
                let mut obj = doc::Shape::nothing();
                obj.type_ = json::schema::types::OBJECT;

                current.type_ = json::schema::types::OBJECT;
                current.object.properties.push(doc::shape::ObjProperty {
                    name: Box::from(name.as_str()),
                    is_required: true,
                    shape: obj,
                });

                // Move to the newly created object
                current = &mut current.object.properties.last_mut().unwrap().shape;
            }
            doc::ptr::Token::Index(_) => {
                // Create an array shape with the next level nested inside
                let mut array = doc::Shape::nothing();
                array.type_ = json::schema::types::ARRAY;
                array.array.additional_items = Some(Box::new(doc::Shape::nothing()));

                current.type_ = json::schema::types::ARRAY;
                *current = array;

                // Move to the array items shape for the next iteration
                current = current.array.additional_items.as_mut().unwrap();
            }
            _ => unreachable!("NextIndex/NextProperty shouldn't appear in concrete pointers"),
        }
    }

    // Add the actual field shape at the final position
    if let Some(doc::ptr::Token::Property(name)) = ptr.iter().last() {
        current.type_ = json::schema::types::OBJECT;
        current.object.properties.push(doc::shape::ObjProperty {
            name: Box::from(name.as_str()),
            is_required: true,
            shape: shape.clone(),
        });
    }

    current_shape
}

fn build_field_selection_shape(
    source_shape: doc::Shape,
    fields: Vec<String>,
    projections: Vec<flow::Projection>,
) -> anyhow::Result<(doc::Shape, Vec<flow::Projection>)> {
    let selected_projections = fields
        .iter()
        .filter(|f| f.len() > 0)
        .map(|field| {
            let projection = projections.iter().find(|proj| proj.field == *field);
            if let Some(projection) = projection {
                Some(projection.clone())
            } else {
                tracing::warn!(
                    ?field,
                    "Missing projection for field on materialization built spec"
                );
                None
            }
        })
        .flatten(); // transform from Option<T> to T by filtering out Nones

    let mut starting_shape = doc::Shape::nothing();
    starting_shape.type_ = json::schema::types::OBJECT;

    let mapped_shape =
        selected_projections
            .clone()
            .fold(starting_shape, |value_shape, projection| {
                let source_ptr = doc::Pointer::from_str(&projection.ptr);
                let (source_shape, exists) = source_shape.locate(&source_ptr);
                if exists.cannot() {
                    tracing::warn!(
                        projection = ?source_ptr,
                        "Projection field not found in schema"
                    );
                    value_shape
                } else {
                    let nested_shape = build_shape_at_pointer(
                        &doc::Pointer::from_str(&format!("/{}", projection.field)),
                        source_shape,
                    );
                    doc::Shape::intersect(value_shape, nested_shape)
                }
            });

    Ok((mapped_shape, selected_projections.collect_vec()))
}
