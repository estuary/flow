use super::{jobs, logs, HandleResult, Handler, Id};
use agent_sql::connector_tags::Row;
use anyhow::Context;
use proto_flow::flow;
use runtime::{LogHandler, Runtime, RuntimeProtocol};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tables::utils::pointer_for_schema;
use tracing::info;

/// JobStatus is the possible outcomes of a handled connector tag.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    PullFailed,
    SpecFailed,
    OpenGraphFailed { error: String },
    ValidationFailed { error: ValidationError },
    Success,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum ValidationError {
    ResourcePathPointersChanged { rejected: Vec<String> },
    InvalidDekafTag,
}

impl JobStatus {
    fn resource_path_pointers_changed(rejected: Vec<String>) -> JobStatus {
        JobStatus::ValidationFailed {
            error: ValidationError::ResourcePathPointersChanged { rejected },
        }
    }
}

/// A TagHandler is a Handler which evaluates tagged connector images.
pub struct TagHandler {
    connector_network: String,
    logs_tx: logs::Tx,
    allow_local: bool,
}

impl TagHandler {
    pub fn new(connector_network: &str, logs_tx: &logs::Tx, allow_local: bool) -> Self {
        Self {
            connector_network: connector_network.to_string(),
            logs_tx: logs_tx.clone(),
            allow_local,
        }
    }
}

#[async_trait::async_trait]
impl Handler for TagHandler {
    async fn handle(
        &mut self,
        pg_pool: &sqlx::PgPool,
        allow_background: bool,
    ) -> anyhow::Result<HandleResult> {
        let mut txn = pg_pool.begin().await?;

        let row: Row = match agent_sql::connector_tags::dequeue(&mut txn, allow_background).await? {
            None => return Ok(HandleResult::NoJobs),
            Some(row) => row,
        };

        let time_queued = chrono::Utc::now().signed_duration_since(row.updated_at);
        let (id, status) = self.process(row, &mut txn).await?;
        info!(%id, %time_queued, ?status, "finished");

        agent_sql::connector_tags::resolve(id, status, &mut txn).await?;
        txn.commit().await?;

        Ok(HandleResult::HadJob)
    }

    fn table_name(&self) -> &'static str {
        "connector_tags"
    }
}

/// This tag is used for local development of connectors. Any images having this tag will not be
/// pulled from a registry, so that developers can simply `docker build` and then update
/// connector_tags without having to push to a registry.
pub const LOCAL_IMAGE_TAG: &str = ":local";

impl TagHandler {
    #[tracing::instrument(err, skip_all, fields(id=?row.tag_id))]
    async fn process(
        &mut self,
        row: Row,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<(Id, JobStatus)> {
        info!(
            %row.image_name,
            %row.created_at,
            %row.image_tag,
            %row.logs_token,
            %row.updated_at,
            "processing connector image tag",
        );
        let image_composed = format!("{}{}", row.image_name, row.image_tag);

        // A Dekaf connector's tag is meaningless since it'll never get pulled, _except_ that
        // it must later on match the value in `live_specs.connector_image_tag`. Since we hard-code
        // that to the value of DEKAF_IMAGE_TAG, we must also ensure that no dekaf `connector_tags` rows
        // get inserted with a different image_tag value.
        if row.image_name.starts_with(models::DEKAF_IMAGE_NAME_PREFIX) {
            if row.image_tag != models::DEKAF_IMAGE_TAG {
                return Ok((
                    row.tag_id,
                    JobStatus::ValidationFailed {
                        error: ValidationError::InvalidDekafTag,
                    },
                ));
            }
        }

        if row.image_tag != LOCAL_IMAGE_TAG
            && !row.image_name.starts_with(models::DEKAF_IMAGE_NAME_PREFIX)
        {
            // Pull the image.
            let pull = jobs::run(
                "pull",
                &self.logs_tx,
                row.logs_token,
                async_process::Command::new("docker")
                    .arg("pull")
                    .arg(&image_composed),
            )
            .await?;

            if !pull.success() {
                return Ok((row.tag_id, JobStatus::PullFailed));
            }
        }

        let proto_type = match runtime::flow_runtime_protocol(&image_composed).await {
            Ok(ct) => ct,
            Err(err) => {
                tracing::warn!(image = %image_composed, error = %err, "failed to determine connector protocol");
                return Ok((row.tag_id, JobStatus::SpecFailed));
            }
        };

        let log_handler =
            logs::ops_handler(self.logs_tx.clone(), "spec".to_string(), row.logs_token);

        let runtime = Runtime::new(
            self.allow_local,
            self.connector_network.clone(),
            log_handler,
            None, // no need to change log level
            "ops/connector-tags-job".to_string(),
        );

        let spec_result = match proto_type {
            RuntimeProtocol::Capture => spec_capture(&image_composed, runtime).await,
            RuntimeProtocol::Materialize => spec_materialization(&image_composed, runtime).await,
            RuntimeProtocol::Derive => {
                tracing::warn!(image = %image_composed, "unhandled Spec RPC for derivation connector image");
                return Ok((row.tag_id, JobStatus::SpecFailed));
            }
        };

        let spec = match spec_result {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(error = ?err, image = %image_composed, "connector Spec RPC failed");
                return Ok((row.tag_id, JobStatus::SpecFailed));
            }
        };

        let ConnectorSpec {
            endpoint_config_schema,
            resource_config_schema,
            documentation_url,
            oauth2,
            resource_path_pointers,
        } = spec;

        if proto_type == RuntimeProtocol::Capture && resource_path_pointers.is_empty() {
            tracing::warn!(image = %image_composed, "capture connector spec omits resource_path_pointers");
        }

        // Validate that there is an x-collection-name annotation in the resource config schema
        // of materialization connectors
        if proto_type == RuntimeProtocol::Materialize {
            if let Err(err) = pointer_for_schema(resource_config_schema.get()) {
                tracing::warn!(image = %image_composed, error = %err, "resource schema does not have x-collection-name annotation");
                return Ok((row.tag_id, JobStatus::SpecFailed));
            }
        }

        // The tag fields may not be updated if the resource_path_pointers have
        // changed. If that happens, then we bail without making any changes
        // other than to job_status.
        let tag_updated = agent_sql::connector_tags::update_tag_fields(
            row.tag_id,
            documentation_url,
            endpoint_config_schema.into(),
            proto_type.database_string_value().to_string(),
            resource_config_schema.into(),
            resource_path_pointers.clone(),
            txn,
        )
        .await?;
        if !tag_updated {
            return Ok((
                row.tag_id,
                JobStatus::resource_path_pointers_changed(resource_path_pointers),
            ));
        }

        if let Some(oauth2) = oauth2 {
            agent_sql::connector_tags::update_oauth2_spec(row.connector_id, oauth2.into(), txn)
                .await?;
        }

        return Ok((row.tag_id, JobStatus::Success));
    }
}

// TODO(phil): maybe unify this with the controlplane::ConnectorSpec?
struct ConnectorSpec {
    documentation_url: String,
    endpoint_config_schema: Box<RawValue>,
    resource_config_schema: Box<RawValue>,
    resource_path_pointers: Vec<String>,
    oauth2: Option<Box<RawValue>>,
}

async fn spec_materialization(
    image: &str,
    runtime: Runtime<impl LogHandler>,
) -> anyhow::Result<ConnectorSpec> {
    use proto_flow::materialize;

    let connector_type = if image.starts_with(models::DEKAF_IMAGE_NAME_PREFIX) {
        flow::materialization_spec::ConnectorType::Dekaf as i32
    } else {
        flow::materialization_spec::ConnectorType::Image as i32
    };

    let req = materialize::Request {
        spec: Some(materialize::request::Spec {
            connector_type,
            config_json: serde_json::to_string(&serde_json::json!({"image": image, "config":{}}))
                .unwrap(),
        }),
        ..Default::default()
    };

    // TODO(johnny): select a data-plane and use ProxyConnectors.
    let spec = runtime
        .unary_materialize(req)
        .await?
        .spec
        .ok_or_else(|| anyhow::anyhow!("connector didn't send expected Spec response"))?;

    let materialize::response::Spec {
        protocol: _,
        config_schema_json,
        resource_config_schema_json,
        documentation_url,
        oauth2,
    } = spec;

    let oauth = if let Some(oa) = oauth2 {
        Some(serde_json::value::to_raw_value(&oa).expect("serializing oauth2 config"))
    } else {
        None
    };
    Ok(ConnectorSpec {
        documentation_url,
        endpoint_config_schema: RawValue::from_string(config_schema_json)
            .context("parsing endpoint config schema")?,
        resource_config_schema: RawValue::from_string(resource_config_schema_json)
            .context("parsing resource config schema")?,

        // materialization connectors don't currently specify resrouce_path_pointers, though perhaps they should
        resource_path_pointers: Vec::new(),
        oauth2: oauth,
    })
}

async fn spec_capture(
    image: &str,
    runtime: Runtime<impl LogHandler>,
) -> anyhow::Result<ConnectorSpec> {
    use proto_flow::capture;
    let req = capture::Request {
        spec: Some(capture::request::Spec {
            connector_type: flow::capture_spec::ConnectorType::Image as i32,
            config_json: serde_json::to_string(&serde_json::json!({"image": image, "config": {}}))
                .unwrap(),
        }),
        ..Default::default()
    };

    // TODO(johnny): select a data-plane and use ProxyConnectors.
    let spec = runtime
        .unary_capture(req)
        .await?
        .spec
        .ok_or_else(|| anyhow::anyhow!("connector didn't send expected Spec response"))?;

    let capture::response::Spec {
        // protocol here is the numeric version of the capture protocol
        protocol: _,
        config_schema_json,
        resource_config_schema_json,
        documentation_url,
        oauth2,
        resource_path_pointers,
    } = spec;

    let oauth = if let Some(oa) = oauth2 {
        Some(
            RawValue::from_string(serde_json::to_string(&oa).expect("can serialize oauth2 config"))
                .expect("serialization of oauth2 config cannot fail"),
        )
    } else {
        None
    };
    Ok(ConnectorSpec {
        documentation_url,
        endpoint_config_schema: RawValue::from_string(config_schema_json)
            .context("parsing endpoint config schema")?,
        resource_config_schema: RawValue::from_string(resource_config_schema_json)
            .context("parsing resource config schema")?,
        resource_path_pointers,
        oauth2: oauth,
    })
}
