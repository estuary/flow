use anyhow::Context;
use control_plane_api::{
    connector_tags::{self, Row, fetch_connector_tag, resolve},
    connectors::ConnectorFactory,
};
use models::Id;
use proto_flow::{connector, flow};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tables::utils::pointer_for_schema;
use tracing::info;

/// JobStatus is the possible outcomes of a handled connector tag.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    SpecFailed,
    ValidationFailed { error: ValidationError },
    Success,
    // Updating is a temporary state that means we're in the process of updating
    // the connector tags table. This exists because the connector tags table
    // has a trigger that will create an `internal.tasks` row whenever the
    // `job_status->>'type' = 'queued'`. So we temporarily set the job status to
    // `updating` until we're done and know the final status.
    Updating,
    InternalError,
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
pub struct TagExecutor {
    connector_factory: std::sync::Arc<dyn ConnectorFactory>,
    snapshot_watch: std::sync::Arc<dyn tokens::Watch<control_plane_api::Snapshot>>,
}

impl TagExecutor {
    pub fn new(
        connector_factory: std::sync::Arc<dyn ConnectorFactory>,
        snapshot_watch: std::sync::Arc<dyn tokens::Watch<control_plane_api::Snapshot>>,
    ) -> Self {
        Self {
            connector_factory,
            snapshot_watch,
        }
    }
}

pub struct TagOutcome {
    id: Id,
    status: JobStatus,
}

impl automations::Outcome for TagOutcome {
    async fn apply<'s>(
        self,
        txn: &'s mut sqlx::PgConnection,
    ) -> anyhow::Result<automations::Action> {
        resolve(self.id, self.status, txn).await?;
        Ok(automations::Action::Done)
    }
}

impl automations::Executor for TagExecutor {
    const TASK_TYPE: automations::TaskType = automations::task_types::CONNECTOR_TAGS;
    type Receive = serde_json::Value;
    type State = ();
    type Outcome = TagOutcome;

    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        task_id: models::Id,
        _parent_id: Option<models::Id>,
        _state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> anyhow::Result<Self::Outcome> {
        let row = fetch_connector_tag(task_id, pool).await?;
        tracing::debug!(?inbox, %task_id, "processing connector_tags task");
        let time_queued = chrono::Utc::now().signed_duration_since(row.updated_at);
        let next_status = self.process(row, pool).await?;

        info!(%time_queued, id = %task_id, status = ?next_status, "finished");
        inbox.clear();
        Ok(TagOutcome {
            id: task_id,
            status: next_status,
        })
    }
}

impl TagExecutor {
    #[tracing::instrument(err, skip_all, fields(id=?row.tag_id))]
    async fn process(&self, row: Row, pool: &sqlx::PgPool) -> anyhow::Result<JobStatus> {
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
                return Ok(JobStatus::ValidationFailed {
                    error: ValidationError::InvalidDekafTag,
                });
            }
        }

        // Spin awaiting a Snapshot that includes a ready public data-plane.
        // (This spin is meaningful for local-stack startup, not in production).
        let mut refresh: std::sync::Arc<tokens::Refresh<control_plane_api::Snapshot>>;
        let data_plane_id = loop {
            refresh = self.snapshot_watch.token();
            let snapshot = refresh.result().expect("Snapshot refresh never fails");

            if let Some(data_plane_id) = snapshot
                .data_planes()
                .find(|plane| {
                    plane.data_plane_name.starts_with("ops/dp/public/") && plane.can_sign()
                })
                .map(|plane| plane.control_id)
            {
                break data_plane_id;
            }

            snapshot.request_refresh();
            tracing::info!("awaiting a Snapshot with a ready public data-plane");
            _ = refresh.expired().await;
        };

        // Call out to the named public plane to fulfill the Spec request.
        let connectors = self.connector_factory.make_connectors(
            refresh.result().unwrap(),
            "spec",
            row.logs_token,
        );

        let protocols = sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT protocol FROM connector_tags WHERE connector_id = $1 AND protocol IS NOT NULL ORDER BY protocol",
        )
        .bind(row.connector_id)
        .fetch_all(pool)
        .await?;

        let protocol = if row.image_name.starts_with(models::DEKAF_IMAGE_NAME_PREFIX) {
            Some(ConnectorProtocol::Materialization)
        } else {
            match protocols.as_slice() {
                [] => None,
                [protocol] if protocol == "capture" => Some(ConnectorProtocol::Capture),
                [protocol] if protocol == "materialization" => {
                    Some(ConnectorProtocol::Materialization)
                }
                protocols => {
                    tracing::warn!(?protocols, "connector has invalid or conflicting protocols");
                    return Ok(JobStatus::InternalError);
                }
            }
        };

        let spec_result = match protocol {
            Some(ConnectorProtocol::Capture) => {
                spec_capture(&image_composed, connectors.as_ref(), data_plane_id)
                    .await
                    .map(|spec| (ConnectorProtocol::Capture, spec))
            }
            Some(ConnectorProtocol::Materialization) => {
                spec_materialization(&image_composed, connectors.as_ref(), data_plane_id)
                    .await
                    .map(|spec| (ConnectorProtocol::Materialization, spec))
            }
            None => match spec_capture(&image_composed, connectors.as_ref(), data_plane_id).await {
                Ok(spec) => Ok((ConnectorProtocol::Capture, spec)),
                Err(capture_err) => {
                    match spec_materialization(&image_composed, connectors.as_ref(), data_plane_id)
                        .await
                    {
                        Ok(spec) => Ok((ConnectorProtocol::Materialization, spec)),
                        Err(_) if is_retryable(&capture_err) => Err(capture_err),
                        Err(materialization_err) => Err(materialization_err),
                    }
                }
            },
        };

        let (proto_type, spec) = match spec_result {
            Ok(spec) => spec,
            Err(err) if is_retryable(&err) => return Err(err),
            Err(err) => {
                tracing::warn!(error = ?err, image = %image_composed, "connector Spec RPC failed");
                return Ok(JobStatus::SpecFailed);
            }
        };

        let ConnectorSpec {
            endpoint_config_schema,
            resource_config_schema,
            documentation_url,
            oauth2,
            resource_path_pointers,
        } = spec;

        if proto_type == ConnectorProtocol::Capture {
            tracing::info!(
                image = %image_composed,
                included = %!resource_path_pointers.is_empty(),
                "does capture spec response include resource_path_pointers"
            );
        }

        // Validate that there is an x-collection-name annotation in the resource config schema
        // of materialization connectors
        if proto_type == ConnectorProtocol::Materialization {
            if let Err(err) = pointer_for_schema(resource_config_schema.get()) {
                tracing::warn!(image = %image_composed, error = %err, "resource schema does not have x-collection-name annotation");
                return Ok(JobStatus::SpecFailed);
            }
        }

        // The tag fields may not be updated if the resource_path_pointers have
        // changed. If that happens, then we bail without making any changes
        // other than to job_status.
        let tag_updated = connector_tags::update_tag_fields(
            row.tag_id,
            documentation_url,
            endpoint_config_schema.into(),
            proto_type.database_string_value().to_string(),
            resource_config_schema.into(),
            resource_path_pointers.clone(),
            pool,
        )
        .await?;
        if !tag_updated {
            return Ok(JobStatus::resource_path_pointers_changed(
                resource_path_pointers,
            ));
        }

        if let Some(oauth2) = oauth2 {
            connector_tags::update_oauth2_spec(row.connector_id, oauth2.into(), pool).await?;
        }

        return Ok(JobStatus::Success);
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ConnectorProtocol {
    Capture,
    Materialization,
}

impl ConnectorProtocol {
    fn database_string_value(self) -> &'static str {
        match self {
            Self::Capture => "capture",
            Self::Materialization => "materialization",
        }
    }
}

fn is_retryable(err: &anyhow::Error) -> bool {
    if err.downcast_ref::<tokio::time::error::Elapsed>().is_some() {
        return true;
    }
    matches!(
        err.downcast_ref::<proto_grpc::StatusError>()
            .map(|err| err.code()),
        Some(tonic::Code::Unavailable | tonic::Code::DeadlineExceeded)
    )
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
    connectors: &validation::Connectors<'_>,
    data_plane_id: models::Id,
) -> anyhow::Result<ConnectorSpec> {
    use proto_flow::materialize;

    let (connector_type, config_json) = if image.starts_with(models::DEKAF_IMAGE_NAME_PREFIX) {
        let variant = &image
            [models::DEKAF_IMAGE_NAME_PREFIX.len()..image.len() - models::DEKAF_IMAGE_TAG.len()];

        (
            flow::materialization_spec::ConnectorType::Dekaf as i32,
            serde_json::to_string(
                &serde_json::json!({"variant": variant.to_string(), "config": {}}),
            )
            .unwrap()
            .into(),
        )
    } else {
        (
            flow::materialization_spec::ConnectorType::Image as i32,
            serde_json::to_string(&serde_json::json!({"image": image, "config": {}}))
                .unwrap()
                .into(),
        )
    };

    let req = connector::Request {
        start: Some(connector::request::Start::default()),
        kind: Some(connector::request::Kind::Materialize(
            materialize::Request {
                kind: Some(materialize::request::Kind::Spec(
                    materialize::request::Spec {
                        connector_type,
                        config_json,
                    },
                )),
                ..Default::default()
            },
        )),
    };

    let (started, response) = connectors(data_plane_id, req).await?;
    let (
        Some(connector::response::started::Spec::Materialize(spec)),
        connector::response::Kind::Materialize(materialize::Response {
            kind: Some(materialize::response::Kind::Spec(_)),
            ..
        }),
    ) = (started.spec, response)
    else {
        anyhow::bail!("connector didn't return materialization Spec");
    };

    let materialize::response::Spec {
        protocol: _,
        config_schema_json,
        resource_config_schema_json,
        documentation_url,
        oauth2,
    } = *spec;

    let oauth2 = if let Some(oa) = oauth2 {
        Some(serde_json::value::to_raw_value(&oa).expect("serializing oauth2 config"))
    } else {
        None
    };

    Ok(ConnectorSpec {
        documentation_url,
        endpoint_config_schema: serde_json::from_slice(&config_schema_json)
            .context("parsing endpoint config schema")?,
        resource_config_schema: serde_json::from_slice(&resource_config_schema_json)
            .context("parsing resource config schema")?,
        resource_path_pointers: Vec::new(),
        oauth2,
    })
}

async fn spec_capture(
    image: &str,
    connectors: &validation::Connectors<'_>,
    data_plane_id: models::Id,
) -> anyhow::Result<ConnectorSpec> {
    use proto_flow::capture;
    let req = connector::Request {
        start: Some(connector::request::Start::default()),
        kind: Some(connector::request::Kind::Capture(capture::Request {
            kind: Some(capture::request::Kind::Spec(capture::request::Spec {
                connector_type: flow::capture_spec::ConnectorType::Image as i32,
                config_json: serde_json::json!({"image": image, "config": {}})
                    .to_string()
                    .into(),
            })),
            ..Default::default()
        })),
    };

    let (started, response) = connectors(data_plane_id, req).await?;
    let (
        Some(connector::response::started::Spec::Capture(spec)),
        connector::response::Kind::Capture(capture::Response {
            kind: Some(capture::response::Kind::Spec(_)),
            ..
        }),
    ) = (started.spec, response)
    else {
        anyhow::bail!("connector didn't return capture Spec");
    };

    let capture::response::Spec {
        // protocol here is the numeric version of the capture protocol
        protocol: _,
        config_schema_json,
        resource_config_schema_json,
        documentation_url,
        oauth2,
        resource_path_pointers,
    } = *spec;

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
        endpoint_config_schema: serde_json::from_slice(&config_schema_json)
            .context("parsing endpoint config schema")?,
        resource_config_schema: serde_json::from_slice(&resource_config_schema_json)
            .context("parsing resource config schema")?,
        resource_path_pointers,
        oauth2: oauth,
    })
}
