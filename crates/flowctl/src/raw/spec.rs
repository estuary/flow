use crate::{local_connector, local_specs};
use anyhow::Context;
use proto_flow::{capture, flow, materialize};
use tables::DraftCatalog;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Spec {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Name of the task to request the spec for within the Flow specification file.
    /// Required if there are multiple tasks in --source specifications.
    #[clap(long, alias = "name")]
    task: Option<String>,
}

pub async fn do_spec(
    ctx: &mut crate::CliContext,
    Spec { source, task }: &Spec,
) -> anyhow::Result<()> {
    let source = build::arg_source_to_url(source, false)?;
    let mut draft = local_specs::surface_errors(local_specs::load(&source).await.into_result())?;
    let task = local_specs::select_task(&mut draft, task.as_deref())?;

    let router = ctx.local_connector_router();
    let serialized = get_spec_response(&task, &draft, &*router).await?;
    println!("{}", serialized);

    Ok(())
}

async fn get_spec_response(
    name: &str,
    draft: &DraftCatalog,
    router: &dyn proto_grpc::connector::Router,
) -> anyhow::Result<String> {
    for row in draft.captures.iter() {
        if name != row.capture.as_str() {
            continue;
        }

        let model = row.model.as_ref().expect("not a capture");

        let request = match &model.endpoint {
            models::CaptureEndpoint::Connector(config) => capture::request::Spec {
                connector_type: flow::capture_spec::ConnectorType::Image as i32,
                config_json: serde_json::to_string(&config).unwrap().into(),
            },
            models::CaptureEndpoint::Local(config) => capture::request::Spec {
                connector_type: flow::capture_spec::ConnectorType::Local as i32,
                config_json: serde_json::to_string(config).unwrap().into(),
            },
        };
        let response =
            local_connector::spec_capture(router, model.shards.log_level.as_deref(), request)
                .await?;

        return serde_json::to_string(&response).context("Failed to serialize spec response");
    }

    for row in draft.collections.iter() {
        if name != row.collection.as_str() {
            continue;
        }

        let model = row
            .model
            .as_ref()
            .and_then(|m| m.derive.as_ref())
            .expect("not a derivation");

        let request = validation::derive_spec_request(&model.using, &model.shards);
        let response =
            local_connector::spec_derive(router, model.shards.log_level.as_deref(), request)
                .await?;

        return serde_json::to_string(&response).context("Failed to serialize spec response");
    }

    for row in draft.materializations.iter() {
        if name != row.materialization.as_str() {
            continue;
        }

        let model = row.model.as_ref().expect("not a materialization");

        let request = match &model.endpoint {
            models::MaterializationEndpoint::Connector(config) => materialize::request::Spec {
                connector_type: flow::materialization_spec::ConnectorType::Image as i32,
                config_json: serde_json::to_string(&config).unwrap().into(),
            },
            models::MaterializationEndpoint::Local(config) => materialize::request::Spec {
                connector_type: flow::materialization_spec::ConnectorType::Local as i32,
                config_json: serde_json::to_string(config).unwrap().into(),
            },
            models::MaterializationEndpoint::Dekaf(config) => materialize::request::Spec {
                connector_type: flow::materialization_spec::ConnectorType::Dekaf as i32,
                config_json: serde_json::to_string(config).unwrap().into(),
            },
        };
        let response =
            local_connector::spec_materialize(router, model.shards.log_level.as_deref(), request)
                .await?;

        return serde_json::to_string(&response).context("Failed to serialize spec response");
    }

    unreachable!("`select_task` found task {name}");
}
