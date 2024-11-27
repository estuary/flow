use crate::local_specs;
use anyhow::Context;
use proto_flow::{capture, flow, materialize};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Spec {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Name of the task (capture or materialization) to request the spec for.
    /// Required if there are multiple tasks in --source specifications.
    #[clap(long)]
    task: Option<String>,
    /// Docker network to run the connector, if one exists
    #[clap(long, default_value = "bridge")]
    network: String,
}

pub async fn do_spec(
    _ctx: &mut crate::CliContext,
    Spec {
        source,
        task,
        network,
    }: &Spec,
) -> anyhow::Result<()> {
    let source = build::arg_source_to_url(source, false)?;
    let draft = local_specs::surface_errors(local_specs::load(&source).await.into_result())?;

    // Identify the task to inspect.
    let needle = if let Some(needle) = task {
        needle.as_str()
    } else if draft.captures.len() + draft.materializations.len() > 1 {
        anyhow::bail!("sourced specification files contain multiple tasks. Use --task to identify a specific one");
    } else if draft.captures.is_empty() && draft.materializations.is_empty() {
        anyhow::bail!(
            "sourced specification files do not contain any captures or materializations"
        );
    } else if draft.captures.len() == 1 {
        draft.captures.first().unwrap().capture.as_str()
    } else {
        draft
            .materializations
            .first()
            .unwrap()
            .materialization
            .as_str()
    };

    let capture = match draft
        .captures
        .binary_search_by_key(&needle, |c| c.capture.as_str())
    {
        Ok(index) => Some(&draft.captures[index]),
        Err(_) => None,
    };

    let materialization = match draft
        .materializations
        .binary_search_by_key(&needle, |c| c.materialization.as_str())
    {
        Ok(index) => Some(&draft.materializations[index]),
        Err(_) => None,
    };

    let runtime = runtime::Runtime::new(
        true, // Allow local.
        network.clone(),
        ops::tracing_log_handler,
        None,
        format!("spec/{}", needle),
    );

    let serialized = match (capture, materialization) {
        (Some(capture), _) => {
            let model = capture.model.as_ref().expect("not a capture");

            let request = match &model.endpoint {
                models::CaptureEndpoint::Connector(config) => capture::request::Spec {
                    connector_type: flow::capture_spec::ConnectorType::Image as i32,
                    config_json: serde_json::to_string(&config).unwrap(),
                },
                models::CaptureEndpoint::Local(config) => capture::request::Spec {
                    connector_type: flow::capture_spec::ConnectorType::Local as i32,
                    config_json: serde_json::to_string(config).unwrap(),
                },
            };
            let request = capture::Request {
                spec: Some(request),
                ..Default::default()
            }
            .with_internal(|internal| {
                if let Some(s) = &model.shards.log_level {
                    internal.set_log_level(ops::LogLevel::from_str_name(s).unwrap_or_default());
                }
            });
            let response = runtime
                .unary_capture(request)
                .await?
                .spec
                .context("connector didn't send expected Spec response")?;

            serde_json::to_string(&response).context("Failed to serialize spec response")?
        }
        (_, Some(materialization)) => {
            let model = materialization
                .model
                .as_ref()
                .expect("not a materialization");

            let request = match &model.endpoint {
                models::MaterializationEndpoint::Connector(config) => materialize::request::Spec {
                    connector_type: flow::materialization_spec::ConnectorType::Image as i32,
                    config_json: serde_json::to_string(&config).unwrap(),
                },
                models::MaterializationEndpoint::Local(config) => materialize::request::Spec {
                    connector_type: flow::materialization_spec::ConnectorType::Local as i32,
                    config_json: serde_json::to_string(config).unwrap(),
                },
                models::MaterializationEndpoint::Dekaf(_) => {
                    anyhow::bail!("Dekaf not supported")
                }
            };
            let request = materialize::Request {
                spec: Some(request),
                ..Default::default()
            }
            .with_internal(|internal| {
                if let Some(s) = &model.shards.log_level {
                    internal.set_log_level(ops::LogLevel::from_str_name(s).unwrap_or_default());
                }
            });
            let response = runtime
                .unary_materialize(request)
                .await?
                .spec
                .context("connector didn't send expected Spec response")?;

            serde_json::to_string(&response).context("Failed to serialize spec response")?
        }
        (None, None) => anyhow::bail!("task not found"),
    };

    println!("{}", serialized);

    Ok(())
}
