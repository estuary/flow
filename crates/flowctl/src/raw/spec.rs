use crate::local_specs;
use anyhow::Context;
use proto_flow::{capture, flow};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Spec {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Name of the capture to discover within the Flow specification file.
    /// Capture is required if there are multiple captures in --source specifications.
    #[clap(long)]
    capture: Option<String>,
    /// Docker network to run the connector, if one exists
    #[clap(long, default_value = "bridge")]
    network: String,
}

pub async fn do_spec(
    _ctx: &mut crate::CliContext,
    Spec {
        source,
        capture,
        network,
    }: &Spec,
) -> anyhow::Result<()> {
    let source = build::arg_source_to_url(source, false)?;
    let draft = local_specs::surface_errors(local_specs::load(&source).await.into_result())?;

    // Identify the capture to inspect.
    let needle = if let Some(needle) = capture {
        needle.as_str()
    } else if draft.captures.len() == 1 {
        draft.captures.first().unwrap().capture.as_str()
    } else if draft.captures.is_empty() {
        anyhow::bail!("sourced specification files do not contain any captures");
    } else {
        anyhow::bail!("sourced specification files contain multiple captures. Use --capture to identify a specific one");
    };

    let capture = match draft
        .captures
        .binary_search_by_key(&needle, |c| c.capture.as_str())
    {
        Ok(index) => &draft.captures[index],
        Err(_) => anyhow::bail!("could not find the capture {needle}"),
    };
    let model = capture.model.as_ref().expect("not a deletion");

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

    let response = runtime::Runtime::new(
        true, // Allow local.
        network.clone(),
        ops::tracing_log_handler,
        None,
        format!("spec/{}", capture.capture),
    )
    .unary_capture(request)
    .await?
    .spec
    .context("connector didn't send expected Spec response")?;

    let serialized =
        serde_json::to_string(&response).context("Failed to serialize spec response")?;

    println!("{}", serialized);

    Ok(())
}
