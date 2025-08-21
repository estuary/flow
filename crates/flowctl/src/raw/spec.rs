use crate::local_specs;
use anyhow::Context;
use proto_flow::{capture, derive, flow, materialize};
use tables::DraftCatalog;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Spec {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Name of the task to request the spec for within the Flow specification file.
    /// Required if there are multiple tasks in --source specifications.
    #[clap(long)]
    name: Option<String>,
    /// Docker network to run the connector, if one exists
    #[clap(long, default_value = "bridge")]
    network: String,
}

pub async fn do_spec(
    _ctx: &mut crate::CliContext,
    Spec {
        source,
        name,
        network,
    }: &Spec,
) -> anyhow::Result<()> {
    let source = build::arg_source_to_url(source, false)?;
    let draft = local_specs::surface_errors(local_specs::load(&source).await.into_result())?;

    // Identify the task to inspect.
    let num_tasks = draft.captures.len()
        + draft.materializations.len()
        + draft
            .collections
            .iter()
            .filter(|c| {
                c.model
                    .as_ref()
                    .map(|m| m.derive.is_some())
                    .unwrap_or_default()
            })
            .count();

    if num_tasks == 0 {
        anyhow::bail!("sourced specification files do not contain any tasks (captures, derivations, or materializations)");
    } else if num_tasks > 1 && name.is_none() {
        anyhow::bail!("sourced specification files contain multiple tasks (captures, derivations, or materializations). Use --name to identify a specific task");
    }

    let needle = if let Some(needle) = name {
        needle.as_str()
    } else if draft.captures.len() == 1 {
        draft.captures.first().unwrap().capture.as_str()
    } else if draft.materializations.len() == 1 {
        draft
            .materializations
            .first()
            .unwrap()
            .materialization
            .as_str()
    } else {
        draft
            .collections
            .iter()
            .filter(|c| {
                c.model
                    .as_ref()
                    .map(|m| m.derive.is_some())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .first()
            .unwrap()
            .collection
            .as_str()
    };

    let serialized = get_spec_response(needle, &network, &draft).await?;
    println!("{}", serialized);

    Ok(())
}

async fn get_spec_response(
    name: &str,
    network: &str,
    draft: &DraftCatalog,
) -> anyhow::Result<String> {
    let runtime = runtime::Runtime::new(
        true, // Allow local.
        network.to_string(),
        ops::tracing_log_handler,
        None,
        format!("spec/{}", name),
    );

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

        return serde_json::to_string(&response).context("Failed to serialize spec response");
    }

    for row in draft.collections.iter() {
        if name != row.collection.as_str() {
            continue;
        }

        let model = row.model.as_ref().and_then(|m| m.derive.as_ref());
        let model = if model.is_none() {
            anyhow::bail!("{} is not a derivation", name)
        } else {
            model.unwrap()
        };

        let request = match &model.using {
            models::DeriveUsing::Connector(config) => derive::request::Spec {
                connector_type: flow::collection_spec::derivation::ConnectorType::Image as i32,
                config_json: serde_json::to_string(&config).unwrap().into(),
            },
            models::DeriveUsing::Sqlite(config) => derive::request::Spec {
                connector_type: flow::collection_spec::derivation::ConnectorType::Sqlite as i32,
                config_json: serde_json::to_string(&config).unwrap().into(),
            },
            models::DeriveUsing::Typescript(config) => derive::request::Spec {
                connector_type: flow::collection_spec::derivation::ConnectorType::Typescript as i32,
                config_json: serde_json::to_string(&config).unwrap().into(),
            },
            models::DeriveUsing::Local(config) => derive::request::Spec {
                connector_type: flow::collection_spec::derivation::ConnectorType::Local as i32,
                config_json: serde_json::to_string(&config).unwrap().into(),
            },
        };

        let request = derive::Request {
            spec: Some(request),
            ..Default::default()
        }
        .with_internal(|internal| {
            if let Some(s) = &model.shards.log_level {
                internal.set_log_level(ops::LogLevel::from_str_name(s).unwrap_or_default());
            }
        });
        let response = runtime
            .unary_derive(request)
            .await?
            .spec
            .context("connector didn't send expected Spec response")?;

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

        return serde_json::to_string(&response).context("Failed to serialize spec response");
    }

    anyhow::bail!("could not find task {}", name);
}
