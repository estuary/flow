use crate::local_specs;
use anyhow::{bail, Context};
use itertools::Itertools;
use proto_flow::{capture, flow};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    net::ToSocketAddrs,
    sync::{Arc, Mutex},
};
use warp::{http::Response, Filter};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Oauth {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Name of the task to test oauth for.
    #[clap(long)]
    task: Option<String>,
    /// Docker network to run the connector, if one exists
    #[clap(long, default_value = "bridge")]
    network: String,

    /// Your OAuth application's client ID. This corresponds to the
    /// `connectors.oauth2_client_id` column in the database
    #[clap(long)]
    client_id: String,
    /// Your OAuth application's client secret. This corresponds to the
    /// `connectors.oauth2_client_secret` column in the database
    #[clap(long)]
    client_secret: String,
    /// The endpoint config you want to test with. This is the config for a
    /// sample user account that will perform the end-to-end consent flow
    #[clap(long)]
    endpoint_config: serde_json::Value,
    /// If the connector's oauth spec required needs any other values injected.
    /// This will match the `connectors.oauth2_injected_values` column in the database.
    #[clap(long)]
    injected_values: Option<serde_json::Value>,
}

pub async fn do_oauth(
    ctx: &mut crate::CliContext,
    Oauth {
        source,
        task,
        network,
        client_id,
        client_secret,
        endpoint_config,
        injected_values,
    }: &Oauth,
) -> anyhow::Result<()> {
    // TESTING
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into()) // Otherwise it's ERROR.
        .from_env_lossy();

    let guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt::fmt()
            .with_env_filter(env_filter)
            .compact()
            .without_time()
            .with_target(false)
            .with_writer(std::io::stderr)
            .finish(),
    );
    // END TESTING
    let source = build::arg_source_to_url(source, false)?;
    let mut sources = local_specs::surface_errors(local_specs::load(&source).await.into_result())?;

    // Identify the capture to inspect.
    // TODO (jshearer): materialization support
    let needle = if let Some(needle) = task {
        needle.as_str()
    } else if sources.captures.len() == 1 {
        sources.captures.first().unwrap().capture.as_str()
    } else if sources.captures.is_empty() {
        anyhow::bail!("sourced specification files do not contain any captures");
    } else {
        anyhow::bail!("sourced specification files contain multiple captures. Use --capture to identify a specific one");
    };

    let capture = match sources
        .captures
        .binary_search_by_key(&needle, |c| c.capture.as_str())
    {
        Ok(index) => &mut sources.captures[index],
        Err(_) => anyhow::bail!("could not find the capture {needle}"),
    };

    tracing::info!(
        task_name = capture.capture.as_str(),
        "Performing oauth flow on task"
    );

    let spec_req = match &capture.spec.endpoint {
        models::CaptureEndpoint::Connector(config) => capture::request::Spec {
            connector_type: flow::capture_spec::ConnectorType::Image as i32,
            config_json: serde_json::to_string(&config).unwrap(),
        },
        models::CaptureEndpoint::Local(config) => capture::request::Spec {
            connector_type: flow::capture_spec::ConnectorType::Local as i32,
            config_json: serde_json::to_string(config).unwrap(),
        },
    };
    // Get the task spec's oauth field
    let mut spec_req = capture::Request {
        spec: Some(spec_req),
        ..Default::default()
    };

    if let Some(log_level) = capture
        .spec
        .shards
        .log_level
        .as_ref()
        .and_then(|s| ops::LogLevel::from_str_name(s))
    {
        spec_req.set_internal_log_level(log_level);
    }

    let spec_response = runtime::Runtime::new(
        true, // All local.
        network.clone(),
        ops::tracing_log_handler,
        None,
        format!("spec/{}", capture.capture),
    )
    .unary_capture(spec_req, build::CONNECTOR_TIMEOUT)
    .await
    .map_err(crate::status_to_anyhow)?
    .spec
    .context("connector didn't send expected Spec response")?;

    let oauth_spec = spec_response
        .oauth2
        .expect("Connector did not return an oauth config");

    // TODO (jshearer): Validate endpoint config against spec.config_schema_json

    tracing::info!(
        "Got connector's OAuth spec: {}",
        serde_json::to_string_pretty(&oauth_spec)?
    );

    let port = 16963;

    tracing::warn!("Make sure that your application has http://localhost:{port} set as an allowed redirect URL");
    let api = ctx
        .config
        .api
        .as_ref()
        .expect("Cannot connect to edge functions");

    let mut oauth_endpoint = api.endpoint.clone();
    oauth_endpoint.set_path("functions/v1/oauth");

    #[derive(serde::Deserialize)]
    struct AuthorizeResponse {
        url: String,
        state: String,
        code_verifier: String,
    }

    let authorize_response = reqwest::Client::new()
        .post(oauth_endpoint.clone())
        .bearer_auth(api.access_token.to_owned())
        .header("apikey", api.public_token.to_owned())
        .json(&serde_json::json!({
            "operation": "auth-url",
            "connector_config": {
                "oauth2_client_id": client_id,
                "oauth2_spec": oauth_spec
            },
            "redirect_uri": format!("http://localhost:{port}").as_str(),
            "config": endpoint_config
        }))
        .send()
        .await
        .context("Fetching auth-url")?
        .json::<AuthorizeResponse>()
        .await
        .context("Parsing auth-url response")?;

    tracing::info!(url = authorize_response.url, "Opening authorize URL");

    open::that(authorize_response.url)?;

    let redirect_params = get_single_request_query(([127, 0, 0, 1], port)).await?;

    tracing::info!(
        redirect_params = ?redirect_params,
        "Got auth code response parameters"
    );

    let mut code_request_body = serde_json::json!({
        "operation": "access-token",
        "connector_config": {
            "oauth2_client_id": client_id,
            "oauth2_client_secret": client_secret,
            "oauth2_injected_values": injected_values.as_ref().unwrap_or(&serde_json::json!(null)),
            "oauth2_spec": oauth_spec
        },
        "state": authorize_response.state,
        "code_verifier": authorize_response.code_verifier,
        "config": endpoint_config
    });

    let code_request_map = code_request_body.as_object_mut().unwrap();

    for (k, v) in redirect_params.into_iter() {
        code_request_map.insert(k, serde_json::Value::String(v));
    }

    tracing::info!("Exchanging auth code for access token");

    let code_response = reqwest::Client::new()
        .post(oauth_endpoint)
        .bearer_auth(api.access_token.to_owned())
        .header("apikey", api.public_token.to_owned())
        .json(&code_request_body)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    tracing::info!(
        "🎉 Got access token response: \n{}",
        serde_json::to_string_pretty(&code_response)?
    );

    drop(guard);

    Ok(())
}

/// Spawns an HTTP server that listens for a single request
/// then shuts down and returns that requet's query params.
pub async fn get_single_request_query<S>(listen_addr: S) -> anyhow::Result<HashMap<String, String>>
where
    std::net::SocketAddr: From<S>,
    S: Debug,
{
    let (tx, rx) = tokio::sync::oneshot::channel::<HashMap<String, String>>();
    // All this because a oneshot Sender's `.send()` method consumes `self`
    let wrapped = Arc::new(Mutex::new(Some(tx)));
    let service = warp::get()
        .and(warp::query::<HashMap<String, String>>())
        .map({
            let tx = wrapped.clone();
            move |query| {
                let mut maybe_sender = tx.try_lock().unwrap();
                if let Some(tx) = maybe_sender.take() {
                    tx.send(query).unwrap();
                    Response::builder().body("You may close this window now.")
                } else {
                    Response::builder().body("Something went wrong")
                }
            }
        });

    tokio::select! {
        _ = warp::serve(service).run(listen_addr) => {
            bail!("Server exited early")
        }
        resp = rx => {
            return Ok(resp?)
        }
    }
}
