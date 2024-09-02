use crate::local_specs;
use anyhow::{bail, Context};
use proto_flow::{capture, flow};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
    time::Duration,
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

const OAUTH_CREDENTIALS_KEY: &str = "credentials";

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
    let source = build::arg_source_to_url(source, false)?;
    let draft = local_specs::surface_errors(local_specs::load(&source).await.into_result())?;

    // Identify the capture to inspect.
    // TODO (jshearer): materialization support
    let needle = if let Some(needle) = task {
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

    println!(
        "Performing oauth flow on task '{}'",
        capture.capture.as_str()
    );

    let spec_req = match &model.endpoint {
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
    let spec_req = capture::Request {
        spec: Some(spec_req),
        ..Default::default()
    }
    .with_internal(|internal| {
        if let Some(s) = &model.shards.log_level {
            internal.set_log_level(ops::LogLevel::from_str_name(s).unwrap_or_default());
        }
    });

    let spec_response = runtime::Runtime::new(
        true, // All local.
        network.clone(),
        ops::tracing_log_handler,
        None,
        format!("spec/{}", capture.capture),
    )
    .unary_capture(spec_req)
    .await?
    .spec
    .context("connector didn't send expected Spec response")?;

    let oauth_spec = spec_response
        .oauth2
        .expect("Connector did not return an oauth config");

    // Let's make sure that the provided endpoint config matches the
    // schema emitted by the connector
    let curi = url::Url::parse("flow://fixture").unwrap();
    let mut parsed_schema: serde_json::Value =
        serde_json::from_str(spec_response.config_schema_json.as_str()).unwrap();

    // We have to remove the special "credentials" key from the list of required fields
    // because the whole point of this command is to generate it, so it's expected
    // to be missing from the input. Also validate that it's there in the first place,
    // as it needs to be for oauth to work properly.
    let creds_key_position = parsed_schema
        .get("required")
        .and_then(|val| val.as_array())
        .and_then(|required_fields| {
            required_fields.iter().position(|item| {
                item.as_str()
                    .map(|str| str.eq(OAUTH_CREDENTIALS_KEY))
                    .unwrap_or(false)
            })
        })
        .expect(format!("{OAUTH_CREDENTIALS_KEY} must be a required field").as_str());

    // Unwrap here because we already validate above
    parsed_schema
        .get_mut("required")
        .unwrap()
        .as_array_mut()
        .unwrap()
        .remove(creds_key_position);

    let schema = doc::validation::build_schema(curi, &parsed_schema).unwrap();
    let mut validator = doc::Validator::new(schema).unwrap();

    let pretty_endpoint_config = serde_json::to_string_pretty(&endpoint_config).unwrap();
    let pretty_schema = serde_json::to_string_pretty(&parsed_schema).unwrap();

    validator.validate(
        None,
        endpoint_config
    ).context(
        format!("Provided endpoint config did not match schema. \nEndpoint config: {pretty_endpoint_config}\nSchema: {pretty_schema}")
    )?.ok()
    .context("Provided endpoint config did not match schema.")?;

    println!(
        "Got connector's OAuth spec: {}",
        serde_json::to_string_pretty(&oauth_spec)?
    );

    let port = 16963;

    let redirect_uri = format!("http://localhost:{port}/");
    tracing::warn!(
        "Make sure that your application has {redirect_uri} set as an allowed redirect URL"
    );
    let api = ctx
        .config
        .api
        .as_ref()
        .expect("Cannot connect to edge functions");

    let mut oauth_endpoint = api.endpoint.clone();
    oauth_endpoint.set_path("functions/v1/oauth");

    #[derive(serde::Deserialize, serde::Serialize)]
    struct AuthorizeResponse {
        url: String,
        state: String,
    }

    let authorize_response_bytes = reqwest::Client::new()
        .post(oauth_endpoint.clone())
        .bearer_auth(api.access_token.to_owned())
        .header("apikey", api.public_token.to_owned())
        .json(&serde_json::json!({
            "operation": "auth-url",
            "connector_config": {
                "oauth2_client_id": client_id,
                "oauth2_spec": oauth_spec
            },
            "redirect_uri": redirect_uri,
            "config": endpoint_config
        }))
        .send()
        .await
        .context("Fetching auth-url")?
        .bytes()
        .await?;

    let authorize_response = serde_json::from_slice::<AuthorizeResponse>(&authorize_response_bytes)
        .with_context(|| {
            let str = std::str::from_utf8(&authorize_response_bytes).unwrap();
            format!("Unexpected auth-url response body: {str}")
        })?;

    println!(
        "Got authorize response: {}",
        serde_json::to_string_pretty(&authorize_response).unwrap()
    );
    println!("Opening authorize URL {}", authorize_response.url);

    open::that(authorize_response.url)?;

    let redirect_params = get_single_request_query(([127, 0, 0, 1], port)).await?;

    println!("Got auth code response parameters: {:?}", redirect_params);

    let mut code_request_body = serde_json::json!({
        "operation": "access-token",
        "connector_config": {
            "oauth2_client_id": client_id,
            "oauth2_client_secret": client_secret,
            "oauth2_injected_values": injected_values.as_ref().unwrap_or(&serde_json::json!(null)),
            "oauth2_spec": oauth_spec
        },
        "state": authorize_response.state,
        "config": endpoint_config,
        "redirect_uri": redirect_uri
    });

    let code_request_map = code_request_body.as_object_mut().unwrap();

    for (k, v) in redirect_params.into_iter() {
        code_request_map.insert(k, serde_json::Value::String(v));
    }

    println!(
        "Exchanging auth code for access token. Request map: {}",
        serde_json::to_string_pretty(&code_request_map).unwrap()
    );

    let code_response = reqwest::Client::new()
        .post(oauth_endpoint)
        .bearer_auth(api.access_token.to_owned())
        .header("apikey", api.public_token.to_owned())
        .json(&code_request_body)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    println!(
        "🎉 Got access token response: \n{}",
        serde_json::to_string_pretty(&code_response)?
    );

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
        _ = tokio::time::sleep(Duration::from_secs(90)) => {
            bail!("Timed out waiting for auth code redirect")
        }
        resp = rx => {
            return Ok(resp?)
        }
    }
}
