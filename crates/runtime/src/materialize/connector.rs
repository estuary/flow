use super::triggers::CompiledTriggers;
use crate::{LogHandler, Runtime};
use anyhow::Context;
use futures::{FutureExt, StreamExt, TryStreamExt, channel::mpsc, stream::BoxStream};
use proto_flow::{
    flow::materialization_spec::ConnectorType,
    materialize::{Request, Response},
};
use unseal;
use zeroize::Zeroize;

/// Ancillary data extracted during connector start for Open requests.
pub struct OpenExtras {
    /// Pre-compiled trigger configurations, if any were specified.
    pub compiled_triggers: Option<CompiledTriggers>,
    /// The OCI image name of the connector (empty for non-image connectors).
    pub connector_image: String,
}

// Start a materialization connector as indicated by the `initial` Request.
// Returns a pair of Streams for sending Requests and receiving Responses,
// plus OpenExtras with decrypted trigger configs and connector metadata.
pub async fn start<L: LogHandler>(
    runtime: &Runtime<L>,
    mut initial: Request,
) -> anyhow::Result<(
    mpsc::Sender<Request>,
    BoxStream<'static, anyhow::Result<Response>>,
    OpenExtras,
)> {
    let log_level = initial.get_internal()?.log_level();
    let (endpoint, config_json, connector_type, catalog_name, sealed_config_json) =
        extract_endpoint(&mut initial)?;
    let (mut connector_tx, connector_rx) = mpsc::channel(crate::CHANNEL_BUFFER);

    fn attach_container(response: &mut Response, container: crate::image_connector::Container) {
        response.set_internal(|internal| {
            internal.container = Some(container);
        });
    }

    fn start_rpc(
        channel: tonic::transport::Channel,
        rx: mpsc::Receiver<Request>,
    ) -> crate::image_connector::StartRpcFuture<Response> {
        async move {
            proto_grpc::materialize::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .materialize(rx)
                .await
        }
        .boxed()
    }

    // Sealed endpoint configuration, extracted from the matched endpoint and
    // decrypted later, once the connector's spec response is available. `None`
    // for Dekaf, which decrypts its own config outside this path.
    let sealed_config;
    let (mut connector_rx, connector_image) = match endpoint {
        models::MaterializationEndpoint::Connector(models::ConnectorConfig { image, config }) => {
            sealed_config = Some(config);

            let rx = crate::image_connector::serve(
                attach_container,
                1, // Skip first (internal) Spec response.
                image.clone(),
                runtime.log_handler.clone(),
                log_level,
                &runtime.container_network,
                connector_rx,
                start_rpc,
                &runtime.task_name,
                ops::TaskType::Materialization,
                runtime.plane,
            )
            .await?
            .boxed();

            (rx, image)
        }
        models::MaterializationEndpoint::Local(_)
            if !matches!(runtime.plane, crate::Plane::Local) =>
        {
            return Err(tonic::Status::failed_precondition(
                "Local connectors are not permitted in this context",
            )
            .into());
        }
        models::MaterializationEndpoint::Local(models::LocalConfig {
            command,
            config,
            env,
            protobuf,
        }) => {
            sealed_config = Some(config);

            let rx = crate::local_connector::serve(
                command,
                env,
                runtime.log_handler.clone(),
                log_level,
                protobuf,
                connector_rx,
            )?
            .boxed();

            (rx, String::new())
        }
        models::MaterializationEndpoint::Dekaf(_) => {
            // Dekaf is in-process Rust and consumes prost requests directly. It
            // decrypts its own (nested) endpoint config, so there's nothing to
            // decrypt or overlay here.
            sealed_config = None;

            (
                dekaf_connector::connector(connector_rx).boxed(),
                String::new(),
            )
        }
    };

    // Send an initial Spec request which may direct us to perform an IAM token exchange.
    connector_tx
        .try_send(Request {
            spec: Some(proto_flow::materialize::request::Spec {
                config_json: "{}".into(),
                connector_type: connector_type,
            }),
            ..Default::default()
        })
        .unwrap();

    let verify = crate::verify("connector", "spec response");
    let spec_response = match verify.not_eof(connector_rx.try_next().await?)? {
        Response { spec: Some(r), .. } => r,
        response => return verify.fail(response),
    };

    // Decrypt the sealed endpoint configuration into the connector request, applying
    // any nonsensitive `sops.overlay` properties subject to schema validation.
    if let Some(sealed_config) = &sealed_config {
        *config_json =
            unseal::overlay::decrypt_with_overlay(sealed_config, &spec_response.config_schema_json)
                .await?
                .into();
    }

    if let Ok(Some(iam_config)) = iam_auth::extract_iam_auth_from_connector_config(
        config_json,
        &spec_response.config_schema_json,
    ) {
        // Only proceed with IAM auth if we have an actual catalog name
        if let Some(task_name) = catalog_name.as_deref() {
            let mut tokens = iam_config
                .generate_tokens(task_name)
                .await
                .map_err(crate::anyhow_to_status)?;

            *config_json = tokens.inject_into(config_json)?.to_string().into();
            tokens.zeroize();
        }
    }

    // Provide the connector with the sealed endpoint configuration alongside the
    // decrypted `config_json`, so it may emit `configUpdate`s which adjust its own
    // `sops.overlay` without re-encrypting the configuration. Only present on Open.
    if let (Some(sealed_config_json), Some(sealed_config)) = (sealed_config_json, sealed_config) {
        *sealed_config_json = sealed_config.into();
    }

    // Decrypt trigger configs and pre-compile their Handlebars templates.
    let triggers_json = initial
        .open
        .as_ref()
        .and_then(|open| open.materialization.as_ref())
        .map(|s| &s.triggers_json)
        .filter(|b| !b.is_empty());

    let compiled_triggers = match triggers_json {
        None => None,
        Some(triggers_json) => {
            let decrypted = decrypt_triggers(triggers_json).await?;
            Some(
                CompiledTriggers::compile(decrypted.config.into_map())
                    .context("compiling trigger templates")?,
            )
        }
    };

    let open_extras = OpenExtras {
        compiled_triggers,
        connector_image,
    };

    connector_tx.try_send(initial).unwrap();

    Ok((connector_tx, connector_rx, open_extras))
}

/// Decrypt a sealed `triggers_json` into its plaintext model.
///
/// New configs decrypt through the `sops.overlay` path. Legacy (pre-overlay)
/// configs — recognized by a list-shaped `config` — are decrypted through the
/// old HMAC-exclusion path instead, since their MAC was computed over a
/// placeholder-stripped document. This compatibility branch is removed once no
/// legacy configs remain.
async fn decrypt_triggers(triggers_json: &[u8]) -> anyhow::Result<models::Triggers> {
    let probe: serde_json::Value =
        serde_json::from_slice(triggers_json).context("parsing triggers JSON")?;

    if probe.get("config").is_some_and(|c| c.is_array()) {
        let mut legacy: models::triggers::LegacyTriggers =
            serde_json::from_value(probe).context("parsing legacy triggers JSON")?;
        let originals = models::triggers::strip_hmac_excluded_fields(&mut legacy);
        let stripped = models::RawValue::from_string(
            serde_json::to_string(&legacy).context("serializing stripped triggers")?,
        )
        .expect("stripped triggers serialize to JSON");

        let mut decrypted: models::triggers::LegacyTriggers = serde_json::from_str(
            unseal::decrypt_sops(&stripped)
                .await
                .context("decrypting legacy triggers_json")?
                .get(),
        )
        .context("parsing decrypted legacy triggers JSON")?;
        models::triggers::restore_hmac_excluded_fields(&mut decrypted, originals);
        return Ok(decrypted.into_triggers());
    }

    let sealed: Box<models::RawValue> =
        serde_json::from_slice(triggers_json).context("parsing triggers JSON")?;
    let schema =
        serde_json::to_vec(&models::triggers_schema()).expect("triggers schema must serialize");

    serde_json::from_str(
        unseal::overlay::decrypt_with_overlay(&sealed, &schema)
            .await
            .context("decrypting triggers_json")?
            .get(),
    )
    .context("parsing decrypted triggers JSON")
}

fn extract_endpoint<'r>(
    request: &'r mut Request,
) -> anyhow::Result<(
    models::MaterializationEndpoint,
    &'r mut bytes::Bytes,
    i32,
    Option<String>,
    Option<&'r mut bytes::Bytes>,
)> {
    let (connector_type, config_json, catalog_name, sealed_config_json) = match request {
        Request {
            spec: Some(spec), ..
        } => (spec.connector_type, &mut spec.config_json, None, None),
        Request {
            validate: Some(validate),
            ..
        } => (
            validate.connector_type,
            &mut validate.config_json,
            Some(validate.name.clone()),
            None,
        ),
        Request {
            apply: Some(apply), ..
        } => {
            let catalog_name = apply.materialization.as_ref().map(|m| m.name.clone());
            let inner = apply
                .materialization
                .as_mut()
                .context("`apply` missing required `materialization`")?;

            (
                inner.connector_type,
                &mut inner.config_json,
                catalog_name,
                None,
            )
        }
        Request {
            open: Some(open), ..
        } => {
            let catalog_name = open.materialization.as_ref().map(|m| m.name.clone());
            let sealed_config_json = &mut open.sealed_config_json;
            let inner = open
                .materialization
                .as_mut()
                .context("`open` missing required `materialization`")?;

            (
                inner.connector_type,
                &mut inner.config_json,
                catalog_name,
                Some(sealed_config_json),
            )
        }
        request => return crate::verify("client", "valid first request").fail(request),
    };

    if connector_type == ConnectorType::Image as i32 {
        Ok((
            models::MaterializationEndpoint::Connector(
                serde_json::from_slice(config_json).context("parsing connector config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
            sealed_config_json,
        ))
    } else if connector_type == ConnectorType::Local as i32 {
        Ok((
            models::MaterializationEndpoint::Local(
                serde_json::from_slice(config_json).context("parsing local config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
            sealed_config_json,
        ))
    } else if connector_type == ConnectorType::Dekaf as i32 {
        Ok((
            models::MaterializationEndpoint::Dekaf(
                serde_json::from_slice(config_json).context("parsing local config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
            sealed_config_json,
        ))
    } else {
        anyhow::bail!("invalid connector type: {connector_type}");
    }
}
