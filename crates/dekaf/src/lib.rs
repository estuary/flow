use anyhow::Context;
use bytes::{BufMut, Bytes};
use kafka_protocol::{
    messages::{self, ApiKey, TopicName},
    protocol::{buf::ByteBuf, Decodable, Encodable, StrBytes},
};
use tracing::instrument;

mod topology;
use topology::{fetch_all_collection_names, Collection, Partition};

mod read;
use read::Read;

mod session;
pub use session::Session;

pub mod registry;

mod api_client;
pub use api_client::KafkaApiClient;

use percent_encoding::{percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use simple_crypt::{decrypt, encrypt};

pub struct App {
    /// Anonymous API client for the Estuary control plane.
    pub anon_client: postgrest::Postgrest,
    /// Hostname which is advertised for Kafka access.
    pub advertise_host: String,
    /// Port which is advertised for Kafka access.
    pub advertise_kafka_port: u16,
    /// Client used when proxying group management APIs.
    pub kafka_client: KafkaApiClient,
}

// Kind of a cool/gross hack modified from this comment
// in a thread requesting literal default values in Serde:
// https://github.com/serde-rs/serde/issues/368#issuecomment-1579475447
fn bool<const U: bool>() -> bool {
    U
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigOptions {
    #[serde(default = "bool::<false>")]
    pub strict_topic_names: bool,
}

impl App {
    #[tracing::instrument(level = "info", err(Debug, level = "warn"), skip(self, password))]
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> anyhow::Result<(postgrest::Postgrest, ConfigOptions, String)> {
        let username_str = if username.contains("{") {
            username.to_string()
        } else {
            decode_safe_name(username.to_string()).context("failed to decode username")?
        };
        let config: ConfigOptions = serde_json::from_str(&username_str)
            .context("failed to parse username as a JSON object")?;

        #[derive(serde::Deserialize)]
        struct RefreshToken {
            id: String,
            secret: String,
        }
        let RefreshToken {
            id: refresh_token_id,
            secret,
        } = serde_json::from_slice(&base64::decode(password).context("password is not base64")?)
            .context("failed to decode refresh token from password")?;

        tracing::info!(refresh_token_id, "authenticating refresh token");

        #[derive(serde::Deserialize)]
        struct AccessToken {
            access_token: String,
        }
        let AccessToken { access_token } = self
            .anon_client
            .rpc(
                "generate_access_token",
                serde_json::json!({"refresh_token_id": refresh_token_id, "secret": secret})
                    .to_string(),
            )
            .execute()
            .await
            .and_then(|r| r.error_for_status())
            .context("generating access token")?
            .json()
            .await?;

        let authenticated_client = self
            .anon_client
            .clone()
            .insert_header("Authorization", format!("Bearer {access_token}"));

        let uid: String = authenticated_client
            .rpc("auth_uid", "")
            .select("*")
            .execute()
            .await
            .and_then(|r| r.error_for_status())
            .context("fetching user id")?
            .json()
            .await?;

        Ok((authenticated_client, config, uid))
    }
}

/// Dispatch a read request `frame` of the current session, writing its response into `out`.
/// `raw_sasl_auth` is the state of SASL "raw" mode authentication,
/// and conditions the interpretation of request frames.
#[tracing::instrument(level = "trace", err(level = "warn"), skip_all)]
pub async fn dispatch_request_frame(
    session: &mut Session,
    raw_sasl_auth: &mut bool,
    frame: bytes::BytesMut,
    out: &mut bytes::BytesMut,
) -> anyhow::Result<()> {
    /*
    println!(
         "full frame:\n{}",
         hexdump::hexdump_iter(&frame)
             .map(|line| format!(" {line}"))
             .collect::<Vec<_>>()
             .join("\n")
     );
    */

    let (api_key, version) = if !*raw_sasl_auth {
        let api_key = i16::from_be_bytes(frame[0..2].try_into().context("parsing api key")?);
        let api_key = messages::ApiKey::try_from(api_key)
            .map_err(|()| anyhow::anyhow!("invalid request API key: {api_key}"))?;

        let version =
            i16::from_be_bytes(frame[2..4].try_into().context("parsing request version")?);

        (api_key, version)
    } else {
        (messages::ApiKey::SaslAuthenticateKey, 0)
    };

    /*
    tracing::debug!(version, ?api_key, "parsed API key and version");
    println!(
        "payload frame:\n{}",
        hexdump::hexdump_iter(&frame)
            .map(|line| format!(" {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    */

    handle_api(api_key, version, session, raw_sasl_auth, frame, out).await
}

#[instrument(level="debug", skip_all,fields(?api_key,v=version))]
async fn handle_api(
    api_key: ApiKey,
    version: i16,
    session: &mut Session,
    raw_sasl_auth: &mut bool,
    frame: bytes::BytesMut,
    out: &mut bytes::BytesMut,
) -> anyhow::Result<()> {
    tracing::debug!("Handling request");
    use messages::*;
    let ret = match api_key {
        ApiKey::ApiVersionsKey => {
            // https://github.com/confluentinc/librdkafka/blob/e03d3bb91ed92a38f38d9806b8d8deffe78a1de5/src/rdkafka_request.c#L2823
            let (header, request) = dec_request(frame, version)?;
            tracing::debug!(client_id=?header.client_id, "Got client ID!");
            Ok(enc_resp(out, &header, session.api_versions(request).await?))
        }
        ApiKey::SaslHandshakeKey => {
            let (header, request) = dec_request(frame, version)?;
            *raw_sasl_auth = header.request_api_version == 0;
            Ok(enc_resp(
                out,
                &header,
                session.sasl_handshake(request).await?,
            ))
        }
        ApiKey::SaslAuthenticateKey if *raw_sasl_auth => {
            *raw_sasl_auth = false;

            let request =
                messages::SaslAuthenticateRequest::default().with_auth_bytes(frame.freeze());
            let response = session.sasl_authenticate(request).await?;

            out.put_i32(response.auth_bytes.len() as i32);
            out.extend(response.auth_bytes);
            Ok(())
        }
        ApiKey::SaslAuthenticateKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(
                out,
                &header,
                session.sasl_authenticate(request).await?,
            ))
        }
        ApiKey::MetadataKey => {
            // https://github.com/confluentinc/librdkafka/blob/e03d3bb91ed92a38f38d9806b8d8deffe78a1de5/src/rdkafka_request.c#L2417
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(out, &header, session.metadata(request).await?))
        }
        ApiKey::FindCoordinatorKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(
                out,
                &header,
                session.find_coordinator(request).await?,
            ))
        }
        ApiKey::ListOffsetsKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(out, &header, session.list_offsets(request).await?))
        }

        ApiKey::FetchKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(out, &header, session.fetch(request).await?))
        }

        ApiKey::DescribeConfigsKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(
                out,
                &header,
                session.describe_configs(request).await?,
            ))
        }
        ApiKey::ProduceKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(out, &header, session.produce(request).await?))
        }

        ApiKey::JoinGroupKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(
                out,
                &header.clone(),
                session.join_group(request, header).await?,
            ))
        }
        ApiKey::LeaveGroupKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(
                out,
                &header.clone(),
                session.leave_group(request, header).await?,
            ))
        }
        ApiKey::ListGroupsKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(
                out,
                &header.clone(),
                session.list_groups(request, header).await?,
            ))
        }
        ApiKey::SyncGroupKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(
                out,
                &header.clone(),
                session.sync_group(request, header).await?,
            ))
        }
        ApiKey::DeleteGroupsKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(
                out,
                &header.clone(),
                session.delete_group(request, header).await?,
            ))
        }
        ApiKey::HeartbeatKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(
                out,
                &header.clone(),
                session.heartbeat(request, header).await?,
            ))
        }
        ApiKey::OffsetFetchKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(
                out,
                &header.clone(),
                session.offset_fetch(request, header).await?,
            ))
        }
        ApiKey::OffsetCommitKey => {
            let (header, request) = dec_request(frame, version)?;
            Ok(enc_resp(
                out,
                &header.clone(),
                session.offset_commit(request, header).await?,
            ))
        }
        /*
        ApiKey::CreateTopicsKey => Ok(K::CreateTopicsRequest(CreateTopicsRequest::decode(b, v)?)),
        ApiKey::ListGroupsKey => Ok(K::ListGroupsRequest(ListGroupsRequest::decode(b, v)?)),
        */
        _ => anyhow::bail!("unsupported request type {api_key:?}"),
    };
    tracing::debug!("Response sent");

    ret
}

// Easier dispatch to type-specific decoder by using result-type inference.
fn dec_request<T: kafka_protocol::protocol::Request + std::fmt::Debug>(
    mut frame: bytes::BytesMut,
    req_version: i16,
) -> anyhow::Result<(messages::RequestHeader, T)> {
    let header_version = T::header_version(req_version);
    let header = messages::RequestHeader::decode(&mut frame, header_version)?;

    let request = T::decode(&mut frame, header.request_api_version).with_context(|| {
        format!(
            "failed to decode {} with header version {header_version}: {header:?}",
            std::any::type_name::<T>()
        )
    })?;

    if !frame.is_empty() {
        tracing::warn!(
            "frame with header version {header_version}: ({header:?}) has {} bytes remaining after decoding {}. Parsed: {request:?}, remaining bytes: {:?}",
            frame.len(),
            std::any::type_name::<T>(),
            frame.peek_bytes(0..frame.len())
        );
    }
    tracing::trace!(?request, ?header, "decoded request");

    Ok((header, request))
}

// Encodes a complete frame for the given request header and response payload.
fn enc_resp<
    T: kafka_protocol::protocol::Encodable + kafka_protocol::protocol::HeaderVersion + std::fmt::Debug,
>(
    b: &mut bytes::BytesMut,
    rh: &messages::RequestHeader,
    response: T,
) {
    b.put_i32(0); // Length header placeholder.
    let offset = b.len();

    let mut wh = messages::ResponseHeader::default();
    wh.correlation_id = rh.correlation_id;
    wh.encode(b, T::header_version(rh.request_api_version))
        .expect("encoding ResponseHeader does not fail");
    response
        .encode(b, rh.request_api_version)
        .expect("encoding response payload does not fail");

    // Go back and write the length header.
    let len = (b.len() - offset) as u32;
    b[(offset - 4)..offset].copy_from_slice(&len.to_be_bytes());
}

/// Convert a plain topic name to a name that can be sent to
/// upstream Kafka brokers, i.e for group management requests.
/// The output topic names should conform to the Kafka topic
/// name conventions ([^a-zA-Z0-9._-]), and ideally not leak
/// any customer-specific information like collection names.
fn to_upstream_topic_name(topic: TopicName, secret: String) -> TopicName {
    let encrypted = encrypt(topic.as_bytes(), secret.as_bytes()).unwrap();
    let encoded = hex::encode(encrypted);
    TopicName::from(StrBytes::from_string(encoded))
}

/// Convert the output of [`to_upstream_topic_name`] back into
/// its plain collection name format.
fn from_upstream_topic_name(topic: TopicName, secret: String) -> TopicName {
    let decoded = hex::decode(topic.as_bytes()).unwrap();
    let decrypted = decrypt(decoded.as_slice(), secret.as_bytes()).unwrap();

    TopicName::from(StrBytes::from_utf8(Bytes::from(decrypted)).unwrap())
}

/// Convert a topic name to a name that is compatible with Kafka's
/// topic name conventions, while still being as close to the
/// original topic name as possible. These will get returned
/// to e.g `Metadata` requests when configured in order to
/// accommodate consumer systems that require restricted topic names.
fn to_downstream_topic_name(topic: TopicName) -> TopicName {
    let encoded = utf8_percent_encode(topic.as_str(), percent_encoding::NON_ALPHANUMERIC)
        .to_string()
        .replace("%", ".");
    TopicName::from(StrBytes::from_string(encoded))
}

/// Convert the output of [`to_downstream_topic_name`] back into
/// its plain collection name format
fn from_downstream_topic_name(topic: TopicName) -> TopicName {
    if topic.contains("/") {
        // Impossible for the string to be .-encoded
        return topic;
    } else {
        // String must be .-encoded, as all collection names must contain a slash
        TopicName::from(StrBytes::from_string(
            decode_safe_name(topic.to_string())
                .expect(&format!("Unable to parse topic name {topic:?}")),
        ))
    }
}

fn decode_safe_name(safe_name: String) -> anyhow::Result<String> {
    let percent_encoded = safe_name.replace(".", "%");
    percent_decode_str(percent_encoded.as_str())
        .decode_utf8()
        .and_then(|decoded| Ok(decoded.into_owned()))
        .map_err(anyhow::Error::from)
}
