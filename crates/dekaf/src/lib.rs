use anyhow::Context;
use bytes::{BufMut, Bytes};
use kafka_protocol::{
    messages::{self, ApiKey, TopicName},
    protocol::{buf::ByteBuf, Decodable, Encodable, StrBytes},
};
use tracing::instrument;

pub mod log_appender;
pub mod logging;

mod topology;
pub use topology::extract_dekaf_config;
use topology::{Collection, Partition};

mod read;
pub use read::extract_and_encode;
use read::Read;

pub mod utils;

mod task_manager;
pub use task_manager::{TaskManager, TaskState};

mod session;
pub use session::Session;

pub mod connector;
pub mod metrics_server;
pub mod registry;

mod api_client;
pub use api_client::{KafkaApiClient, KafkaClientAuth};

use aes_siv::{aead::Aead, Aes256SivAead, KeyInit, KeySizeUser};
use flow_client::client::{refresh_authorizations, RefreshToken};
use log_appender::SESSION_CLIENT_ID_FIELD_MARKER;
use percent_encoding::{percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing_record_hierarchical::SpanExt;

pub struct App {
    /// Hostname which is advertised for Kafka access.
    pub advertise_host: String,
    /// Port which is advertised for Kafka access.
    pub advertise_kafka_port: u16,
    /// Secret used to secure Prometheus endpoint
    pub secret: String,
    /// Share a single base client in order to re-use connection pools
    pub client_base: flow_client::Client,
    /// The domain name of the data-plane that we're running inside of
    pub data_plane_fqdn: String,
    /// The key used to sign data-plane access token requests
    pub data_plane_signer: jsonwebtoken::EncodingKey,
    /// The manager responsible for maintaining fresh task metadata
    pub task_manager: Arc<TaskManager>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
#[serde(deny_unknown_fields)]
pub struct DeprecatedConfigOptions {
    #[serde(default = "bool::<false>")]
    pub strict_topic_names: bool,
    #[serde(default)]
    pub deletions: connector::DeletionMode,
}

pub struct UserAuth {
    client: flow_client::Client,
    refresh_token: RefreshToken,
    access_token: String,
    claims: models::authorizations::ControlClaims,
    config: DeprecatedConfigOptions,
}

pub struct TaskAuth {
    client: flow_client::Client,
    task_name: String,
    config: connector::DekafConfig,
    task_state_listener: task_manager::TaskStateListener,

    // When access token expires
    exp: time::OffsetDateTime,
}

pub enum SessionAuthentication {
    User(UserAuth),
    Task(TaskAuth),
}

impl SessionAuthentication {
    pub fn valid_until(&self) -> SystemTime {
        match self {
            SessionAuthentication::User(user) => {
                std::time::UNIX_EPOCH + std::time::Duration::new(user.claims.exp, 0)
            }
            SessionAuthentication::Task(task) => task.exp.into(),
        }
    }

    pub async fn flow_client(&mut self) -> anyhow::Result<&flow_client::Client> {
        match self {
            SessionAuthentication::User(auth) => auth.authenticated_client().await,
            SessionAuthentication::Task(auth) => auth.authenticated_client().await,
        }
    }

    pub fn refresh_gazette_clients(&mut self) {
        match self {
            SessionAuthentication::User(auth) => {
                auth.client = auth.client.clone().with_fresh_gazette_client();
            }
            SessionAuthentication::Task(auth) => {
                auth.client = auth.client.clone().with_fresh_gazette_client();
            }
        }
    }

    pub fn deletions(&self) -> connector::DeletionMode {
        match self {
            SessionAuthentication::User(user_auth) => user_auth.config.deletions,
            SessionAuthentication::Task(task_auth) => task_auth.config.deletions,
        }
    }
}

impl UserAuth {
    pub async fn authenticated_client(&mut self) -> anyhow::Result<&flow_client::Client> {
        let (access, refresh) = refresh_authorizations(
            &self.client,
            Some(self.access_token.to_owned()),
            Some(self.refresh_token.to_owned()),
        )
        .await?;

        if access != self.access_token {
            self.access_token = access.clone();
            self.refresh_token = refresh;

            self.client = self
                .client
                .clone()
                .with_user_access_token(Some(access))
                .with_fresh_gazette_client();
        }

        Ok(&self.client)
    }
}

impl TaskAuth {
    pub fn new(
        client: flow_client::Client,
        task_name: String,
        config: connector::DekafConfig,
        task_state_listener: task_manager::TaskStateListener,
        exp: time::OffsetDateTime,
    ) -> Self {
        Self {
            client,
            task_name,
            config,
            task_state_listener,
            exp,
        }
    }
    pub async fn authenticated_client(&mut self) -> anyhow::Result<&flow_client::Client> {
        if (self.exp - time::OffsetDateTime::now_utc()).whole_seconds() < 60 {
            let TaskState {
                access_token: token,
                access_token_claims: claims,
                ..
            } = self.task_state_listener.get().await?;

            self.client = self
                .client
                .clone()
                .with_user_access_token(Some(token))
                .with_fresh_gazette_client();
            self.exp =
                time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(claims.exp as i64);
        }

        Ok(&self.client)
    }
}

#[derive(Debug, thiserror::Error)]
enum DekafError {
    #[error("Authentication failed: {0}")]
    Authentication(String),
    #[error("{0}")]
    Unknown(
        #[from]
        #[source]
        anyhow::Error,
    ),
}

impl App {
    #[tracing::instrument(level = "info", err(Debug, level = "warn"), skip(self, password))]
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<SessionAuthentication, DekafError> {
        let username = if let Ok(decoded) = decode_safe_name(username.to_string()) {
            decoded
        } else {
            username.to_string()
        };

        if models::Materialization::regex().is_match(username.as_ref())
            && !username.starts_with("{")
        {
            let listener = self.task_manager.get_listener(&username);
            // Ask the agent for information about this task, as well as a short-lived
            // control-plane access token authorized to interact with the avro schemas table
            let TaskState {
                access_token: token,
                access_token_claims: claims,
                spec,
                ..
            } = listener.get().await?;

            // Decrypt this materialization's endpoint config
            let config = topology::extract_dekaf_config(&spec).await?;

            let labels = spec
                .shard_template
                .as_ref()
                .context("missing shard template")?
                .labels
                .as_ref()
                .context("missing shard labels")?;
            let labels =
                labels::shard::decode_labeling(labels).context("parsing shard labeling")?;

            // This marks this Session as being associated with the task name contained in `username`.
            // We only set this after successfully validating that this task exists and is a Dekaf
            // materialization. Otherwise we will either log auth errors attempting to append to
            // a journal that doesn't exist, or possibly log confusing errors to a different task's logs entirely.
            logging::get_log_forwarder()
                .map(|f| f.set_task_name(username.clone(), labels.build.clone()));

            // 3. Validate that the provided password matches the task's bearer token
            if password != config.token {
                return Err(DekafError::Authentication(
                    "Invalid username or password".into(),
                ));
            }

            logging::set_log_level(labels.log_level());

            Ok(SessionAuthentication::Task(TaskAuth::new(
                self.client_base
                    .clone()
                    .with_user_access_token(Some(token))
                    .with_fresh_gazette_client(),
                username,
                config,
                listener,
                time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(claims.exp as i64),
            )))
        } else if username.contains("{") {
            // Since we don't have a task, we also don't have a logs journal to write to,
            // so we should disable log forwarding for this session.
            logging::get_log_forwarder().map(|f| f.shutdown());

            let raw_token = String::from_utf8(
                base64::decode(password)
                    .map_err(anyhow::Error::from)?
                    .to_vec(),
            )
            .map_err(anyhow::Error::from)?;
            let refresh: RefreshToken =
                serde_json::from_str(raw_token.as_str()).map_err(anyhow::Error::from)?;

            let (access, refresh) =
                refresh_authorizations(&self.client_base, None, Some(refresh)).await?;

            let client = self
                .client_base
                .clone()
                .with_user_access_token(Some(access.clone()))
                .with_fresh_gazette_client();

            let claims = flow_client::client::client_claims(&client)?;

            let config: DeprecatedConfigOptions = serde_json::from_str(&username)
                .context("failed to parse username as a JSON object")?;

            Ok(SessionAuthentication::User(UserAuth {
                client,
                access_token: access,
                refresh_token: refresh,
                claims,
                config,
            }))
        } else {
            return Err(DekafError::Authentication(
                "Invalid username or password".into(),
            ));
        }
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
    let start_time = SystemTime::now();
    use messages::*;
    tracing::debug!(?api_key, v = version, "handling API request");
    let ret = match api_key {
        ApiKey::ApiVersionsKey => {
            // https://github.com/confluentinc/librdkafka/blob/e03d3bb91ed92a38f38d9806b8d8deffe78a1de5/src/rdkafka_request.c#L2823
            let (header, request) = dec_request(frame, version)?;
            if let Some(client_id) = &header.client_id {
                tracing::Span::current()
                    .record_hierarchical(SESSION_CLIENT_ID_FIELD_MARKER, client_id.to_string());
                tracing::info!("Got client ID!");
            }
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
            let metadata_response =
                tokio::time::timeout(Duration::from_secs(30), session.metadata(request))
                    .await
                    .context("metadata request timed out")??;
            Ok(enc_resp(out, &header, metadata_response))
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
    let handle_duration = SystemTime::now().duration_since(start_time)?;

    metrics::histogram!("dekaf_api_call_time", "api_key" => format!("{:?}",api_key))
        .record(handle_duration.as_secs_f32() as f64);

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
/// NOTE that the output of this function must be deterministic,
/// that is: it cannot use a random nonce like you normally would
/// when encrypting data.
fn to_upstream_topic_name(topic: TopicName, secret: String, nonce: String) -> TopicName {
    let (cipher, nonce) = create_crypto(secret, nonce);

    let encrypted = cipher.encrypt(&nonce, topic.as_bytes()).unwrap();
    let encoded = hex::encode(encrypted);
    TopicName::from(StrBytes::from_string(encoded))
}

/// Convert the output of [`to_upstream_topic_name`] back into
/// its plain collection name format.
fn from_upstream_topic_name(topic: TopicName, secret: String, nonce: String) -> TopicName {
    let (cipher, nonce) = create_crypto(secret, nonce);
    let decoded = hex::decode(topic.as_bytes()).unwrap();
    let decrypted = cipher.decrypt(&nonce, decoded.as_slice()).unwrap();

    TopicName::from(StrBytes::from_utf8(Bytes::from(decrypted)).unwrap())
}

fn create_crypto(secret: String, nonce: String) -> (Aes256SivAead, aes_siv::Nonce) {
    let mut key = secret.as_bytes().to_vec();
    key.resize(Aes256SivAead::key_size(), 0);

    let mut nonce = nonce.as_bytes().to_vec();
    // "Nonce = GenericArray<u8, U16>"
    nonce.resize(16, 0);

    let cipher = Aes256SivAead::new_from_slice(&key[..]).unwrap();
    let nonce = aes_siv::Nonce::from_slice(&nonce[..]);

    return (cipher, *nonce);
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

/// A "shard template id" is normally the most-specific task identifier
/// used throughout the data-plane. Dekaf materializations, on the other hand, have predictable
/// shard template IDs since they never publish shards whose names could conflict.
fn dekaf_shard_template_id(task_name: &str) -> String {
    format!("materialize/{task_name}/0000000000000000/")
}

/// Modified from [this](https://github.com/serde-rs/serde/issues/368#issuecomment-1579475447)
/// comment in a thread requesting literal default values in Serde, this method uses
/// const generics to let you specify a default boolean value for Serde to use when
/// deserializing a struct field.
///
/// ex: `#[serde(default = "bool::<false>")]`
fn bool<const U: bool>() -> bool {
    U
}

#[cfg(test)]
mod test {
    use crate::{from_upstream_topic_name, to_upstream_topic_name};
    use kafka_protocol::{messages::TopicName, protocol::StrBytes};

    #[test]
    fn test_encryption_deterministic() {
        let enc_1 = to_upstream_topic_name(
            TopicName::from(StrBytes::from_static_str("Test Topic")),
            "pizza".to_string(),
            "sauce".to_string(),
        );
        let enc_2 = to_upstream_topic_name(
            TopicName::from(StrBytes::from_static_str("Test Topic")),
            "pizza".to_string(),
            "sauce".to_string(),
        );

        assert_eq!(enc_1, enc_2);
    }

    #[test]
    fn test_encrypt_decrypt() {
        let encrypted = to_upstream_topic_name(
            TopicName::from(StrBytes::from_static_str("Test Topic")),
            "pizza".to_string(),
            "sauce".to_string(),
        );

        let decrypted =
            from_upstream_topic_name(encrypted, "pizza".to_string(), "sauce".to_string());

        assert_eq!(decrypted.as_str(), "Test Topic");
    }
}
