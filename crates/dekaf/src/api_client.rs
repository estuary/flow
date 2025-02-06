use anyhow::{anyhow, bail, Context};
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, TryStreamExt};
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{self, ApiKey},
    protocol::{self, Decodable, Encodable, Request},
};
use rsasl::{config::SASLConfig, mechname::Mechname, prelude::SASLClient};
use rustls::RootCertStore;
use std::{
    boxed::Box,
    collections::HashMap,
    fmt::Debug,
    io,
    time::{Duration, SystemTime},
};
use std::{io::BufWriter, pin::Pin, sync::Arc};
use tokio::sync::OnceCell;
use tokio_rustls::rustls;
use tokio_util::codec;
use tracing::instrument;
use url::Url;

type BoxedKafkaConnection = Pin<
    Box<
        tokio_util::codec::Framed<
            tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
            codec::LengthDelimitedCodec,
        >,
    >,
>;

static ROOT_CERT_STORE: OnceCell<Arc<RootCertStore>> = OnceCell::const_new();

#[tracing::instrument(skip_all)]
async fn async_connect(broker_url: &str) -> anyhow::Result<BoxedKafkaConnection> {
    // Establish a TCP connection to the Kafka broker

    let parsed_url = Url::parse(broker_url)?;

    let root_certs = ROOT_CERT_STORE
        .get_or_try_init(|| async {
            let mut certs = rustls::RootCertStore::empty();
            certs.add_parsable_certificates(
                rustls_native_certs::load_native_certs().expect("failed to load native certs"),
            );
            Ok::<Arc<RootCertStore>, anyhow::Error>(Arc::new(certs))
        })
        .await?;

    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_certs.to_owned())
        .with_no_client_auth();

    let tls_connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));

    let hostname = parsed_url
        .host()
        .ok_or(anyhow!("Broker URL must contain a hostname"))?;
    let port = parsed_url.port().unwrap_or(9092);
    let dnsname = rustls::pki_types::ServerName::try_from(hostname.to_string())?;

    tracing::debug!(port = port,host = ?hostname, "Attempting to connect");
    let tcp_stream = tokio::net::TcpStream::connect(format!("{hostname}:{port}")).await?;

    // Let's keep this stream alive
    let sock_ref = socket2::SockRef::from(&tcp_stream);
    let ka = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(20))
        .with_interval(Duration::from_secs(20));
    sock_ref.set_tcp_keepalive(&ka)?;

    let stream = tls_connector.connect(dnsname, tcp_stream).await?;
    tracing::debug!(port = port,host = ?hostname, "Connection established");

    // https://kafka.apache.org/protocol.html#protocol_common
    // All requests and responses originate from the following:
    // > RequestOrResponse => Size (RequestMessage | ResponseMessage)
    // >   Size => int32
    let framed = tokio_util::codec::Framed::new(
        stream,
        tokio_util::codec::LengthDelimitedCodec::builder()
            .big_endian()
            .length_field_length(4)
            .max_frame_length(1 << 27) // 128 MiB
            .new_codec(),
    );

    Ok(Box::pin(framed))
}

#[tracing::instrument(skip_all)]
async fn get_supported_sasl_mechanisms(broker_url: &str) -> anyhow::Result<Vec<String>> {
    // In order to pick the best method to use, we need to know the options supported by the server.
    // `SaslHandshakeResponse` contains this list, but you have to send a `SaslHandshakeRequest` to get it,
    // and if you send an invalid mechanism, Kafka will close the connection. So we need to open a throw-away
    // connection and send an invalid `SaslHandshakeRequest` all in order to discover the supported mechanisms.
    let mut new_conn = async_connect(broker_url)
        .await
        .map_err(|e| io::Error::other(e))?;

    let discovery_handshake_req = messages::SaslHandshakeRequest::default();

    let handshake_resp = send_request(&mut new_conn, discovery_handshake_req, None).await?;

    let offered_mechanisms: Vec<_> = handshake_resp
        .mechanisms
        .iter()
        .cloned()
        .map(|m| m.to_string())
        .collect();

    tracing::debug!(
        mechanisms = ?offered_mechanisms,
        "Discovered supported SASL mechanisms"
    );

    Ok(offered_mechanisms)
}

#[tracing::instrument(skip_all)]
async fn send_request<Req: protocol::Request + Debug>(
    conn: &mut BoxedKafkaConnection,
    req: Req,
    header: Option<messages::RequestHeader>,
) -> anyhow::Result<Req::Response> {
    let mut req_buf = BytesMut::new();

    // The API key indicate which API is being called. See here for
    // a mapping of API keys to messages:
    // https://kafka.apache.org/protocol.html#protocol_api_keys
    let req_api_key = messages::ApiKey::try_from(Req::KEY).expect("API key should exist");

    let request_header = match header {
        Some(h) => h,
        None => messages::RequestHeader::default()
            .with_request_api_key(Req::KEY)
            .with_request_api_version(Req::VERSIONS.max),
    };

    // Kafka APIs are versioned. This is the version of the request being made
    let request_api_version = request_header.request_api_version;

    // 1. Serialize the header based on the API version
    request_header.encode(
        &mut req_buf,
        // Kafka message headers themselves are also versioned, so in order to
        // properly encode a message, we need to know which header version to use
        // in addition to which body version. [`kafka_protocol::protocol::HeaderVersion`]
        // provides this mapping for each message type.
        Req::header_version(request_api_version),
    )?;

    tracing::debug!(api_key_name=?req_api_key, api_key=Req::KEY, api_version=request_api_version, "Sending request");

    // 2. Serialize the message based on the request API version
    req.encode(&mut req_buf, request_api_version)?;

    // 3. Then write out the message
    conn.send(req_buf.freeze()).await?;

    let mut response_frame = conn
        .try_next()
        .await?
        .context("connection unexpectedly closed")?;

    // To further muddy the waters, responses are also messages wrapped with a header,
    // and those header versions are yet again different, and need to be looked up based on
    // the request version. [`kafka_protocol::messages::ApiKey::response_header_version()`]
    // conveniently provides this mapping.
    let response_header_version = req_api_key.response_header_version(request_api_version);

    let resp_header =
        messages::ResponseHeader::decode(&mut response_frame, response_header_version).unwrap();

    tracing::debug!(response_header_version, resp_header=?resp_header, "Got response header");

    let resp = Req::Response::decode(&mut response_frame, request_api_version)?;

    Ok(resp)
}

#[tracing::instrument(skip_all)]
async fn sasl_auth(
    conn: &mut BoxedKafkaConnection,
    broker_url: &str,
    sasl_config: Arc<SASLConfig>,
) -> anyhow::Result<()> {
    let sasl = SASLClient::new(sasl_config.clone());

    let mechanisms = get_supported_sasl_mechanisms(broker_url).await?;

    let offered_mechanisms = mechanisms
        .iter()
        .map(|m| Mechname::parse(m.as_str().as_bytes()))
        .collect::<Result<Vec<_>, _>>()?;

    // select the best offered mechanism that the user enabled in the `config`
    let mut session = sasl.start_suggested(offered_mechanisms.iter())?;

    let selected_mechanism = session.get_mechname().as_str().to_owned();

    tracing::debug!(mechamism=?selected_mechanism, "Starting SASL request with handshake");

    // Now we know which mechanism we want to request
    let handshake_req = messages::SaslHandshakeRequest::default().with_mechanism(
        protocol::StrBytes::from_utf8(Bytes::from(selected_mechanism))?,
    );

    let handshake_resp = send_request(conn, handshake_req, None).await?;

    if handshake_resp.error_code > 0 {
        let err = kafka_protocol::ResponseError::try_from_code(handshake_resp.error_code)
            .map(|code| format!("{code:?}"))
            .unwrap_or(format!("Unknown error {}", handshake_resp.error_code));
        bail!(
            "Error performing SASL handshake: {err}. Supported mechanisms: {:?}",
            handshake_resp.mechanisms
        );
    }

    let mut state_buf = BufWriter::new(Vec::new());
    let mut state = session.step(None, &mut state_buf)?;

    // SASL can happen over multiple steps
    while state.is_running() {
        let authenticate_request = messages::SaslAuthenticateRequest::default()
            .with_auth_bytes(Bytes::from(state_buf.into_inner()?));

        let auth_resp = send_request(conn, authenticate_request, None).await?;

        if auth_resp.error_code > 0 {
            let err = kafka_protocol::ResponseError::try_from_code(handshake_resp.error_code)
                .map(|code| format!("{code:?}"))
                .unwrap_or(format!("Unknown error {}", handshake_resp.error_code));
            bail!(
                "Error performing SASL authentication: {err} {:?}",
                auth_resp.error_message
            )
        }
        let data = Some(auth_resp.auth_bytes.to_vec());
        state_buf = BufWriter::new(Vec::new());
        state = session.step(data.as_deref(), &mut state_buf)?;
    }

    tracing::debug!("Successfully completed SASL flow");

    Ok(())
}

async fn get_versions(
    conn: &mut BoxedKafkaConnection,
) -> anyhow::Result<messages::ApiVersionsResponse> {
    let versions = send_request(
        conn,
        messages::ApiVersionsRequest::default()
            .with_client_software_name(protocol::StrBytes::from_static_str("Dekaf"))
            .with_client_software_version(protocol::StrBytes::from_static_str("1.0")),
        None,
    )
    .await?;
    match versions.error_code.err() {
        None => {}
        Some(e) => bail!("Error connecting to broker: {e}"),
    };

    Ok(versions)
}

/// Exposes a low level Kafka wire protocol client. Used when we need to
/// make API calls at the wire protocol level, as opposed to higher-level producer/consumer
/// APIs that Kafka client libraries usually expose. Currently used to serve
/// the group management protocol requests by proxying to a real Kafka broker.
pub struct KafkaApiClient {
    /// A raw IO stream to the Kafka broker.
    conn: BoxedKafkaConnection,
    url: String,
    sasl_config: Arc<SASLConfig>,
    versions: messages::ApiVersionsResponse,
    // Sometimes we need to connect to a particular broker, be it the coordinator
    // for a particular group, or the cluster controller for whatever reason.
    // Rather than opening/closing a new connection for every request, let's
    // keep around a map of these connections that live as long as we do.
    // It's important that these child connections not outlive the parent,
    // as otherwise we won't be able to propagate disconnects correctly.
    clients: HashMap<String, KafkaApiClient>,
}

impl KafkaApiClient {
    #[instrument(name = "api_client_connect", skip(sasl_config))]
    pub async fn connect(
        broker_urls: &[String],
        sasl_config: Arc<SASLConfig>,
    ) -> anyhow::Result<Self> {
        tracing::debug!("Attempting to establish new connection");

        for url in broker_urls {
            match Self::try_connect(url, sasl_config.clone()).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    let error = e.context(format!("Failed to connect to {}", url));
                    tracing::warn!(?error, "Connection attempt failed");
                }
            }
        }

        anyhow::bail!(
            "Failed to connect to any Kafka brokers. Attempted {} brokers",
            broker_urls.len()
        )
    }

    /// Attempt to open a connection to a specific broker address
    async fn try_connect(url: &str, sasl_config: Arc<SASLConfig>) -> anyhow::Result<Self> {
        let mut conn = async_connect(url)
            .await
            .context("Failed to establish TCP connection")?;

        tracing::debug!("Authenticating connection");
        sasl_auth(&mut conn, url, sasl_config.clone())
            .await
            .context("SASL authentication failed")?;

        let versions = get_versions(&mut conn)
            .await
            .context("Failed to negotiate protocol versions")?;

        Ok(Self {
            conn,
            url: url.to_string(),
            sasl_config,
            versions,
            clients: HashMap::new(),
        })
    }

    /// Returns a [`KafkaApiClient`] for the given broker URL. If a client
    /// for that broker already exists, return it rather than creating a new one.
    async fn client_for_broker(&mut self, broker_url: &str) -> anyhow::Result<&mut Self> {
        if broker_url.eq(self.url.as_str()) {
            return Ok(self);
        }

        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.clients.entry(broker_url.to_string())
        {
            let new_client = Self::try_connect(broker_url, self.sasl_config.clone()).await?;

            entry.insert(new_client);
        }

        Ok(self
            .clients
            .get_mut(broker_url)
            .expect("guarinteed to be present"))
    }

    /// Send a request and wait for the response. Per Kafka wire protocol docs:
    /// The server guarantees that on a single TCP connection, requests will be processed in the order
    /// they are sent and responses will return in that order as well. The broker's request processing
    /// allows only a single in-flight request per connection in order to guarantee this ordering.
    /// https://kafka.apache.org/protocol.html
    pub async fn send_request<Req: protocol::Request + Debug>(
        &mut self,
        req: Req,
        header: Option<messages::RequestHeader>,
    ) -> anyhow::Result<Req::Response> {
        let start_time = SystemTime::now();

        metrics::histogram!("dekaf_pool_wait_time", "upstream_broker" => self.url.to_owned())
            .record(SystemTime::now().duration_since(start_time)?);

        let api_key = ApiKey::try_from(Req::KEY).expect("should be valid api key");

        let start_time = SystemTime::now();
        let resp = send_request(&mut self.conn, req, header).await;
        metrics::histogram!("dekaf_request_time", "api_key" => format!("{:?}",api_key), "upstream_broker" => self.url.to_owned())
            .record(SystemTime::now().duration_since(start_time)?);

        resp
    }

    #[instrument(skip(self))]
    pub async fn connect_to_group_coordinator(&mut self, key: &str) -> anyhow::Result<&mut Self> {
        let req = messages::FindCoordinatorRequest::default()
            .with_key(protocol::StrBytes::from_string(key.to_string()))
            // https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/requests/FindCoordinatorRequest.java#L119
            .with_key_type(0); // 0: consumer, 1: transaction

        let resp = self
            .send_request(
                req,
                Some(
                    messages::RequestHeader::default()
                        .with_request_api_key(messages::FindCoordinatorRequest::KEY)
                        .with_request_api_version(3),
                ),
            )
            .await?;

        let (coord_host, coord_port) = if let Some(coord) = resp.coordinators.first() {
            (coord.host.as_str(), coord.port)
        } else {
            (resp.host.as_str(), resp.port)
        };

        let coord_url = format!("tcp://{}:{}", coord_host.to_string(), coord_port);

        Ok(if coord_host.len() == 0 && coord_port == -1 {
            self
        } else {
            self.client_for_broker(&coord_url).await?
        })
    }

    /// Some APIs can only be sent to the current cluster controller broker.
    /// This method looks up the current controller and, if it's not the one
    /// we're connected to, opens up a new `[KafkaApiClient]` connected to
    /// that broker.
    ///
    /// > In a Kafka cluster, one of the brokers serves as the controller,
    /// > which is responsible for managing the states of partitions and
    /// > replicas and for performing administrative tasks like reassigning partitions.
    /// https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Controller+Internals
    #[instrument(skip(self))]
    pub async fn connect_to_controller(&mut self) -> anyhow::Result<&mut Self> {
        let req = messages::MetadataRequest::default();
        let resp = self.send_request(req, None).await?;

        let controller = resp
            .brokers
            .iter()
            .find(|broker| broker.node_id == resp.controller_id)
            .context("Failed to find controller")?;

        let controller_url = format!("tcp://{}:{}", controller.host.to_string(), controller.port);

        self.client_for_broker(&controller_url).await
    }

    pub fn supported_versions<R: Request>(
        &self,
    ) -> anyhow::Result<messages::api_versions_response::ApiVersion> {
        let api_key = R::KEY;

        let version = self
            .versions
            .api_keys
            .iter()
            .find(|version| version.api_key == api_key)
            .context(format!("Unknown API key {api_key}"))?;

        Ok(version.to_owned())
    }

    #[instrument(skip_all)]
    pub async fn ensure_topics(
        &mut self,
        topic_names: Vec<messages::TopicName>,
    ) -> anyhow::Result<()> {
        let req = messages::MetadataRequest::default()
            .with_topics(Some(
                topic_names
                    .iter()
                    .map(|name| {
                        messages::metadata_request::MetadataRequestTopic::default()
                            .with_name(Some(name.clone()))
                    })
                    .collect(),
            ))
            .with_allow_auto_topic_creation(true);

        let coord = self.connect_to_controller().await?;
        let resp = coord.send_request(req, None).await?;
        tracing::debug!(metadata=?resp, "Got metadata response");

        if resp.topics.iter().all(|topic| {
            topic
                .name
                .as_ref()
                .map(|topic_name| topic_names.contains(topic_name) && topic.error_code == 0)
                .unwrap_or(false)
        }) {
            return Ok(());
        } else {
            let mut topics_map = vec![];
            for topic_name in topic_names.into_iter() {
                topics_map.push(
                    messages::create_topics_request::CreatableTopic::default()
                        .with_name(topic_name)
                        .with_replication_factor(2)
                        .with_num_partitions(-1),
                );
            }
            let create_req = messages::CreateTopicsRequest::default().with_topics(topics_map);
            let create_resp = coord.send_request(create_req, None).await?;
            tracing::debug!(create_response=?create_resp, "Got create response");

            for topic in create_resp.topics {
                if topic.error_code > 0 {
                    let err = kafka_protocol::ResponseError::try_from_code(topic.error_code);
                    tracing::warn!(
                        topic = topic.name.to_string(),
                        error = ?err,
                        message = topic.error_message.map(|m|m.to_string()),
                        "Failed to create topic"
                    );
                    bail!("Failed to create topic");
                }
            }

            Ok(())
        }
    }
}
