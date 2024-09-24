use anyhow::{anyhow, bail, Context};
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, TryStreamExt};
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages,
    protocol::{self, Decodable, Encodable, Request},
};
use rsasl::{config::SASLConfig, mechname::Mechname, prelude::SASLClient};
use std::{boxed::Box, cell::Cell, collections::HashMap, fmt::Debug, io, time::Duration};
use std::{io::BufWriter, pin::Pin, sync::Arc};
use tokio::sync::RwLock;
use tokio_rustls::rustls;
use tokio_util::{codec, task::AbortOnDropHandle};
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

#[tracing::instrument(skip_all)]
async fn async_connect(broker_url: &str) -> anyhow::Result<BoxedKafkaConnection> {
    // Establish a TCP connection to the Kafka broker

    let parsed_url = Url::parse(broker_url)?;

    // This returns an Err indicating that the default provider is already set
    // but without this call rustls crashes with the following error:
    // `no process-level CryptoProvider available -- call CryptoProvider::install_default() before this point`
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let mut root_cert_store = rustls::RootCertStore::empty();
    root_cert_store.add_parsable_certificates(rustls_native_certs::load_native_certs()?);

    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_cert_store)
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
async fn get_supported_sasl_mechanisms(
    params: &KafkaConnectionParams,
) -> anyhow::Result<Vec<String>> {
    // In order to pick the best method to use, we need to know the options supported by the server.
    // `SaslHandshakeResponse` contains this list, but you have to send a `SaslHandshakeRequest` to get it,
    // and if you send an invalid mechanism, Kafka will close the connection. So we need to open a throw-away
    // connection and send an invalid `SaslHandshakeRequest` all in order to discover the supported mechanisms.
    let mut new_conn = async_connect(&params.broker_url)
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
    args: &KafkaConnectionParams,
) -> anyhow::Result<()> {
    let sasl = SASLClient::new(args.sasl_config.clone());

    let mechanisms = get_supported_sasl_mechanisms(args).await?;

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

#[derive(Clone)]
struct KafkaConnectionParams {
    broker_url: String,
    sasl_config: Arc<SASLConfig>,
}

impl deadpool::managed::Manager for KafkaConnectionParams {
    type Type = BoxedKafkaConnection;
    type Error = anyhow::Error;

    async fn create(&self) -> Result<BoxedKafkaConnection, anyhow::Error> {
        tracing::debug!("Attempting to establish a new connection!");
        let mut conn = async_connect(&self.broker_url).await?;
        tracing::debug!("Authenticating opened connection");
        sasl_auth(&mut conn, self).await?;
        tracing::debug!("Finished authenticating opened connection");
        Ok(conn)
    }

    async fn recycle(
        &self,
        conn: &mut BoxedKafkaConnection,
        _: &deadpool::managed::Metrics,
    ) -> deadpool::managed::RecycleResult<anyhow::Error> {
        // Other than auth, Kafka connections themselves are stateless
        // so the only thing we need to do when recycling a connection
        // is to confirm that it's still connected.
        get_versions(conn).await.map(|_| ()).map_err(|e| {
            tracing::warn!(err=?e, broker=self.broker_url, "Connection failed healthcheck");
            deadpool::managed::RecycleError::Backend(e)
        })
    }
}

type Pool = deadpool::managed::Pool<KafkaConnectionParams>;

/// Exposes a low level Kafka wire protocol client. Used when we need to
/// make API calls at the wire protocol level, as opposed to higher-level producer/consumer
/// APIs that Kafka client libraries usually expose. Currently used to serve
/// the group management protocol requests by proxying to a real Kafka broker.
#[derive(Clone)]
pub struct KafkaApiClient {
    /// A raw IO stream to the Kafka broker.
    pool: Pool,
    url: String,
    sasl_config: Arc<SASLConfig>,
    versions: messages::ApiVersionsResponse,
    // Maintain a mapping of broker URI to API Client.
    // The same map should be shared between all clients
    // and should be propagated to newly created clients
    // when a new broker address is encounted.
    clients: Arc<RwLock<HashMap<String, KafkaApiClient>>>,
    _pool_connection_reaper: Arc<AbortOnDropHandle<()>>,
}

impl KafkaApiClient {
    /// Returns a [`KafkaApiClient`] for the given broker URL.
    /// If a client for that broker already exists, return it
    /// rather than creating a new one.
    pub async fn connect_to(&self, broker_url: &str) -> anyhow::Result<Self> {
        if broker_url.eq(self.url.as_str()) {
            return Ok(self.to_owned());
        }

        if let Some(client) = self.clients.read().await.get(broker_url) {
            return Ok(client.clone());
        }

        let mut clients = self.clients.clone().write_owned().await;

        // It's possible that between the check above and when we successfully acquired the write lock
        // someone else already acquired the write lock and created/stored this new client
        if let Some(client) = clients.get(broker_url) {
            return Ok(client.clone());
        }

        let new_client = Self::connect(broker_url, self.sasl_config.clone()).await?;

        clients.insert(broker_url.to_owned(), new_client.clone());

        Ok(new_client)
    }

    #[instrument(name = "api_client_connect", skip(sasl_config))]
    pub async fn connect(broker_url: &str, sasl_config: Arc<SASLConfig>) -> anyhow::Result<Self> {
        let pool = Pool::builder(KafkaConnectionParams {
            broker_url: broker_url.to_owned(),
            sasl_config: sasl_config.clone(),
        })
        .build()?;

        // Close idle connections, and any free connection older than 30m.
        // It seems that after running for a while, connections can get into
        // a broken state where every response returns an error. This, plus
        // the healthcheck when recycling a connection solves that problem.
        let reap_interval = Duration::from_secs(30);
        let max_age = Duration::from_secs(60 * 30);
        let max_idle = Duration::from_secs(60);
        let reaper = tokio_util::task::AbortOnDropHandle::new(tokio::spawn({
            let pool = pool.clone();
            let broker_url = broker_url.to_string();
            async move {
                loop {
                    let pool_state = pool.status();

                    metrics::gauge!("pool_size", "upstream_broker" => broker_url.to_owned())
                        .set(pool_state.size as f64);
                    metrics::gauge!("pool_available", "upstream_broker" => broker_url.to_owned())
                        .set(pool_state.available as f64);
                    metrics::gauge!("pool_waiting", "upstream_broker" => broker_url.to_owned())
                        .set(pool_state.waiting as f64);

                    let age_sum = Cell::new(Duration::ZERO);
                    let idle_sum = Cell::new(Duration::ZERO);
                    let connections = Cell::new(0);
                    tokio::time::sleep(reap_interval).await;
                    pool.retain(|_, metrics: deadpool::managed::Metrics| {
                        age_sum.set(age_sum.get() + metrics.age());
                        idle_sum.set(idle_sum.get() + metrics.last_used());
                        connections.set(connections.get() + 1);
                        metrics.age() < max_age && metrics.last_used() < max_idle
                    });

                    metrics::gauge!("pool_connection_avg_age", "upstream_broker" => broker_url.to_owned()).set(if connections.get() > 0 { age_sum.get()/connections.get() } else { Duration::ZERO });
                    metrics::gauge!("pool_connection_avg_idle", "upstream_broker" => broker_url.to_owned()).set(if connections.get() > 0 { idle_sum.get()/connections.get() } else { Duration::ZERO });
                }
            }
        }));

        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(deadpool::managed::PoolError::Backend(e)) => return Err(e),
            Err(e) => {
                anyhow::bail!(e)
            }
        };

        let versions = get_versions(conn.as_mut()).await?;
        drop(conn);

        Ok(Self {
            pool,
            url: broker_url.to_string(),
            sasl_config: sasl_config,
            versions,
            clients: Arc::new(RwLock::new(HashMap::new())),
            _pool_connection_reaper: Arc::new(reaper),
        })
    }

    /// Send a request and wait for the response. Per Kafka wire protocol docs:
    /// The server guarantees that on a single TCP connection, requests will be processed in the order
    /// they are sent and responses will return in that order as well. The broker's request processing
    /// allows only a single in-flight request per connection in order to guarantee this ordering.
    /// https://kafka.apache.org/protocol.html
    pub async fn send_request<Req: protocol::Request + Debug>(
        &self,
        req: Req,
        header: Option<messages::RequestHeader>,
    ) -> anyhow::Result<Req::Response> {
        // TODO: This could be optimized by pipelining.
        let mut conn = match self.pool.get().await {
            Ok(c) => c,
            Err(deadpool::managed::PoolError::Backend(e)) => return Err(e),
            Err(e) => {
                anyhow::bail!(e)
            }
        };

        send_request(conn.as_mut(), req, header).await
    }

    #[instrument(skip(self))]
    pub async fn connect_to_group_coordinator(&self, key: &str) -> anyhow::Result<KafkaApiClient> {
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

        let (coord_host, coord_port) = if resp.coordinators.len() > 0 {
            let coord = resp.coordinators.get(0).expect("already checked length");
            (coord.host.as_str(), coord.port)
        } else {
            (resp.host.as_str(), resp.port)
        };

        let coord_url = format!("tcp://{}:{}", coord_host.to_string(), coord_port);

        Ok(if coord_host.len() == 0 && coord_port == -1 {
            self.to_owned()
        } else {
            self.connect_to(&coord_url).await?
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
    pub async fn connect_to_controller(&self) -> anyhow::Result<KafkaApiClient> {
        let req = messages::MetadataRequest::default();
        let resp = self.send_request(req, None).await?;

        let controller = resp
            .brokers
            .get(&resp.controller_id)
            .context("Failed to find controller")?;

        let controller_url = format!("tcp://{}:{}", controller.host.to_string(), controller.port);

        self.connect_to(&controller_url).await
    }

    pub fn supported_versions<R: Request>(
        &self,
    ) -> anyhow::Result<messages::api_versions_response::ApiVersion> {
        let api_key = R::KEY;

        let version = self
            .versions
            .api_keys
            .get(&api_key)
            .context(format!("Unknown API key {api_key}"))?;

        Ok(version.to_owned())
    }

    #[instrument(skip_all)]
    pub async fn ensure_topics(&self, topic_names: Vec<messages::TopicName>) -> anyhow::Result<()> {
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

        if resp
            .topics
            .iter()
            .all(|(name, topic)| topic_names.contains(&name) && topic.error_code == 0)
        {
            return Ok(());
        } else {
            let mut topics_map = kafka_protocol::indexmap::IndexMap::new();
            for topic_name in topic_names.into_iter() {
                topics_map.insert(
                    topic_name,
                    messages::create_topics_request::CreatableTopic::default()
                        .with_replication_factor(2)
                        .with_num_partitions(-1),
                );
            }
            let create_req = messages::CreateTopicsRequest::default().with_topics(topics_map);
            let create_resp = coord.send_request(create_req, None).await?;
            tracing::debug!(create_response=?create_resp, "Got create response");

            for (name, topic) in create_resp.topics {
                if topic.error_code > 0 {
                    let err = kafka_protocol::ResponseError::try_from_code(topic.error_code);
                    tracing::warn!(
                        topic = name.to_string(),
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
