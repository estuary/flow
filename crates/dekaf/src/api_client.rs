use anyhow::{anyhow, bail, Context};
use bytes::{Bytes, BytesMut};
use futures::{
    lock::{MappedMutexGuard, Mutex, MutexGuard},
    Sink,
};
use futures::{SinkExt, Stream, TryStreamExt};
use kafka_protocol::{
    messages::{
        sasl_authenticate_request::SaslAuthenticateRequestBuilder,
        sasl_handshake_request::SaslHandshakeRequestBuilder, ApiKey, RequestHeader, ResponseHeader,
    },
    protocol::{Builder, Decodable, Encodable, Request, StrBytes},
};
use rsasl::{config::SASLConfig, mechname::Mechname, prelude::SASLClient};
use std::boxed::Box;
use std::{io::BufWriter, pin::Pin, sync::Arc};
use tokio::net::TcpStream;
use tokio_rustls::{
    client::TlsStream,
    rustls::{pki_types::ServerName, ClientConfig, RootCertStore},
    TlsConnector,
};
use url::Url;

struct BoxedDuplexConnection {
    pub reader: Pin<Box<dyn Stream<Item = Result<BytesMut, std::io::Error>> + Send + Unpin>>,
    pub writer: Pin<Box<dyn Sink<Bytes, Error = std::io::Error> + Send + Unpin>>,
}

/// Exposes a low level Kafka wire protocol client. Used when we need to
/// make API calls at the wire protocol level, as opposed to higher-level producer/consumer
/// APIs that Kafka client libraries usually expose. Currently used to serve
/// the group management protocol requests by proxying to a real Kafka broker.
pub struct KafkaApiClient {
    /// A raw IO stream to the Kafka broker.
    connection: Arc<Mutex<Option<BoxedDuplexConnection>>>,
    broker_url: String,
    sasl_config: Arc<SASLConfig>,
}

impl KafkaApiClient {
    pub fn new(broker_url: &str, sasl_config: Arc<SASLConfig>) -> Self {
        Self {
            connection: Default::default(),
            broker_url: broker_url.to_string(),
            sasl_config,
        }
    }

    async fn get_mechanisms(&self) -> anyhow::Result<Vec<String>> {
        // In order to pick the best method to use, we need to know the options supported by the server.
        // `SaslHandshakeResponse` contains this list, but you have to send a `SaslHandshakeRequest` to get it,
        // and if you send an invalid mechanism, Kafka will close the connection. So we need to open a throw-away
        // connection and send an invalid `SaslHandshakeRequest` all in order to discover the supported mechanisms.
        let mut new_conn = self.connect().await?;

        let discovery_handshake_req = SaslHandshakeRequestBuilder::default().build()?;

        let handshake_resp = self
            ._send_request(discovery_handshake_req, None, &mut new_conn)
            .await?;

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
    pub async fn validate_auth(&self) -> anyhow::Result<()> {
        self.get_connection().await?;
        Ok(())
    }

    async fn connect(&self) -> anyhow::Result<BoxedDuplexConnection> {
        // Establish a TCP connection to the Kafka broker

        let parsed_url = Url::parse(&self.broker_url)?;

        let mut root_cert_store = RootCertStore::empty();
        root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let tls_connector = TlsConnector::from(Arc::new(tls_config));

        let hostname = parsed_url
            .host()
            .ok_or(anyhow!("Broker URL must contain a hostname"))?;
        let port = parsed_url.port().unwrap_or(9092);
        let dnsname = ServerName::try_from(hostname.to_string())?;

        tracing::debug!(port = port,host = ?hostname, "Attempting to connect");
        let tcp_stream = TcpStream::connect(format!("{hostname}:{port}")).await?;
        let stream = tls_connector.connect(dnsname, tcp_stream).await?;
        tracing::debug!(port = port,host = ?hostname, "Connectione established");

        // https://kafka.apache.org/protocol.html#protocol_common
        // All requests and responses originate from the following:
        // > RequestOrResponse => Size (RequestMessage | ResponseMessage)
        // >   Size => int32
        let (reader, writer) = tokio::io::split(stream);

        let framed_reader = tokio_util::codec::FramedRead::new(
            reader,
            tokio_util::codec::LengthDelimitedCodec::builder()
                .big_endian()
                .length_field_length(4)
                .max_frame_length(1 << 27) // 128 MiB
                .new_codec(),
        );

        let framed_writer = tokio_util::codec::FramedWrite::new(
            writer,
            tokio_util::codec::LengthDelimitedCodec::builder()
                .big_endian()
                .length_field_length(4)
                .max_frame_length(1 << 27) // 128 MiB
                .new_codec(),
        );

        // TODO: Automatically close the connection after x minutes of inactivity

        Ok(BoxedDuplexConnection {
            reader: Pin::new(Box::new(framed_reader)),
            writer: Pin::new(Box::new(framed_writer)),
        })
    }

    /// Responsible for handing out an opened, authenticated connection to the broker. Returns a locked mutex guard.
    async fn get_connection(
        &self,
    ) -> anyhow::Result<MappedMutexGuard<Option<BoxedDuplexConnection>, BoxedDuplexConnection>>
    {
        let mut maybe_conn = self.connection.lock().await;
        if maybe_conn.is_none() {
            let mut new_conn = self.connect().await?;

            // Newly created connections need to be authenticated
            self.sasl_auth(&mut new_conn).await?;

            *maybe_conn = Some(new_conn);
        }

        let ret = MutexGuard::map(maybe_conn, |c| c.as_mut().unwrap());

        Ok(ret)
    }

    async fn _send_request<Req: Request>(
        &self,
        req: Req,
        header: Option<RequestHeader>,
        conn: &mut BoxedDuplexConnection,
    ) -> anyhow::Result<Req::Response> {
        let mut req_buf = BytesMut::new();

        let req_api_key = ApiKey::try_from(Req::KEY).expect("API key should exist");

        let api_version = Req::VERSIONS.max;

        let request_header = match header {
            Some(h) => h,
            None => RequestHeader::builder()
                .request_api_key(Req::KEY)
                .request_api_version(api_version)
                .build()?,
        };

        request_header.encode(
            &mut req_buf,
            Req::header_version(request_header.request_api_version),
        )?;

        req.encode(&mut req_buf, request_header.request_api_version)?;

        tracing::debug!(api_key_name=?req_api_key, api_key=Req::KEY, api_version=request_header.request_api_version, "Sending request");

        // Then write the message
        conn.writer.send(req_buf.freeze()).await?;

        // Now we can read the whole message. Let's not worry about streaming this
        // for the moment. I don't think we'll get messages large enough to cause
        // issues with memory consumption... but I've been wrong about that before.
        let mut response_frame = conn
            .reader
            .try_next()
            .await?
            .context("connection unexpectedly closed")?;

        let response_header_version = req_api_key.response_header_version(api_version);

        let resp_header =
            ResponseHeader::decode(&mut response_frame, response_header_version).unwrap();

        tracing::debug!(response_header_version, resp_header=?resp_header, "Got response header");

        let resp = Req::Response::decode(&mut response_frame, api_version)?;

        Ok(resp)
    }

    /// Send a request and wait for the response. Per Kafka wire protocol docs:
    /// The server guarantees that on a single TCP connection, requests will be processed in the order
    /// they are sent and responses will return in that order as well. The broker's request processing
    /// allows only a single in-flight request per connection in order to guarantee this ordering.
    /// https://kafka.apache.org/protocol.html
    pub async fn send_request<Req: Request>(
        &self,
        req: Req,
        header: Option<RequestHeader>,
    ) -> anyhow::Result<Req::Response> {
        // TODO: This could be optimized by pipelining.
        let mut conn = self.get_connection().await?;

        return self._send_request(req, header, &mut conn).await;
    }

    async fn sasl_auth(&self, conn: &mut BoxedDuplexConnection) -> anyhow::Result<()> {
        let sasl = SASLClient::new(self.sasl_config.clone());

        let mechanisms = self.get_mechanisms().await?;

        let maybe_offered_mechanisms: Result<Vec<_>, _> = mechanisms
            .iter()
            .map(|m| Mechname::parse(m.as_str().as_bytes()))
            .collect();

        let offered_mechanisms = maybe_offered_mechanisms?;

        // select the best offered mechanism that the user enabled in the `config`
        let mut session = sasl.start_suggested(offered_mechanisms.iter())?;

        let selected_mechanism = session.get_mechname().as_str().to_owned();

        tracing::debug!(mechamism=?selected_mechanism, "Starting SASL request with handshake");

        // Now we know which mechanism we want to request
        let handshake_req = SaslHandshakeRequestBuilder::default()
            .mechanism(StrBytes::from_utf8(Bytes::from(selected_mechanism))?)
            .build()?;

        let handshake_resp = self._send_request(handshake_req, None, conn).await?;

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
            let authenticate_request = SaslAuthenticateRequestBuilder::default()
                .auth_bytes(Bytes::from(state_buf.into_inner()?))
                .build()?;

            let auth_resp = self._send_request(authenticate_request, None, conn).await?;

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

        Ok(())
    }
}
