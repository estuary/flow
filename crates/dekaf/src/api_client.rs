use anyhow::{anyhow, bail};
use bytes::{Buf, Bytes, BytesMut};
use futures::lock::Mutex;
use kafka_protocol::{
    messages::{
        sasl_authenticate_request::SaslAuthenticateRequestBuilder,
        sasl_handshake_request::SaslHandshakeRequestBuilder, ApiKey, ApiVersionsRequest,
        RequestHeader, SaslAuthenticateRequest,
    },
    protocol::{Builder, Decodable, Encodable, HeaderVersion, Request, StrBytes},
};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{
    client::TlsStream,
    rustls::{pki_types::ServerName, ClientConfig, RootCertStore},
    TlsConnector,
};
use url::Url;

/// Exposes a low level Kafka wire protocol client. Used when we need to
/// make API calls at the wire protocol level, as opposed to higher-level producer/consumer
/// APIs that Kafka client libraries usually expose. Currently used to serve
/// the group management protocol requests by proxying to a real Kafka broker.
pub struct KafkaApiClient {
    /// A raw IO stream to the Kafka broker.
    // TODO: Do all Kafka brokers support TLS? Should this really be
    // something like `Pin<Box<dyn AsyncRead + AsyncWrite + Send>>`?
    connection: Arc<Mutex<TlsStream<TcpStream>>>,
}

impl KafkaApiClient {
    pub async fn connect(broker_url: &str) -> anyhow::Result<Self> {
        // Establish a TCP connection to the Kafka broker

        let parsed_url = Url::parse(broker_url)?;

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

        let tcp_stream = TcpStream::connect(format!("{hostname}:{port}")).await?;
        let stream = tls_connector.connect(dnsname, tcp_stream).await?;

        Ok(Self {
            connection: Arc::new(Mutex::new(stream)),
        })
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
        let mut req_buf = BytesMut::new();

        let header = match header {
            Some(h) => h,
            None => RequestHeader::builder().request_api_key(Req::KEY).build()?,
        };

        header.encode(
            &mut req_buf,
            ApiVersionsRequest::header_version(header.request_api_version),
        )?;

        req.encode(&mut req_buf, header.request_api_version);

        // TODO: This could be optimized by pipelining.
        let mut conn = self.connection.lock().await;

        // https://kafka.apache.org/protocol.html#protocol_common
        // All requests and responses originate from the following:
        // > RequestOrResponse => Size (RequestMessage | ResponseMessage)
        // >   Size => int32

        // First write the size. Why this is int32 and not uint32, I do not know.
        conn.write_i32(i32::try_from(req_buf.len())?).await?;

        // Then write the message
        conn.write_all(&req_buf).await?;

        // Wait until we start to get the response. It will begin with a 4 byte length
        let response_len = conn.read_i32().await?;

        // Now we can read the whole message. Let's not worry about streaming this
        // for the moment. I don't think we'll get messages large enough to cause
        // issues with memory consumption... but I've been wrong about that before.
        let mut resp_buf = vec![0; usize::try_from(response_len)?];
        conn.read_exact(&mut resp_buf).await?;

        let api_key = match ApiKey::try_from((&resp_buf[0..2]).get_i16()) {
            Ok(k) => k,
            Err(_) => bail!(
                "Unknown API key in response: {}",
                (&resp_buf[0..2]).get_i16()
            ),
        };

        let api_version = (&resp_buf[2..4]).get_i16();
        let header_version = api_key.request_header_version(api_version);

        let header = RequestHeader::decode(&mut resp_buf.as_slice(), header_version).unwrap();

        if !header.request_api_key.eq(&Req::KEY) {
            bail!(format!(
                "Unexpected message respose ApiKey {}",
                header.request_api_key
            ));
        }

        let resp = Req::Response::decode(&mut resp_buf.as_slice(), header.request_api_version)?;

        Ok(resp)
    }

    pub async fn sasl_auth(&self, username: &str, password: &str) -> anyhow::Result<()> {
        let handshake_req = SaslHandshakeRequestBuilder::default()
            .mechanism(StrBytes::from_static_str("PLAIN"))
            .build()?;

        let handshake_resp = self.send_request(handshake_req, None).await?;

        if handshake_resp.error_code > 0 {
            bail!(
                "Error performing SASL handshake: {}. Supported mechanisms: {:?}",
                handshake_resp.error_code,
                handshake_resp.mechanisms
            );
        }

        let auth_bytes = format!("\0{}\0{}", username, password).into_bytes();

        let authenticate_request = SaslAuthenticateRequestBuilder::default()
            .auth_bytes(Bytes::from(auth_bytes))
            .build()?;

        let auth_resp = self.send_request(authenticate_request, None).await?;

        if auth_resp.error_code > 0 {
            bail!(
                "Error performing SASL authentication: {} {:?}",
                auth_resp.error_code,
                auth_resp.error_message
            )
        }

        Ok(())
    }
}
