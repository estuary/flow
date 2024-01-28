use anyhow::Context;
use bytes::BufMut;
use futures::{FutureExt, TryStreamExt};
use kafka_protocol::{
    messages::{self, metadata_request::MetadataRequestTopic, MetadataResponse},
    protocol::{Builder, Decodable, Encodable, StrBytes},
};
use proto_gazette::broker;
use std::collections::BTreeMap;
use tokio::io::AsyncWriteExt;
use tracing_subscriber::{filter::LevelFilter, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into()) // Otherwise it's ERROR.
        .from_env_lossy();

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    let mut stop = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for CTRL-C")
    }
    .shared();

    let client = postgrest::Postgrest::new(PUBLIC_ENDPOINT).insert_header("apikey", PUBLIC_TOKEN);

    let listener = tokio::net::TcpListener::bind(format!("[::]:{ADVERTISE_PORT}"))
        .await
        .context("failed to bind server port")?;
    tracing::info!(ADVERTISE_PORT, "now listening");

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (socket, addr) = accept?;

                let dekaf = Dekaf{
                    client: client.clone(),
                    cached_collections: BTreeMap::new(),
                    streams: BTreeMap::new(),
                };
                tokio::spawn(serve_connection(dekaf, socket, addr, stop.clone()));
            }
            _ = &mut stop => break,
        }
    }

    Ok(())
}

async fn build_journal_client(
    client: &postgrest::Postgrest,
    collection: &str,
) -> anyhow::Result<gazette::journal::Client> {
    let body = serde_json::json!({
        "prefixes": [collection],
    })
    .to_string();

    #[derive(serde::Deserialize)]
    struct Auth {
        token: String,
        gateway_url: String,
    }

    let auth: [Auth; 1] = client
        .rpc("gateway_auth_token", body)
        .build()
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .context("requesting data plane gateway auth token")?
        .json()
        .await?;

    tracing::info!(
        token = auth[0].token,
        gateway = auth[0].gateway_url,
        "data-plane token"
    );

    let router = gazette::journal::Router::new(
        &auth[0].gateway_url,
        gazette::Interceptor::new(Some(auth[0].token.clone()))
            .context("failed to build gazette router")?,
        "dekaf",
    )?;
    let client = gazette::journal::Client::new(Default::default(), router);

    Ok(client)
}

async fn start_topic_stream(
    client: gazette::journal::Client,
    collection: &str,
    offset: i64,
) -> anyhow::Result<gazette::journal::Docs> {
    let lr = broker::ListRequest {
        selector: Some(broker::LabelSelector {
            include: Some(labels::build_set([(labels::COLLECTION, collection)])),
            exclude: None,
        }),
    };
    let mut listing = client.list(lr).await?;

    tracing::info!(collection, offset, journals=?ops::DebugJson(&listing.journals), "starting new stream for topic");

    if listing.journals.len() != 1 {
        anyhow::bail!("this preview implementation supports collections with only one partition, but {collection} has {}", listing.journals.len());
    }
    let journal = listing.journals.pop().unwrap();

    let stream = client.read_docs(broker::ReadRequest {
        offset,
        block: true,
        journal: journal
            .spec
            .as_ref()
            .map(|spec| spec.name.clone())
            .unwrap_or_default(),
        begin_mod_time: 0,
        do_not_proxy: false,
        end_offset: 0,
        header: None, // TODO(johnny): attach `journal` route.
        metadata_only: false,
    });

    Ok(stream)
}

struct Dekaf {
    client: postgrest::Postgrest,
    cached_collections: BTreeMap<String, ()>,
    streams: BTreeMap<String, (i64, gazette::journal::Docs)>,
}

impl Dekaf {
    async fn collections(&mut self) -> anyhow::Result<&BTreeMap<String, ()>> {
        // TODO(johnny): Refresh policy.
        if !self.cached_collections.is_empty() {
            return Ok(&self.cached_collections);
        }

        #[derive(serde::Deserialize)]
        struct Row {
            catalog_name: String,
        }
        let rows: Vec<Row> = self
            .client
            .from("live_specs_ext")
            .eq("spec_type", "collection")
            .select("catalog_name")
            .execute()
            .await
            .and_then(|r| r.error_for_status())
            .context("listing current catalog specifications")?
            .json()
            .await?;

        self.cached_collections.clear();
        for Row { catalog_name } in rows {
            self.cached_collections.insert(catalog_name, ());
        }

        Ok(&self.cached_collections)
    }

    pub async fn sasl_handshake(
        &mut self,
        req: messages::SaslHandshakeRequest,
    ) -> anyhow::Result<messages::SaslHandshakeResponse> {
        let mut response = messages::SaslHandshakeResponse::default();
        response.mechanisms.push(StrBytes::from_str("PLAIN"));

        if req.mechanism.ne("PLAIN") {
            response.error_code =
                kafka_protocol::error::ResponseError::UnsupportedSaslMechanism.code();
        }
        Ok(response)
    }

    pub async fn sasl_authenticate(
        &mut self,
        req: messages::SaslAuthenticateRequest,
    ) -> anyhow::Result<messages::SaslAuthenticateResponse> {
        let mut it = req.auth_bytes.split(|b| *b == 0).map(std::str::from_utf8);

        let authzid = it.next().context("expected SASL authzid")??;
        let authcid = it.next().context("expected SASL authcid")??;
        let password = it.next().context("expected SASL passwd")??;

        tracing::info!(authzid, authcid, "sasl_authenticate");

        self.client = self
            .client
            .clone()
            .insert_header("Authorization", format!("Bearer {password}"));

        // Ensure we can use credentials to refresh collections from the control plane.
        _ = self.collections().await?;

        let mut response = messages::SaslAuthenticateResponse::default();
        response.session_lifetime_ms = i64::MAX;
        Ok(response)
    }

    pub async fn metadata(
        &mut self,
        mut req: messages::MetadataRequest,
    ) -> anyhow::Result<messages::MetadataResponse> {
        use messages::metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        };

        let collections = self.collections().await?;

        // If requested topics is empty, then fetch all topics.
        if req.topics.is_none() {
            req.topics = Some(
                collections
                    .keys()
                    .map(|k| {
                        MetadataRequestTopic::builder()
                            .name(Some(messages::TopicName(string_bytes(k.clone()))))
                            .build()
                            .unwrap()
                    })
                    .collect(),
            );
        }

        let mut brokers = kafka_protocol::indexmap::IndexMap::new();
        brokers.insert(
            messages::BrokerId(1),
            MetadataResponseBroker::builder()
                .host(string_bytes(ADVERTISE_HOST.into()))
                .port(ADVERTISE_PORT.into())
                .build()
                .unwrap(),
        );

        let mut topics = kafka_protocol::indexmap::IndexMap::new();

        for mut topic in req.topics.take().unwrap() {
            let name = topic.name.take().unwrap_or_default();

            if let Some(()) = collections.get(&*name.0) {
                let partition = MetadataResponsePartition::builder()
                    .partition_index(0)
                    .leader_id(messages::BrokerId(1))
                    .replica_nodes(vec![messages::BrokerId(1)])
                    .isr_nodes(vec![messages::BrokerId(1)])
                    .build()
                    .unwrap();

                topics.insert(
                    name,
                    MetadataResponseTopic::builder()
                        .is_internal(false)
                        .partitions(vec![partition])
                        .build()
                        .unwrap(),
                );
            } else {
                topics.insert(
                    name,
                    MetadataResponseTopic::builder()
                        .error_code(
                            kafka_protocol::error::ResponseError::UnknownTopicOrPartition.code(),
                        )
                        .build()
                        .unwrap(),
                );
            }
        }

        Ok(MetadataResponse::builder()
            .brokers(brokers)
            .cluster_id(Some(string_bytes("estuary-dekaf".into())))
            .controller_id(messages::BrokerId(1))
            .topics(topics)
            .build()
            .unwrap())
    }

    pub async fn find_coordinator(
        &mut self,
        req: messages::FindCoordinatorRequest,
    ) -> anyhow::Result<messages::FindCoordinatorResponse> {
        let coordinators = req
            .coordinator_keys
            .iter()
            .map(|_key| {
                messages::find_coordinator_response::Coordinator::builder()
                    .node_id(messages::BrokerId(1))
                    .host(string_bytes(ADVERTISE_HOST.into()))
                    .port(ADVERTISE_PORT.into())
                    .build()
                    .unwrap()
            })
            .collect();

        Ok(messages::FindCoordinatorResponse::builder()
            .node_id(messages::BrokerId(1))
            .host(string_bytes(ADVERTISE_HOST.into()))
            .port(ADVERTISE_PORT.into())
            .coordinators(coordinators)
            .build()
            .unwrap())
    }

    pub async fn list_offsets(
        &mut self,
        req: messages::ListOffsetsRequest,
    ) -> anyhow::Result<messages::ListOffsetsResponse> {
        use messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
        };

        let topics = req
            .topics
            .into_iter()
            .map(|topic| {
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|req| {
                        ListOffsetsPartitionResponse::builder()
                            .partition_index(req.partition_index)
                            .offset(1234)
                            .timestamp(0)
                            .build()
                            .unwrap()
                    })
                    .collect();

                ListOffsetsTopicResponse::builder()
                    .name(topic.name)
                    .partitions(partitions)
                    .build()
                    .unwrap()
            })
            .collect();

        Ok(messages::ListOffsetsResponse::builder()
            .topics(topics)
            .build()
            .unwrap())
    }

    async fn build_record_batch(
        stream: &mut gazette::journal::Docs,
    ) -> Result<bytes::Bytes, gazette::Error> {
        use kafka_protocol::records::{
            Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        };
        let mut records: Vec<Record> = Vec::new();

        while let Some(doc) = stream.try_next().await? {
            let gazette::journal::Doc::Doc { offset, root } = doc else {
                continue;
            };
            let ser = serde_json::to_string(&doc::SerPolicy::noop().on(root.get())).unwrap();

            records.push(Record {
                transactional: true,
                control: false,
                partition_leader_epoch: 1,
                producer_id: 1234, // TODO(johnny): map from UUID.
                producer_epoch: 1,
                timestamp_type: TimestampType::LogAppend,
                offset,
                sequence: offset as i32, // TODO(johnny): map from UUID Clock.
                timestamp: 1234,         // TODO(johnny): map from UUID Clock.
                key: None,
                value: Some(ser.into()),
                headers: Default::default(),
            });
            if records.len() == 500 {
                break;
            }
        }

        let opts = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };

        let mut b = bytes::BytesMut::new();
        RecordBatchEncoder::encode(&mut b, records.iter(), &opts)
            .expect("record encoding cannot fail");

        tracing::info!(
            first = records[0].offset,
            last = records[records.len() - 1].offset,
            "returning records with offset range"
        );

        Ok(b.freeze())
    }

    pub async fn fetch(
        &mut self,
        req: messages::FetchRequest,
    ) -> anyhow::Result<messages::FetchResponse> {
        use kafka_protocol::records::{Record, RecordBatchEncoder};
        use messages::fetch_request::{FetchPartition, FetchTopic};
        use messages::fetch_response::{FetchableTopicResponse, PartitionData};

        let messages::FetchRequest {
            topics,
            max_bytes,
            max_wait_ms,
            min_bytes,
            session_id,
            ..
        } = req;

        let mut topic_responses = Vec::new();

        for topic in topics {
            let FetchTopic {
                topic,
                mut partitions,
                ..
            } = topic;

            if partitions.len() != 1 {
                anyhow::bail!("expected a single fetched partition");
            }
            let FetchPartition {
                partition,
                fetch_offset,
                partition_max_bytes,
                ..
            } = partitions.pop().unwrap();

            let (cur_offset, stream) = match self.streams.get_mut(&*topic.0) {
                Some(entry) /* if entry.0 == fetch_offset */ => entry,
                _ => {
                    let client = build_journal_client(&self.client, &*topic.0).await?;
                    let stream = start_topic_stream(client, &*topic.0, fetch_offset).await?;

                    self.streams
                        .entry(topic.0.to_string())
                        .or_insert((fetch_offset, stream))
                }
            };

            let data = PartitionData::builder()
                .partition_index(partition)
                .records(Some(Self::build_record_batch(stream).await?))
                .high_watermark(1 << 40) // TODO
                .last_stable_offset(1 << 40) // TODO
                .build()
                .unwrap();

            topic_responses.push(
                FetchableTopicResponse::builder()
                    .topic(topic)
                    .partitions(vec![data])
                    .build()
                    .unwrap(),
            );
        }

        //let topic = req.topics.pop().unwrap();
        //let collection = topic.topic.0.to_string();

        Ok(messages::FetchResponse::builder()
            .session_id(session_id)
            .responses(topic_responses)
            .build()
            .unwrap())
    }

    pub async fn offset_commit(
        &mut self,
        _req: messages::OffsetCommitRequest,
    ) -> anyhow::Result<messages::OffsetCommitResponse> {
        Ok(messages::OffsetCommitResponse::builder().build().unwrap())
    }

    pub async fn api_versions(
        &mut self,
        _req: messages::ApiVersionsRequest,
    ) -> anyhow::Result<messages::ApiVersionsResponse> {
        use kafka_protocol::messages::{api_versions_response::ApiVersion, *};

        fn version<T: kafka_protocol::protocol::Message>() -> ApiVersion {
            let mut v = ApiVersion::default();
            v.max_version = T::VERSIONS.max;
            v.min_version = T::VERSIONS.min;
            v
        }
        let mut res = ApiVersionsResponse::default();

        res.api_keys.insert(
            ApiKey::ApiVersionsKey as i16,
            version::<ApiVersionsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::SaslHandshakeKey as i16,
            version::<SaslHandshakeRequest>(),
        );
        res.api_keys.insert(
            ApiKey::SaslAuthenticateKey as i16,
            version::<SaslAuthenticateRequest>(),
        );
        res.api_keys
            .insert(ApiKey::MetadataKey as i16, version::<MetadataRequest>());
        res.api_keys.insert(
            ApiKey::FindCoordinatorKey as i16,
            version::<FindCoordinatorRequest>(),
        );
        res.api_keys.insert(
            ApiKey::ListOffsetsKey as i16,
            version::<ListOffsetsRequest>(),
        );
        res.api_keys
            .insert(ApiKey::FetchKey as i16, version::<FetchRequest>());
        res.api_keys.insert(
            ApiKey::OffsetCommitKey as i16,
            version::<OffsetCommitRequest>(),
        );

        // UNIMPLEMENTED.
        res.api_keys
            .insert(ApiKey::ProduceKey as i16, version::<ProduceRequest>());
        res.api_keys.insert(
            ApiKey::LeaderAndIsrKey as i16,
            version::<LeaderAndIsrRequest>(),
        );
        res.api_keys.insert(
            ApiKey::StopReplicaKey as i16,
            version::<StopReplicaRequest>(),
        );
        res.api_keys
            .insert(ApiKey::JoinGroupKey as i16, version::<JoinGroupRequest>());
        res.api_keys
            .insert(ApiKey::HeartbeatKey as i16, version::<HeartbeatRequest>());
        res.api_keys
            .insert(ApiKey::ListGroupsKey as i16, version::<ListGroupsRequest>());
        res.api_keys
            .insert(ApiKey::SyncGroupKey as i16, version::<SyncGroupRequest>());
        res.api_keys.insert(
            ApiKey::CreateTopicsKey as i16,
            version::<CreateTopicsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::DeleteGroupsKey as i16,
            version::<DeleteGroupsRequest>(),
        );
        res.api_keys
            .insert(ApiKey::ListGroupsKey as i16, version::<ListGroupsRequest>());
        res.api_keys.insert(
            ApiKey::DeleteTopicsKey as i16,
            version::<DeleteTopicsRequest>(),
        );

        Ok(res)
    }
}

#[tracing::instrument(level = "debug", err(level = "warn"), skip_all)]
async fn dispatch_request_frame(
    dekaf: &mut Dekaf,
    raw_sasl_auth: &mut bool,
    frame: bytes::BytesMut,
    out: &mut bytes::BytesMut,
) -> anyhow::Result<()> {
    use messages::*;

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

    match api_key {
        ApiKey::ApiVersionsKey => {
            // https://github.com/confluentinc/librdkafka/blob/e03d3bb91ed92a38f38d9806b8d8deffe78a1de5/src/rdkafka_request.c#L2823
            let (header, request) = dec_request(version >= 3, frame)?;
            Ok(enc_resp(out, &header, dekaf.api_versions(request).await?))
        }
        ApiKey::SaslHandshakeKey => {
            let (header, request) = dec_request(false, frame)?;
            *raw_sasl_auth = header.request_api_version == 0;
            Ok(enc_resp(out, &header, dekaf.sasl_handshake(request).await?))
        }
        ApiKey::SaslAuthenticateKey if *raw_sasl_auth => {
            *raw_sasl_auth = false;

            let request = messages::SaslAuthenticateRequest::builder()
                .auth_bytes(frame.freeze())
                .build()
                .unwrap();
            let response = dekaf.sasl_authenticate(request).await?;

            out.put_i32(response.auth_bytes.len() as i32);
            out.extend(response.auth_bytes);
            Ok(())
        }
        ApiKey::SaslAuthenticateKey => {
            let (header, request) = dec_request(false, frame)?;
            Ok(enc_resp(
                out,
                &header,
                dekaf.sasl_authenticate(request).await?,
            ))
        }
        ApiKey::MetadataKey => {
            // https://github.com/confluentinc/librdkafka/blob/e03d3bb91ed92a38f38d9806b8d8deffe78a1de5/src/rdkafka_request.c#L2417
            let (header, request) = dec_request(version >= 9, frame)?;
            Ok(enc_resp(out, &header, dekaf.metadata(request).await?))
        }
        ApiKey::FindCoordinatorKey => {
            let (header, request) = dec_request(false, frame)?;
            Ok(enc_resp(
                out,
                &header,
                dekaf.find_coordinator(request).await?,
            ))
        }
        ApiKey::ListOffsetsKey => {
            let (header, request) = dec_request(false, frame)?;
            Ok(enc_resp(out, &header, dekaf.list_offsets(request).await?))
        }

        ApiKey::FetchKey => {
            let (header, request) = dec_request(false, frame)?;
            Ok(enc_resp(out, &header, dekaf.fetch(request).await?))
        }

        ApiKey::OffsetCommitKey => {
            let (header, request) = dec_request(false, frame)?;
            Ok(enc_resp(out, &header, dekaf.offset_commit(request).await?))
        }

        /*
        ApiKey::CreateTopicsKey => Ok(K::CreateTopicsRequest(CreateTopicsRequest::decode(b, v)?)),
        ApiKey::FindCoordinatorKey => Ok(K::FindCoordinatorRequest(
            FindCoordinatorRequest::decode(b, v)?,
        )),
        ApiKey::ListGroupsKey => Ok(K::ListGroupsRequest(ListGroupsRequest::decode(b, v)?)),
        */
        _ => anyhow::bail!("unsupported request type {api_key:?}"),
    }
}

// Easier dispatch to type-specific decoder by using result-type inference.
fn dec_request<T: kafka_protocol::protocol::Decodable + std::fmt::Debug>(
    flexver: bool,
    mut frame: bytes::BytesMut,
) -> anyhow::Result<(messages::RequestHeader, T)> {
    let header = messages::RequestHeader::decode(&mut frame, if flexver { 2 } else { 1 })?;

    let request = T::decode(&mut frame, header.request_api_version).with_context(|| {
        format!(
            "failed to decode {} with header {header:?}",
            std::any::type_name::<T>()
        )
    })?;

    if !frame.is_empty() {
        anyhow::bail!(
            "frame has {} bytes remaining after decoding {}",
            frame.len(),
            std::any::type_name::<T>()
        );
    }
    tracing::debug!(?request, ?header, "decoded request");

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

    // tracing::debug!(?response, "encoded response");
}

#[tracing::instrument(level = "debug", ret, err(level = "warn"), skip(dekaf, socket, _stop), fields(?addr))]
async fn serve_connection(
    mut dekaf: Dekaf,
    mut socket: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    _stop: impl futures::Future<Output = ()>, // TODO(johnny): stop.
) -> anyhow::Result<()> {
    tracing::info!("accepted client connection");

    socket.set_nodelay(true)?;
    let (r, mut w) = socket.split();

    let mut r = tokio_util::codec::FramedRead::new(
        r,
        tokio_util::codec::LengthDelimitedCodec::builder()
            .big_endian()
            .length_field_length(4)
            .max_frame_length(1 << 27) // 128 MiB
            .new_codec(),
    );

    let mut out = bytes::BytesMut::new();
    let mut raw_sasl_auth = false;
    while let Some(frame) = r.try_next().await? {
        () = dispatch_request_frame(&mut dekaf, &mut raw_sasl_auth, frame, &mut out).await?;
        () = w.write_all(&mut out).await?;
        out.clear();
    }
    Ok(())
}

fn string_bytes(s: String) -> StrBytes {
    unsafe { StrBytes::from_utf8_unchecked(bytes::Bytes::from(s)) }
}

pub const PUBLIC_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5cmNubXV6enlyaXlwZGFqd2RrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NDg3NTA1NzksImV4cCI6MTk2NDMyNjU3OX0.y1OyXD3-DYMz10eGxzo1eeamVMMUwIIeOoMryTRAoco";
pub const PUBLIC_ENDPOINT: &str = "https://eyrcnmuzzyriypdajwdk.supabase.co/rest/v1";

pub const ADVERTISE_HOST: &str = "127.0.0.1";
pub const ADVERTISE_PORT: u16 = 9092;
