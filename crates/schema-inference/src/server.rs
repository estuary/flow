use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use crate::inference::infer_shape;
use crate::json_decoder::{JsonCodec, JsonCodecError};
use crate::schema::SchemaBuilder;
use crate::shape;
use bytesize::ByteSize;
use serde_json::json;

use anyhow::Context;
use assemble::journal_selector;
use doc::inference::Shape;
use futures::{Stream, TryStreamExt};
use journal_client::broker::{fragments_response, FragmentsRequest, JournalSpec};
use journal_client::fragments::FragmentIter;
use journal_client::list::list_journals;
use journal_client::read::uncommitted::{
    ExponentialBackoff, JournalRead, ReadStart, ReadUntil, Reader,
};
use journal_client::{connect_journal_client, ConnectError};
use models;

use futures_util::StreamExt;
use schemars::schema::RootSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::watch::Receiver;
use tokio::time::{sleep, Instant};
use tokio_util::codec::FramedRead;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use warp::hyper::StatusCode;
use warp::reply::Response;
use warp::{Filter, Reply};

#[derive(Error, Debug)]
pub enum InferenceError {
    #[error(transparent)]
    ConnectionError(#[from] ConnectError),
    #[error("No documents found, cannot infer shape")]
    NoDocsFound,
    #[error("Missing fragment spec")]
    NoFragmentSpec(),
    #[error("{}", .0.to_string())]
    TonicStatus(#[from] tonic::Status),
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
    #[error(transparent)]
    JournalClientFragmentError(#[from] journal_client::fragments::Error),
    #[error(transparent)]
    JsonCodecError(#[from] JsonCodecError),
}

/// An API error serializable to JSON.
#[derive(Serialize)]
struct ErrorMessage {
    code: u16,
    message: String,
}

impl InferenceError {
    fn into_response(self) -> Response {
        let message = self.to_string();

        let code = match self {
            InferenceError::ConnectionError(err) => match err {
                ConnectError::BadUri(_) => StatusCode::BAD_REQUEST,
                ConnectError::Grpc(_) => StatusCode::INTERNAL_SERVER_ERROR,
                ConnectError::InvalidBearerToken => StatusCode::UNAUTHORIZED,
            },
            InferenceError::NoDocsFound => StatusCode::NOT_FOUND,
            InferenceError::NoFragmentSpec() => StatusCode::INTERNAL_SERVER_ERROR,
            InferenceError::TonicStatus(_) => StatusCode::BAD_REQUEST,
            InferenceError::SerdeError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            InferenceError::JournalClientFragmentError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            InferenceError::JsonCodecError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let json = warp::reply::json(&ErrorMessage {
            code: code.as_u16(),
            message: message.into(),
        });

        warp::reply::with_status(json, code).into_response()
    }
}

/// Used to roll up shapes inferred from collections,
/// as well as metadata about the data that went into them
#[derive(Default, Debug)]
struct ShapeAndMeta {
    shape: Shape,
    /// The number of documents that were read to infer this shape
    docs: u64,
    /// The number of bytes that composed the documents from which this shape was inferred
    bytes: u64,
}

async fn healthz(broker_url: String) -> Response {
    match connect_journal_client(broker_url.clone(), None).await {
        Ok(_) => warp::reply::json(&json!({"status": "OK"})).into_response(),
        Err(err) => InferenceError::from(err).into_response(),
    }
}

#[derive(Serialize, Deserialize)]
pub struct InferenceResponse {
    pub schema: RootSchema,
    pub documents_read: u64,
    pub exceeded_deadline: bool,
}

impl InferenceResponse {
    fn into_response(self) -> Response {
        warp::reply::json(&self).into_response()
    }
}

async fn infer_schema(
    broker_url: String,
    max_inference_duration: std::time::Duration,
    collection_name: String,
) -> Result<InferenceResponse, InferenceError> {
    tracing::debug!("Starting inference");

    let (abort_tx, abort_rx) = tokio::sync::watch::channel::<bool>(false);

    let start_instant = Instant::now();
    let max_duration = max_inference_duration.clone();

    // This seems to want to be set on a block, rather than a single expression :(
    #[allow(unused_must_use)]
    let handle = tokio::spawn(async move {
        sleep(max_duration).await;
        abort_tx.send(true);
    });

    let mut client = connect_journal_client(broker_url.clone(), None)
        .await
        .map_err(InferenceError::from)?;

    tracing::debug!("Got inference client");

    let flow_collection = models::Collection::new(collection_name.clone());
    let selector = journal_selector(&flow_collection, None);
    let journals = list_journals(&mut client, &selector).await?;

    tracing::debug!(num_journals = journals.len(), "Got journals");

    let buffered = futures::stream::iter(journals)
        .map(|j| journal_to_shape(j.clone(), client.clone(), abort_rx.clone()))
        .buffer_unordered(3);

    let root_schema = match reduce_shape_stream(buffered).await? {
        Some(ShapeAndMeta { shape, docs, bytes }) => {
            let root_schema = SchemaBuilder::new(shape).root_schema();
            let end_time = Instant::now().duration_since(start_instant);
            tracing::info!(
                collection=collection_name,
                documents_read=docs,
                bytes_read=display(ByteSize(bytes)),
                fully_read_collection=!*abort_rx.borrow(),
                duration=?end_time,
                "Finished schema inference"
            );
            Ok(InferenceResponse {
                schema: root_schema,
                documents_read: docs,
                exceeded_deadline: *abort_rx.borrow(),
            })
        }
        None => Err(InferenceError::NoDocsFound),
    };

    handle.abort();
    root_schema
}

async fn journal_to_shape(
    journal: JournalSpec,
    client: journal_client::Client,
    abort_signal: Receiver<bool>,
) -> Result<Option<ShapeAndMeta>, InferenceError> {
    let frag_iter = FragmentIter::new(
        client.clone(),
        FragmentsRequest {
            journal: journal.name.clone(),
            ..Default::default()
        },
    );

    let frag_stream = frag_iter
        .into_stream()
        .map_err(InferenceError::from)
        .map_ok(|frag| fragment_to_shape(client.clone(), frag, abort_signal.clone()))
        .try_buffer_unordered(1)
        .into_stream();

    reduce_shape_stream(frag_stream).await
}

fn is_aborted(rx: &mut Receiver<bool>) -> bool {
    *rx.borrow_and_update()
}

/// Read all documents in a particular fragment and return the most-strict [Shape]
/// that matches every document.
///
/// We explicitly omit documents containing
/// `{"_meta": {"ack": true}}`, as those documents are not relevant to user data,
/// and would just confuse users of the schema inference API
#[tracing::instrument(skip_all, fields(journal_name, offset_start, offset_end))]
async fn fragment_to_shape(
    client: journal_client::Client,
    fragment: fragments_response::Fragment,
    mut abort_signal: Receiver<bool>,
) -> Result<Option<ShapeAndMeta>, InferenceError> {
    // Let's bail early if we're already aborted
    if is_aborted(&mut abort_signal) {
        return Ok(None);
    }

    let fragment_spec = fragment.spec.ok_or(InferenceError::NoFragmentSpec())?;

    tracing::Span::current().record("journal_name", &fragment_spec.journal);
    tracing::Span::current().record("fragment_name", &fragment_spec.path_postfix);
    tracing::Span::current().record("offset_start", &fragment_spec.begin);
    tracing::Span::current().record("offset_end", &fragment_spec.end);

    tracing::debug!("Reading fragment");

    let reader = Reader::start_read(
        client.clone(),
        JournalRead::new(fragment_spec.journal)
            .starting_at(ReadStart::Offset(fragment_spec.begin.try_into().unwrap()))
            .read_until(ReadUntil::Offset(fragment_spec.end.try_into().unwrap())),
        ExponentialBackoff::new(3),
    );

    let codec = JsonCodec::new(); // do we want to limit length here? LinesCodec::new_with_max_length(...) does this
    let mut doc_bytes_stream = FramedRead::new(FuturesAsyncReadCompatExt::compat(reader), codec);

    let mut accumulator: Option<Shape> = None;
    let mut docs: u64 = 0;

    let mut duration_total = Duration::default();
    let mut duration_count: u64 = 0;

    loop {
        tokio::select! {
            maybe_doc_body = doc_bytes_stream.next() => {
                match maybe_doc_body {
                    Some(Ok(doc_val)) => {
                        let parsed: Value = doc_val;
                        // There should probably be a higher-level API for this in `journal-client`

                        let start = Instant::now();
                        if parsed.pointer("/_meta/ack").is_none() {
                            let inferred_shape = infer_shape(&parsed);

                            if let Some(accumulated_shape) = accumulator {
                                accumulator = Some(shape::merge(accumulated_shape, inferred_shape))
                            } else {
                                accumulator = Some(inferred_shape)
                            }
                            docs = docs + 1;
                        }
                        duration_total = duration_total + start.elapsed();
                        duration_count += 1;
                    }
                    Some(Err(e)) => return Err(InferenceError::from(e)),
                    None => break
                }
            }
            _ = abort_signal.changed() => {
                if is_aborted(&mut abort_signal) {
                    tracing::debug!("Aborting schma inference early!");
                    break
                }
            }
        };
    }

    let in_ms = duration_total.as_secs() * 1000 + duration_total.subsec_nanos() as u64 / 1_000_000;
    let avg_ms = in_ms as f64 / duration_count as f64;

    let bytes_read = doc_bytes_stream.decoder().bytes_read().try_into().unwrap();
    tracing::debug!(
        bytes_read = display(ByteSize(bytes_read)),
        docs_read = docs,
        avg_document_inference_ms = avg_ms,
        total_document_inference_ms = in_ms,
        "Done reading fragment"
    );

    match accumulator {
        Some(accum) => Ok(Some(ShapeAndMeta {
            shape: accum,
            docs,
            bytes: bytes_read,
        })),
        None => Ok(None),
    }
}

async fn reduce_shape_stream(
    stream: impl Stream<Item = Result<Option<ShapeAndMeta>, InferenceError>>,
) -> Result<Option<ShapeAndMeta>, InferenceError> {
    stream
        .try_fold(None, |mut maybe_accum: Option<ShapeAndMeta>, item| async {
            if let Some(item) = item {
                match maybe_accum {
                    Some(ref mut accumulator) => {
                        accumulator.shape = shape::merge(accumulator.shape.clone(), item.shape);
                        accumulator.docs += item.docs;
                        accumulator.bytes += item.bytes
                    }
                    None => {
                        maybe_accum = Some(ShapeAndMeta {
                            shape: item.shape,
                            docs: item.docs,
                            bytes: item.bytes,
                        })
                    }
                }
            }
            Ok(maybe_accum)
        })
        .await
}

#[derive(Debug, clap::Args)]
pub struct ServeArgs {
    #[clap(long, value_parser, default_value_t = 9090, env)]
    port: u32,
    #[clap(long, value_parser, default_value = "0.0.0.0", env)]
    bind_address: IpAddr,
    /// URL for a Gazette broker that is a member of the cluster
    #[clap(long, value_parser, env)]
    broker_url: String,
    /// Maximum number of seconds to run inference. This exists to preserve system performance and reliability in the face of large collections.
    ///
    /// Example values: 1m, 30s, 300ms
    #[clap(long("timeout"), default_value = "10s", env("TIMEOUT"))]
    inference_deadline: humantime::Duration,
}

#[derive(Serialize, Deserialize)]
struct QueryParams {
    collection_name: String,
}

async fn handle_inference_api(
    broker_url: String,
    max_inference_duration: std::time::Duration,
    collection_name: String,
) -> Response {
    let resp = infer_schema(broker_url, max_inference_duration, collection_name.clone()).await;

    match resp {
        Ok(success) => return success.into_response(),
        Err(err) => {
            tracing::error!(collection_name, "{:?}", err);
            return err.into_response();
        }
    }
}

impl ServeArgs {
    #[tracing::instrument(skip(self))]
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let broker_url = self.broker_url.clone();
        let inference_deadline = self.inference_deadline.clone();
        let inference_endpoint = warp::get()
            .and(warp::path!("infer_schema"))
            .and(warp::query::<QueryParams>())
            .and_then(move |params: QueryParams| {
                let broker_url_cloned = broker_url.clone();
                let inference_deadline_cloned = inference_deadline.clone();
                async move {
                    Ok(handle_inference_api(
                        broker_url_cloned,
                        inference_deadline_cloned.into(),
                        params.collection_name,
                    )
                    .await)
                        as Result<warp::reply::Response, core::convert::Infallible>
                }
            });

        let broker_url = self.broker_url.clone();
        let healthz_endpoint = warp::get().and(warp::path!("healthz")).and_then(move || {
            let broker_url_cloned = broker_url.clone();
            async move {
                Ok(healthz(broker_url_cloned).await)
                    as Result<warp::reply::Response, core::convert::Infallible>
            }
        });

        let addr: SocketAddr = format!("{}:{}", self.bind_address, self.port)
            .parse()
            .context(format!(
                "Failed to parse server listen socket \"{}:{}\"",
                self.bind_address, self.port
            ))?;

        tracing::info!("🚀 Serving schema inference on {}", addr);

        warp::serve(inference_endpoint.or(healthz_endpoint))
            .run(addr)
            .await;
        Ok(())
    }
}
