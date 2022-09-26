use crate::inference::infer_shape;
use crate::schema::SchemaBuilder;
use crate::shape;

use assemble::journal_selector;
use doc::inference::Shape;
use journal_client::broker::JournalSpec;
use journal_client::list::list_journals;
use journal_client::read::uncommitted::{JournalRead, NoRetry, Reader};
use journal_client::{connect_journal_client, ConnectError};
use models;
use schema_inference::inference_service_server::{InferenceService, InferenceServiceServer};
use schema_inference::{InferenceRequest, InferenceResponse};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{to_value, Value};
use std::collections::BTreeMap as Map;
use tokio_util::codec::{FramedRead, LinesCodec};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tonic::{transport::Server, Request, Response, Status};

pub mod schema_inference {
    tonic::include_proto!("schema_inference"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct InferenceServiceImpl {
    pub broker_url: String,
}

#[tonic::async_trait]
impl InferenceService for InferenceServiceImpl {
    // #[tracing::instrument(skip_all, fields(collection=?request.get_ref().flow_collection))]
    async fn infer_schema(
        &self,
        request: Request<InferenceRequest>,
    ) -> Result<Response<InferenceResponse>, Status> {
        tracing::debug!("Starting inference");

        let InferenceRequest {
            flow_collection: collection_name,
        } = request.get_ref();

        let Authentication { token } = request
            .extensions()
            .get::<Authentication>()
            .ok_or(Status::unauthenticated("No token provided"))?;

        let mut client = connect_journal_client(self.broker_url.clone(), Some(token.clone()))
            .await
            .map_err(|e| match e {
                ConnectError::BadUri(_) => Status::invalid_argument(e.to_string()),
                ConnectError::Grpc(_) => Status::internal(e.to_string()), // Should this be `unavailable`?
                ConnectError::InvalidBearerToken => Status::unauthenticated(e.to_string()),
            })?;

        tracing::debug!("Got inference client");

        let flow_collection = models::Collection::new(collection_name);
        let selector = journal_selector(&flow_collection, None);
        let journals = list_journals(&mut client, &selector).await?;

        tracing::debug!("Got {} journals", journals.len());

        // This lets us control parallelism on the journal axis.
        // As it stands, we frequently only have one journal so it doesn't really matter,
        // but it's a good proof-of-concept for splitting up `journal_to_shape` by fragments,
        // where we'd want to do the same thing for much higher parallelism impact
        let buffered = futures::stream::iter(journals)
            .map(|j| journal_to_shape(j.clone(), client.clone()))
            .buffer_unordered(3);

        let shapes = buffered.collect::<Vec<_>>().await;

        let mut accumulator: Option<(Shape, u64)> = None;

        for shape in shapes {
            match shape {
                Ok(Some((inferred_shape, docs_read))) => {
                    if let Some((accumulated_shape, docs_count)) = accumulator {
                        accumulator = Some((
                            shape::merge(accumulated_shape, inferred_shape),
                            docs_count + docs_read,
                        ))
                    } else {
                        accumulator = Some((inferred_shape, docs_read))
                    }
                }
                Ok(None) => {}
                Err(e) => return Err(e),
            }
        }

        match accumulator {
            Some((shape, docs_count)) => {
                let root_schema = SchemaBuilder::new(shape).root_schema();
                let root = serde_json::ser::to_string(&root_schema)
                    .map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(InferenceResponse {
                    body: Some(schema_inference::inference_response::Body::Schema(
                        schema_inference::InferredSchema {
                            schema_json: root,
                            documents_read: docs_count,
                        },
                    )),
                }))
            }
            None => Err(Status::not_found("No documents found, cannot infer shape")),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct JournalMeta {
    uuid: String,
    #[serde(skip_serializing)]
    ack: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize)]
struct JournalMessage {
    #[serde(rename = "_meta")]
    meta: JournalMeta,
    #[serde(flatten)]
    body: Map<String, Value>,
}

async fn journal_to_shape(
    journal: JournalSpec,
    client: journal_client::Client,
) -> Result<Option<(Shape, u64)>, Status> {
    // TODO: Split this request up into parallelizable chunks.
    // The reason we can't just split in to N even-sized chunks is that
    // we could very easily split in the middle of a document.
    // One option is to enumerate fragments in order to determine reasonable
    // split points, and then use the Reader to read to those in parallel.

    let reader = Reader::start_read(
        client.clone(),
        JournalRead::new(journal.name.clone()),
        NoRetry,
    );

    let codec = LinesCodec::new(); // do we want to limit length here? LinesCodec::new_with_max_length(...) does this
    let mut doc_bytes_stream = FramedRead::new(FuturesAsyncReadCompatExt::compat(reader), codec);

    let mut accumulator: Option<Shape> = None;
    let mut docs: u64 = 0;

    while let Some(maybe_doc_body) = doc_bytes_stream.next().await {
        match maybe_doc_body {
            Ok(doc_str) => {
                let parsed: JournalMessage = serde_json::de::from_str(doc_str.as_str())
                    .map_err(|e| Status::aborted(e.to_string()))?;
                // There should probably be a higher-level API for this in `journal-client`

                if parsed.meta.ack.is_none() {
                    let re_serialized = to_value(parsed).unwrap();
                    let inferred_shape = infer_shape(&re_serialized);

                    if let Some(accumulated_shape) = accumulator {
                        accumulator = Some(shape::merge(accumulated_shape, inferred_shape))
                    } else {
                        accumulator = Some(inferred_shape)
                    }
                    docs = docs + 1;
                }
            }
            Err(e) => return Err(Status::aborted(e.to_string())),
        }
    }

    match accumulator {
        Some(accum) => Ok(Some((accum, docs))),
        None => Ok(None),
    }
}

#[derive(Debug, clap::Args)]
pub struct ServeArgs {
    #[clap(long, value_parser, default_value_t = 50051)]
    port: u16,
    #[clap(long, value_parser, default_value = "[::1]")]
    hostname: String,
    /// URL for a Gazette broker that is a member of the cluster
    #[clap(long, value_parser)]
    broker_url: String,
}

impl ServeArgs {
    #[tracing::instrument(skip(self))]
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let svc = InferenceServiceImpl {
            broker_url: self.broker_url.clone(),
        };
        let addr = format!("{}:{}", self.hostname, self.port).parse()?;

        tracing::info!("🚀 Serving gRPC on {}", addr);

        Server::builder()
            .add_service(InferenceServiceServer::with_interceptor(svc, require_auth))
            .serve(addr)
            .await?;

        Ok(())
    }
}

struct Authentication {
    token: String,
}

fn require_auth(mut req: Request<()>) -> Result<Request<()>, Status> {
    match req.metadata().clone().get("authorization") {
        Some(t) /* TODO: validate JWT token */ => {
            req.extensions_mut().insert(Authentication {
                token: t
                    .to_str()
                    .map_err(|e|Status::unauthenticated(format!("Token invalid: {}",e)))?
                    .to_string()
            });

            Ok(req)
        },
        _ => Err(Status::unauthenticated("No valid auth token")),
    }
}
