//! The request module contains helpers for creating requests used for testing drivers.
use crate::DriverClientImpl;
use crate::test_doc::TestDoc;
use doc::Pointer;
use protocol::{
    arena::ArenaExt,
    collection::CollectionExt,
    flow::{CollectionSpec, Slice},
    materialize::{transaction_request, transaction_response, FieldSelection, LoadEof, TransactionRequest, TransactionResponse },
};
use serde_json::Value;
use tonic::{Response, Streaming, Status};
use tuple::{TupleDepth, TuplePack};
use tracing::{debug, trace};

use std::borrow::Borrow;
use std::fmt::Debug;

/// Constructs a `LoadRequest` for the given documents. Uses the key pointers in the provided
/// `collection` spec to extract and pack the key for each document.
pub fn load_request(
    collection: &CollectionSpec,
    docs: &[impl Borrow<Value>],
) -> TransactionRequest {
    let mut arena = Vec::with_capacity(128);
    let key_ptrs = collection
        .key_ptrs
        .iter()
        .map(Pointer::from)
        .collect::<Vec<_>>();

    let packed_keys = docs
        .into_iter()
        .map(|doc| extract_and_pack(&key_ptrs, doc.borrow(), &mut arena))
        .collect::<Vec<_>>();

    TransactionRequest {
        load: Some(transaction_request::LoadRequest { arena, packed_keys }),
        ..Default::default()
    }
}

/// Constructor for the first Start message in a transaction stream.
pub fn transaction_start_req(
    handle: Vec<u8>,
    fields: FieldSelection,
    checkpoint: Vec<u8>,
) -> TransactionRequest {
    TransactionRequest {
        start: Some(transaction_request::Start {
            handle,
            fields: Some(fields),
            flow_checkpoint: checkpoint,
        }),
        ..Default::default()
    }
}

/// Represents the receive side of a bidirectional streaming request. This wraps the task that
/// awaits the tonic Response because that task may not complete until the server actually sends
/// the first message (either a LoadResponse or LoadEOF). The `Streaming` receiver will be dropped
/// if an error is returned from `recv`, and subsequent calls will simply return a generic error
/// message.
#[must_use = "must be used in order to complete transaction"]
#[derive(Debug)]
enum TransactionReceiver {
    Pending(tokio::task::JoinHandle<Result<Response<Streaming<TransactionResponse>>, Status>>),
    Active(tonic::Streaming<TransactionResponse>),
    Error,
}

impl TransactionReceiver {
    async fn recv(&mut self) -> anyhow::Result<Option<TransactionResponse>> {
        let tmp = std::mem::replace(self, Self::Error);
        let mut stream = match tmp {
            Self::Active(s) => s,
            Self::Pending(fut) => {
                debug!("awaiting transaction response headers");
                // first ? is for the tokio join result, next is for the tonic result
                let resp = fut.await??;
                debug!("got transaction response headers");
                resp.into_inner()
            }
            Self::Error => anyhow::bail!("transaction receiver previously returned error"),
        };
        let message = stream.message().await?;
        let _ = std::mem::replace(self, Self::Active(stream));
        Ok(message)
    }
}

/// Represents the client-side receiver of messages during the Loading phase of the transaction.
#[must_use = "must be used in order to complete transaction"]
#[derive(Debug)]
pub struct LoadReceiver(TransactionReceiver);

impl LoadReceiver {
    /// Receives all LoadResponse messages followed by a single LoadEOF. Returns a result of:
    /// 0. The `StoreResponseReceiver`, for the next phase in the transaction lifecycle.
    /// 1. A `Vec<Value>` containing all the parsed documents from all LoadResponses.
    /// 2. The boolean value of the `always_empty_hint` from the `LoadEOF` message.
    /// An error is returned if an unexpected message is received, or if any returned document
    /// cannot be parsed as json.
    pub async fn recv_all(mut self) -> anyhow::Result<(StoreResponseReceiver, Vec<Value>, bool)> {
        let mut docs = Vec::with_capacity(4);

        loop {
            let msg = self.0.recv().await?.ok_or_else(||
                                                         anyhow::anyhow!("expected a LoadResponse message, but got EOF")
                                                         )?;
            if let Some(eof) = msg.load_eof.as_ref() {
                return Ok((StoreResponseReceiver(self.0), docs, eof.always_empty_hint))
            }
            anyhow::ensure!(msg.load_response.is_some() || msg.load_eof.is_some(),
                "expected a LoadResponse or LoadEOF message, got: {:?}", msg);

            let transaction_response::LoadResponse {arena, docs_json} = msg.load_response.unwrap();
            for slice in docs_json {
                let value = serde_json::from_slice(arena.bytes(slice))?;
                docs.push(value);
            }
        }
    }
}

/// Represents the client-side receiver of messages during the Storing phase of the transaction.
#[must_use = "must be used in order to complete transaction"]
#[derive(Debug)]
pub struct StoreResponseReceiver(TransactionReceiver);
impl StoreResponseReceiver {
    /// Receives the final store response and closed the stream.
    pub async fn recv_store_response(mut self) -> anyhow::Result<transaction_response::StoreResponse> {
        let msg = self.0.recv()
            .await?
            .ok_or_else(||
                        anyhow::anyhow!("expected a LoadResponse message, but got EOF")
                       )?;

        anyhow::ensure!(msg.store_response.is_some(), "expected a StoreResponse, got: {:?}", msg);
        Ok(msg.store_response.unwrap())
    }
}

/// Starts a new Transaction bi-directional streaming rpc, and returns a tuple of the sender and
/// receiver.
pub async fn new_transaction(mut client: DriverClientImpl) -> (TransactionSender, LoadReceiver) {
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let rx = tokio_stream::wrappers::ReceiverStream::new(rx);
    debug!("about to start streaming transaction");

    // The call to `client.transaction(rx).await` will not return until the driver has sent the
    // first message, or at least the response headers. The default golang grpc server
    // implementation will not send response headers automatically until the driver sends the first
    // message. This means that we need to move this call into a background task so that we can
    // send messages on the TransactionSender before receiving the first response. Otherwise, this
    // will essentially deadlock, with the client and driver both blocked awaiting receipt of a message.
    let fut = tokio::spawn(async move {
        client.transaction(rx).await
    });
    debug!("started streaming transaction");
    (TransactionSender(tx), LoadReceiver(TransactionReceiver::Pending(fut)))
}

/// Represents the client-side sender of messages during the Init phase of the transaction.
#[must_use = "must be used in order to complete transaction"]
#[derive(Debug)]
pub struct TransactionSender(tokio::sync::mpsc::Sender<TransactionRequest>);
impl TransactionSender {
    // Sends the Start message and transitions from the Init phase to the Loading phase of the
    // transaction.
    pub async fn send_start(
        self,
        handle: Vec<u8>,
        fields: FieldSelection,
        checkpoint: Vec<u8>,
    ) -> anyhow::Result<LoadSender> {
        debug!("about to send transaction start message");
        let req = transaction_start_req(handle, fields, checkpoint);
        self.0.send(req).await?;
        debug!("sent transaction start request");
        Ok(LoadSender(self))
    }
}

/// Represents the client-side sender of messages during the Loading phase of the transaction.
#[must_use = "must be used in order to complete transaction"]
#[derive(Debug)]
pub struct LoadSender(TransactionSender);
impl LoadSender {
    /// Sends a LoadRequest message to the driver.
    pub async fn send_load(
        &mut self,
        collection: &CollectionSpec,
        docs: &[impl Borrow<Value>],
    ) -> anyhow::Result<()> {
        let req = load_request(collection, docs);
        self.0.0.send(req).await?;
        Ok(())
    }

    pub async fn finish_loads(self) -> anyhow::Result<StoreSender> {
        let req = TransactionRequest {
            load_eof: Some(LoadEof::default()),
            ..Default::default()
        };
        self.0.0.send(req).await?;
        Ok(StoreSender(self.0))
    }
}

/// Represents the client-side sender of messages during the Storing phase of the transaction.
#[must_use = "must be used in order to complete transaction"]
#[derive(Debug)]
pub struct StoreSender(TransactionSender);
impl StoreSender {
    /// Sends the given StoreRequest
    pub async fn send_store_req(
        &mut self,
        req: transaction_request::StoreRequest,
    ) -> anyhow::Result<()> {
        self.0.0
            .send(TransactionRequest {
                store: Some(req),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Sends a StoreRequest that includes the given TestDocs. The keys and values will be
    /// extracted by the given `FieldSelectionPointers`.
    pub async fn send_store(&mut self, fields: &FieldSelectionPointers, documents: &[TestDoc]) -> anyhow::Result<()> {
        let req = new_store_req(fields, documents);
        self.send_store_req(req).await
    }

    /// Drops the StoreSender and the underlying stream, closing the sender.
    pub fn finish(self) {
        // no-op. "finish" just reads nicer than a direct call to std::mem::drop
    }
}

/// Constructs a StoreRequest from the given TestDocs, using the provided `FieldSelectionPointers`
/// to extract the keys and values.
pub fn new_store_req(fields: &FieldSelectionPointers, documents: &[TestDoc]) -> transaction_request::StoreRequest {
    let mut packed_keys = Vec::with_capacity(documents.len());
    let mut packed_values = Vec::with_capacity(documents.len());
    let mut docs_json = Vec::with_capacity(documents.len());
    let mut exists = Vec::with_capacity(documents.len());
    let mut arena = Vec::with_capacity(512);

    for doc in documents {
        packed_keys.push(fields.pack_keys(&doc.json, &mut arena));
        packed_values.push(fields.pack_values(&doc.json, &mut arena));
        let mut writer = arena.writer();
        serde_json::to_writer(&mut writer, &doc.json).expect("failed to serialize document json");
        let slice = writer.finish();
        trace!("Store req document: {}", String::from_utf8_lossy(arena.bytes(slice)));
        docs_json.push(slice);
        exists.push(doc.exists);
    }
    transaction_request::StoreRequest {
        arena, packed_keys, packed_values, docs_json, exists,
    }
}

/// The set of parsed Pointers corresponding to a particular `FieldSelection`.
#[derive(Debug)]
pub struct FieldSelectionPointers {
    keys: Vec<Pointer>,
    values: Vec<Pointer>,
}

impl FieldSelectionPointers {
    /// Returns a new FieldSelectionPointers from the given FieldSelection and CollectionSpec.
    /// Returns an error if any selected fields do not have a corresponding projection.
    pub fn new(
        fields: impl Borrow<FieldSelection>,
        collection: &CollectionSpec,
    ) -> Result<FieldSelectionPointers, anyhow::Error> {
        let fields = fields.borrow();
        let mut keys = Vec::with_capacity(fields.keys.len());
        let mut values = Vec::with_capacity(fields.values.len());

        let get_projection = |f: &str| {
            collection
                .get_projection(f)
                .ok_or_else(|| anyhow::anyhow!("no such projection: '{}'", f))
        };

        for field in fields.keys.iter() {
            let ptr = &get_projection(field)?.ptr;
            keys.push(Pointer::from(ptr));
        }
        for field in fields.values.iter() {
            let ptr = &get_projection(field)?.ptr;
            values.push(Pointer::from(ptr));
        }
        Ok(FieldSelectionPointers { keys, values })
    }

    /// Extracts and packs the key from the given document, returning it as an owned Vec.
    pub fn get_packed_key(&self, json: &Value) -> Vec<u8> {
        let mut key = Vec::new();
        self.pack_keys(json, &mut key);
        key
    }

    /// Extracts and packs the key into the given arena, returning the Slice for the
    /// key within the arena.
    pub fn pack_keys(&self, json: &Value, arena: &mut Vec<u8>) -> Slice {
        extract_and_pack(self.keys.as_slice(), json, arena)
    }

    /// Extracts and packs the values into the given arena, returning the Slice for the
    /// values within the arena.
    pub fn pack_values(&self, json: &Value, arena: &mut Vec<u8>) -> Slice {
        extract_and_pack(self.values.as_slice(), json, arena)
    }
}

fn extract_and_pack(ptrs: &[Pointer], json: &Value, arena: &mut Vec<u8>) -> Slice {
    let json = json.borrow();
    let mut w = arena.writer();
    for ptr in ptrs {
        let value = ptr.query(json).unwrap_or(&Value::Null);
        // TODO: I'm not sure I understand why the depth is 1 here and not 0.
        value
            .pack(&mut w, TupleDepth::new().increment())
            .expect("write to vec cannot fail");
    }
    w.finish()
}
