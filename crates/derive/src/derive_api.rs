use super::combiner::Combiner;
use super::context::Context;
use super::lambda::{self, Lambda};
use super::nodejs::NodeRuntime;
use super::pipeline::PendingPipeline;
use super::registers::{self, Registers};
use crate::setup_env_tracing;

use bytes::BufMut;
use doc::{self, reduce, Pointer};
use futures::channel::mpsc;
use futures::sink::SinkExt;
use futures::stream::{StreamExt, TryStreamExt};
use prost::Message;
use protocol::{cgo, flow, flow::derive_api, message_flags};
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::Instrument;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Lambda invocation error: {0}")]
    Lambda(#[from] lambda::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Schema index: {0}")]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error("channel send error: {0:?}")]
    SendError(#[from] mpsc::SendError),
    #[error("register error: {0}")]
    RegisterErr(#[from] registers::Error),
    #[error("Unknown transform ID: {0}")]
    UnknownTransformID(i32),
    #[error("register reduction error: {0}")]
    RegisterReduction(#[source] reduce::Error),
    #[error("derived document reduction error: {0}")]
    DerivedReduction(#[source] reduce::Error),
    #[error("Protobuf decoding error")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error("source document validation error: {}", serde_json::to_string_pretty(.0).unwrap())]
    SourceValidation(doc::FailedValidation),
    #[error("parsing URL: {0:?}")]
    Url(#[from] url::ParseError),
    #[error(transparent)]
    CatalogError(#[from] catalog::Error),
    #[error(transparent)]
    NodeError(#[from] super::nodejs::Error),
    #[error(transparent)]
    RocksError(#[from] rocksdb::Error),
    #[error("lambda returned fewer rows than expected")]
    TooFewRows,
    #[error("lambda returned more rows than expected")]
    TooManyRows,
    #[error("protocol error (invalid state or invocation)")]
    InvalidState,
}

/// API provides a derivation capability as a cgo::Service.
pub struct API {
    pimpl: Option<APIInner<'static>>,
}

impl cgo::Service for API {
    type Error = Error;

    fn create() -> Self {
        setup_env_tracing();
        Self { pimpl: None }
    }

    fn invoke(
        &mut self,
        code: u32,
        data: &[u8],
        arena: &mut Vec<u8>,
        out: &mut Vec<cgo::Out>,
    ) -> Result<(), Self::Error> {
        match (code, &mut self.pimpl) {
            // Initialize service.
            (0, None) => {
                let cfg = derive_api::Config::decode(data)?;

                // Re-hydrate a &rocksdb::Env from a provided memory address.
                let env_ptr = cfg.rocksdb_env_memptr as usize;
                let env: &rocksdb::Env = unsafe { std::mem::transmute(&env_ptr) };

                self.pimpl = Some(APIInner::from_database(
                    env,
                    &cfg.catalog_path,
                    &cfg.local_dir,
                    &cfg.derivation,
                )?);
            }
            // Restore checkpoint.
            (1, Some(pimpl)) => {
                cgo::send_message(0, &pimpl.restore_checkpoint()?, arena, out);
            }
            // Begin transaction.
            (2, Some(pimpl)) => {
                pimpl.begin_transaction()?;
            }
            // Next document header.
            (3, Some(pimpl)) => {
                pimpl.doc_header(derive_api::DocHeader::decode(data)?)?;
            }
            // Next document body.
            (4, Some(pimpl)) => {
                futures::executor::block_on(pimpl.doc_body(data))?;
            }
            // Flush transaction.
            (5, Some(pimpl)) => {
                let req = derive_api::Flush::decode(data)?;
                futures::executor::block_on(pimpl.flush_transaction(req, arena, out))?;
            }
            // Prepare transaction for commit.
            (6, Some(pimpl)) => {
                let req = derive_api::Prepare::decode(data)?;
                pimpl.prepare_commit(req)?;
            }
            // Clear registers (test support only).
            (7, Some(pimpl)) => {
                pimpl.clear_registers()?;
            }
            _ => return Err(Error::InvalidState),
        }
        Ok(())
    }
}

struct APIInner<'e> {
    runtime: tokio::runtime::Runtime,
    ctx: Arc<Context>,
    node: Option<NodeRuntime>,
    txn_id: usize,
    state: State,

    // This instance cannot outlive the RocksDB environment it uses.
    _env: PhantomData<&'e rocksdb::Env>,
}

enum State {
    Invalid,
    RestoreCheckpoint(Registers),
    BeginTxn(Registers),
    DocHeader(Txn),
    DocBody(derive_api::DocHeader, Txn),
    Prepare(Registers),
}

struct Txn {
    validator: doc::Validator<'static, doc::FullContext>, // TODO(johnny): Remove.
    next: Block,
    tx_blocks: mpsc::Sender<Block>,
    fut: tokio::task::JoinHandle<Result<(Combiner, Registers), Error>>,
}

impl<'e> APIInner<'e> {
    fn build_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("derive-service-worker")
            .enable_all()
            .build()
            .unwrap()
    }

    fn from_database(
        env: &'e rocksdb::Env,
        catalog_path: &str,
        local_dir: &str,
        derivation: &str,
    ) -> Result<APIInner<'e>, Error> {
        // Open catalog DB & build schema index.
        let db = catalog::open(catalog_path)?;
        let schema_index = super::build_schema_index(&db)?;

        // Start NodeJS transform worker.
        let node = super::nodejs::NodeRuntime::start(&db, &local_dir)?;

        // Build derivation context.
        let ctx =
            super::context::Context::build_from_catalog(&db, &derivation, schema_index, &node)?;

        let api = Self::from_parts(
            Self::build_runtime(),
            schema_index,
            ctx,
            env,
            local_dir,
            Some(node),
        )?;

        // Invoke any user-provide runtime bootstraps.
        api.runtime.block_on(
            api.node
                .as_ref()
                .unwrap()
                .invoke_bootstrap(api.ctx.derivation_id),
        )?;

        Ok(api)
    }

    fn from_parts<P: AsRef<std::path::Path>>(
        runtime: tokio::runtime::Runtime,
        schema_index: &'static doc::SchemaIndex,
        ctx: Context,
        env: &'e rocksdb::Env,
        local_dir: P,
        node: Option<NodeRuntime>,
    ) -> Result<APIInner<'e>, Error> {
        let ctx = Arc::new(ctx);

        let mut rocks_opts = rocksdb::Options::default();
        rocks_opts.create_if_missing(true);
        rocks_opts.create_missing_column_families(true);
        rocks_opts.set_env(&env);

        let rocks = rocksdb::DB::open_cf(
            &rocks_opts,
            local_dir,
            [
                rocksdb::DEFAULT_COLUMN_FAMILY_NAME,
                super::registers::REGISTERS_CF,
            ]
            .iter(),
        )?;

        let registers = super::registers::Registers::new(
            rocks,
            schema_index,
            &ctx.register_schema,
            ctx.register_initial.clone(),
        );

        let state = State::RestoreCheckpoint(registers);

        Ok(APIInner {
            runtime,
            ctx,
            node,
            txn_id: 0,
            state,
            _env: PhantomData,
        })
    }

    fn restore_checkpoint(&mut self) -> Result<protocol::consumer::Checkpoint, Error> {
        let state = std::mem::replace(&mut self.state, State::Invalid);

        if let State::RestoreCheckpoint(registers) = state {
            let cp = registers.last_checkpoint()?;
            self.state = State::BeginTxn(registers);
            Ok(cp)
        } else {
            Err(Error::InvalidState)
        }
    }

    fn clear_registers(&mut self) -> Result<(), Error> {
        let state = std::mem::replace(&mut self.state, State::Invalid);

        if let State::BeginTxn(mut registers) = state {
            registers.clear()?;
            self.state = State::BeginTxn(registers);
            Ok(())
        } else {
            Err(Error::InvalidState)
        }
    }

    fn begin_transaction(&mut self) -> Result<(), Error> {
        let state = std::mem::replace(&mut self.state, State::Invalid);

        if let State::BeginTxn(registers) = state {
            self.txn_id += 1;
            let span = tracing::info_span!("txn", id = self.txn_id);

            let (tx_blocks, rx_blocks) = mpsc::channel(0);

            let fut = process_blocks(self.ctx.clone(), registers, rx_blocks).instrument(span);
            let fut = self.runtime.spawn(fut);

            self.state = State::DocHeader(Txn {
                validator: doc::Validator::new(self.ctx.schema_index),
                next: Block::new(self.ctx.clone()),
                tx_blocks,
                fut,
            });
            Ok(())
        } else {
            Err(Error::InvalidState)
        }
    }

    fn doc_header(&mut self, hdr: derive_api::DocHeader) -> Result<(), Error> {
        let state = std::mem::replace(&mut self.state, State::Invalid);

        if let State::DocHeader(txn) = state {
            self.state = State::DocBody(hdr, txn);
            Ok(())
        } else {
            Err(Error::InvalidState)
        }
    }

    async fn doc_body(&mut self, body: &[u8]) -> Result<(), Error> {
        let state = std::mem::replace(&mut self.state, State::Invalid);

        if let State::DocBody(
            derive_api::DocHeader {
                transform_id,
                uuid,
                packed_key,
            },
            Txn {
                mut validator,
                mut tx_blocks,
                mut next,
                fut,
            },
        ) = state
        {
            let uuid = uuid.unwrap_or_default();
            let flags = uuid.producer_and_flags & message_flags::MASK;

            if flags != message_flags::ACK_TXN {
                next.add_document(&mut validator, transform_id, uuid, packed_key, body)?;
            }

            // Measure Block size is the sum of each transform buffer.
            let size = next.buffers.iter().map(|b| b.len()).sum::<usize>();

            let next = if size >= BLOCK_SIZE_CUTOFF {
                // If we're over the cut-off, we must block to dispatch this Block.
                tx_blocks
                    .send(next)
                    .await
                    .expect("cannot fail to send block");
                Block::new(self.ctx.clone())
            } else if flags != message_flags::CONTINUE_TXN && size > 0 {
                // If the block is non-empty and this document *isn't* a
                // continuation of an append transaction (e.x., where we can
                // expect a future ACK_TXN to be forthcoming), then attempt
                // to send.
                match tx_blocks.try_send(next) {
                    Ok(()) => Block::new(self.ctx.clone()),
                    Err(err) => err.into_inner(),
                }
            } else {
                next
            };

            self.state = State::DocHeader(Txn {
                validator,
                tx_blocks,
                next,
                fut,
            });
            Ok(())
        } else {
            Err(Error::InvalidState)
        }
    }

    async fn flush_transaction(
        &mut self,
        req: derive_api::Flush,
        arena: &mut Vec<u8>,
        out: &mut Vec<cgo::Out>,
    ) -> Result<(), Error> {
        let state = std::mem::replace(&mut self.state, State::Invalid);

        if let State::DocHeader(Txn {
            next,
            mut tx_blocks,
            fut,
            ..
        }) = state
        {
            // Dispatch a final, non-empty Block.
            if !next.tf_inds.is_empty() {
                tx_blocks.send(next).await?;
            }
            tx_blocks.close_channel();

            let (combiner, registers) = fut.await.expect("must not have a JoinError")?;

            super::combine_api::drain_combiner(
                combiner,
                &req.uuid_placeholder_ptr,
                &req.field_ptrs.iter().map(Pointer::from).collect::<Vec<_>>(),
                arena,
                out,
            );
            self.state = State::Prepare(registers);

            Ok(())
        } else {
            Err(Error::InvalidState)
        }
    }

    fn prepare_commit(&mut self, req: derive_api::Prepare) -> Result<(), Error> {
        let state = std::mem::replace(&mut self.state, State::Invalid);

        if let State::Prepare(mut registers) = state {
            registers.prepare(req.checkpoint.expect("checkpoint cannot be None"))?;
            self.state = State::BeginTxn(registers);
            Ok(())
        } else {
            Err(Error::InvalidState)
        }
    }
}

// BLOCK_SIZE_CUTOFF is the threshold at which we'll stop adding documents
// to a current block and cut over to a new one.
const BLOCK_SIZE_CUTOFF: usize = 8 * (1 << 16) / 10;
// BLOCK_PARALLELISM is the number of blocks a derivation may process concurrently.
const BLOCK_PARALLELISM: usize = 5;

struct Block {
    ctx: Arc<Context>,
    buffers: Vec<bytes::BytesMut>,
    tf_inds: Vec<u8>,
    keys: Vec<Vec<u8>>,
    uuids: Vec<flow::UuidParts>,
}

impl Block {
    fn new(ctx: Arc<Context>) -> Block {
        let buffers = vec![bytes::BytesMut::new(); ctx.transforms.len()];

        Block {
            ctx,
            buffers,
            tf_inds: Vec::new(),
            keys: Vec::new(),
            uuids: Vec::new(),
        }
    }

    fn add_document<C: json::validator::Context>(
        &mut self,
        val: &mut doc::Validator<C>,
        tf_id: i32,
        uuid: flow::UuidParts,
        packed_key: Vec<u8>,
        data: &[u8],
    ) -> Result<(), Error> {
        let (tf_ind, tf) = self
            .ctx
            .transforms
            .iter()
            .enumerate()
            .find(|(_, t)| t.transform_id == tf_id)
            .ok_or(Error::UnknownTransformID(tf_id))?;

        // Validate.
        // TODO(johnny): Move source schema validation to read-time, avoiding this parsing.
        doc::validate(val, &tf.source_schema, &serde_json::from_slice(data)?)
            .map_err(Error::SourceValidation)?;

        // Accumulate source document into the transform's column buffer.
        let buf = &mut self.buffers[tf_ind];
        if !buf.is_empty() {
            buf.put_u8(b',');
        }
        buf.extend_from_slice(data);

        self.tf_inds.push(tf_ind as u8);
        self.uuids.push(uuid);
        self.keys.push(packed_key);

        Ok(())
    }
}

// Process a continuation block of the derivation's source documents.
#[tracing::instrument(level = "debug", name = "block", err, skip(block, registers, combiner))]
async fn process_block(
    id: usize,
    mut block: Block,
    registers: PendingPipeline<Registers>,
    combiner: PendingPipeline<Combiner>,
) -> Result<(), Error> {
    tracing::debug!(docs = block.tf_inds.len());
    tracing::trace!(keys = ?block.keys.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>());

    // Split off buffers of source document columns, one for each transform.
    // Columns hold comma-separated JSON documents, e.x. `{"doc":1},{"doc":2},...`
    let source_columns = block
        .buffers
        .iter_mut()
        .map(|buffer| buffer.split().freeze())
        .collect::<Vec<_>>();

    let b_open_open = bytes::Bytes::from("[[");
    let b_close_close = bytes::Bytes::from("]]");
    let b_close_comma_open = bytes::Bytes::from("],[");

    // Start invocations of update transforms, then gather deltas from all invocations.
    let mut tf_register_deltas = block
        .ctx
        .transforms
        .iter()
        .zip(source_columns.iter().cloned())
        .map(|(tf, source_column)| {
            let span = tracing::debug_span!("update", tf_id = tf.transform_id);
            tracing::trace!(parent: &span, sources = %String::from_utf8_lossy(&source_column));

            let body = if source_column.is_empty() {
                None
            } else {
                // Stitch "[[" + sources + "]]".
                let body: Vec<Result<_, std::convert::Infallible>> = vec![
                    Ok(b_open_open.clone()),
                    Ok(source_column),
                    Ok(b_close_close.clone()),
                ];
                Some(hyper::Body::wrap_stream(futures::stream::iter(
                    body.into_iter(),
                )))
            };
            tf.update.invoke(body).instrument(span)
        })
        .collect::<futures::stream::FuturesOrdered<_>>()
        .try_collect::<Vec<_>>()
        .await?;

    // Now that we have deltas in-hand, receive |registers| from the
    // processing task ordered ahead of us.
    let mut registers = registers.await;

    // Load all registers in |keys|, so that we may read them below.
    registers.as_mut().load(block.keys.iter())?;
    tracing::trace!(registers = ?registers.as_ref(), "loaded registers");

    // Process documents in sequence, reducing the register updates of each
    // and accumulating register column buffers for future publish invocations.
    for (tf_ind, key) in block.tf_inds.iter().zip(block.keys.iter()) {
        let tf = &block.ctx.transforms[*tf_ind as usize];
        let buf = &mut block.buffers[*tf_ind as usize];

        // If this transform has a update lambda, expect that we received zero or more
        // register deltas for this source document. Otherwise behave as if empty.
        let deltas = if !matches!(tf.update, Lambda::Noop) {
            tf_register_deltas[*tf_ind as usize]
                .next()
                .ok_or(Error::TooFewRows)??
        } else {
            Vec::new()
        };

        // If this transform will invoke a publish lambda, add its "before"
        // register to the invocation body.
        if !matches!(tf.publish, Lambda::Noop) {
            if !buf.is_empty() {
                buf.put_u8(b','); // Continue column.
            }
            buf.put_u8(b'['); // Start a new register row.
            serde_json::to_writer(buf.writer(), registers.as_ref().read(key)).unwrap();
        }

        // If we have deltas to apply, reduce them and assemble into
        // a future publish invocation body.
        if !deltas.is_empty() {
            registers
                .as_mut()
                .reduce(key, deltas.into_iter())
                .map_err(Error::RegisterReduction)?;

            // Write "after" register, completing row.
            if !matches!(tf.publish, Lambda::Noop) {
                buf.put_u8(b','); // Continue register row.
                serde_json::to_writer(buf.writer(), registers.as_ref().read(key)).unwrap();
                buf.put_u8(b']'); // Complete row.
            }
        } else if !matches!(tf.publish, Lambda::Noop) {
            // Complete row without an "after" register (there was no update).
            buf.put_u8(b']');
        }
    }
    tracing::trace!(registers = ?registers.as_ref(), "reduced registers");

    // Release |registers| to the processing task ordered behind us.
    std::mem::drop(registers);

    // Verify that we precisely consumed expected outputs from each lambda.
    for mut it in tf_register_deltas {
        if let Some(_) = it.next() {
            return Err(Error::TooManyRows);
        }
    }

    // Split off buffers of register columns, one for each transform.
    // Columns hold comma-separated rows of JSON documents,
    // e.x. `[{"before":1},{"after":2}],[...]`
    let register_columns = block
        .buffers
        .iter_mut()
        .map(|buffer| buffer.split().freeze())
        .collect::<Vec<_>>();

    // Start invocations of publish transforms, then gather derivations.
    let mut tf_derived_docs = block
        .ctx
        .transforms
        .iter()
        .zip(source_columns.iter().cloned())
        .zip(register_columns.into_iter())
        .map(|((tf, source_column), register_column)| {
            let span = tracing::debug_span!("publish", tf_id = tf.transform_id);
            tracing::trace!(
                parent: &span,
                sources = %String::from_utf8_lossy(&source_column),
                registers = %String::from_utf8_lossy(&register_column),
            );

            let body = if register_column.is_empty() {
                None
            } else {
                // Stitch "[[" + source_column + "],[" + register_column + "]]".
                let body: Vec<Result<_, std::convert::Infallible>> = vec![
                    Ok(b_open_open.clone()),
                    Ok(source_column),
                    Ok(b_close_comma_open.clone()),
                    Ok(register_column),
                    Ok(b_close_close.clone()),
                ];
                Some(hyper::Body::wrap_stream(futures::stream::iter(
                    body.into_iter(),
                )))
            };
            tf.publish.invoke(body).instrument(span)
        })
        .collect::<futures::stream::FuturesOrdered<_>>()
        .try_collect::<Vec<_>>()
        .await?;

    // As with register deltas, now that we have derived documents in-hand,
    // receive |combiner| from the task ordered ahead of us.
    let mut combiner = combiner.await;

    // Process documents in sequence, combining the derived outputs of each.
    for tf_ind in block.tf_inds {
        let tf = &block.ctx.transforms[tf_ind as usize];

        // If this transform has a publish lambda, expect that we received zero or more
        // derived documents for this source document. Otherwise behave as if empty.
        let derived_docs = if !matches!(tf.publish, Lambda::Noop) {
            tf_derived_docs[tf_ind as usize]
                .next()
                .ok_or(Error::TooFewRows)??
        } else {
            Vec::new()
        };

        for doc in derived_docs {
            combiner
                .as_mut()
                .combine(doc, false)
                .map_err(Error::DerivedReduction)?;
        }
    }
    tracing::trace!(combiner = ?combiner.as_ref(), "reduced documents");

    // Release |combiner| to the processing task ordered behind us.
    std::mem::drop(combiner);

    // Verify that we precisely consumed expected outputs from each lambda.
    for mut it in tf_derived_docs {
        if let Some(_) = it.next() {
            return Err(Error::TooManyRows);
        }
    }

    Ok(())
}

async fn process_blocks(
    ctx: Arc<Context>,
    registers: Registers,
    rx_block: mpsc::Receiver<Block>,
) -> Result<(Combiner, Registers), Error> {
    // We'll read zero or more Blocks, followed by a channel close.
    // Each Block will begin a new and concurrent execution task tracked in |pending|.
    let mut pending = futures::stream::FuturesUnordered::new();

    // All Blocks will use a shared Combiner, which is drained and returned
    // when all Blocks have completed.
    let combiner = Combiner::new(
        ctx.schema_index,
        &ctx.derivation_schema,
        ctx.derivation_key.clone(),
    );
    let mut registers = PendingPipeline::new(registers);
    let mut combiner = PendingPipeline::new(combiner);

    let mut rx_block = rx_block.fuse();
    let mut id: usize = 1;
    loop {
        if pending.len() == BLOCK_PARALLELISM {
            // We must complete a Block before we can pull from |rx_block|.
            pending.select_next_some().await?;
        }

        // Read a Block completion, or a new Block to process.
        futures::select_biased! {
            completion = pending.select_next_some() => if let Err(err) = completion {
                return Err(err);
            },
            rx = rx_block.next() => match rx {
                Some(block) => {
                    pending.push(
                        process_block(
                            id,
                            block,
                            registers.chain_before(),
                            combiner.chain_before(),
                        )
                    );
                    id += 1;
                }
                None => break,
            },
        };
    }

    // We've read an |rx_block| close. Drain remaining |pending| tasks.
    while let Some(completion) = pending.next().await {
        if let Err(err) = completion {
            return Err(err);
        }
    }
    // Unwrap and return the Combiner and Registers.
    Ok((combiner.await.into_inner(), registers.await.into_inner()))
}

#[cfg(test)]
mod tests {
    use super::{super::context::Transform, super::test::LambdaTestServer, *};
    use serde_json::{json, Value};
    use tuple::TuplePack;
    use url::Url;

    #[test]
    fn test_basic_rpc() {
        setup_env_tracing();

        let env = rocksdb::Env::mem_env().unwrap();
        let mut api = TestAPI::new(&env);

        assert_eq!(api.inner.restore_checkpoint().unwrap(), Default::default());

        api.inner.begin_transaction().unwrap();
        api.apply_documents(vec![
            (TF_INC, json!({"key": "a"})),  // => 1.
            (TF_INC, json!({"key": "a"})),  // => 2.
            (TF_INC, json!({"key": "bb"})), // => 1.
            (TF_PUB, json!({"key": "bb"})), // Pub 1.
            (TF_PUB, json!({"key": "a"})),  // Pub 2.
            (TF_INC, json!({"key": "bb"})), // => 2.
            (TF_INC, json!({"key": "bb"})), // => 3.
        ]);

        api.apply_documents(vec![
            (TF_PUB, json!({"key": "ccc"})),
            (TF_INC, json!({"key": "bb"})),              // => 4.
            (TF_RST, json!({"key": "bb", "reset": 15})), // Pub 4, => 15.
            (TF_INC, json!({"key": "bb"})),              // => 16.
            (TF_RST, json!({"key": "a", "reset": 0})),   // Pub 2, => 0.
            (TF_INC, json!({"key": "a"})),               // => 1.
            (TF_INC, json!({"key": "a"})),               // => 2.
            (TF_PUB, json!({"key": "a"})),               // Pub 2.
            (TF_PUB, json!({"key": "bb"})),              // Pub 16.
        ]);

        let mut arena = Vec::new();
        let mut out = Vec::new();

        let flush = api.inner.flush_transaction(
            derive_api::Flush {
                uuid_placeholder_ptr: "/_uuid".to_owned(),
                field_ptrs: vec!["/reset".to_owned(), "/key".to_owned()],
            },
            &mut arena,
            &mut out,
        );
        futures::executor::block_on(flush).unwrap();

        api.inner
            .prepare_commit(derive_api::Prepare {
                checkpoint: Some(Default::default()),
            })
            .unwrap();

        // |arena| & |out| hold three documents, with body, keys, fields of:
        //   {"_uuid": UUID_PLACEHOLDER, "key": "a", "reset": 0, "values": [1002, 1002, 2]}, "a", [0, "a"]
        //   {"_uuid": UUID_PLACEHOLDER, "key": "bb", "reset": 15, "values": [1001, 1004, 16]}, "bb", [15, "bb"]
        //   {"_uuid": UUID_PLACEHOLDER, "key": "ccc", "values": [1000]}, "ccc", [null, "ccc"]
        insta::assert_debug_snapshot!((String::from_utf8_lossy(&arena), out));

        // Left ready for the next transaction.
        assert!(matches!(api.inner.state, State::BeginTxn(..)));
    }

    #[test]
    fn test_register_validation_error() {
        let env = rocksdb::Env::mem_env().unwrap();
        let mut api = TestAPI::new(&env);

        assert_eq!(api.inner.restore_checkpoint().unwrap(), Default::default());
        api.inner.begin_transaction().unwrap();

        api.apply_documents(vec![
            (TF_RST, json!({"key": "foobar", "reset": -1})), // => 1.
        ]);

        let (mut arena, mut out) = (Vec::new(), Vec::new());
        let flush = api
            .inner
            .flush_transaction(Default::default(), &mut arena, &mut out);

        insta::assert_display_snapshot!(futures::executor::block_on(flush).unwrap_err(), @r###"
        register reduction error: document is invalid: {
          "document": {
            "type": "set",
            "value": "negative one!"
          },
          "basic_output": {
            "errors": [
              {
                "absoluteKeywordLocation": "https://schema/#/$defs/register",
                "error": "OneOfNotMatched",
                "instanceLocation": "",
                "keywordLocation": "#"
              }
            ],
            "valid": false
          }
        }
        "###);

        // Left in an invalid state.
        assert!(matches!(api.inner.state, State::Invalid));
    }

    #[test]
    fn test_derived_doc_validation_error() {
        let env = rocksdb::Env::mem_env().unwrap();
        let mut api = TestAPI::new(&env);

        assert_eq!(api.inner.restore_checkpoint().unwrap(), Default::default());
        api.inner.begin_transaction().unwrap();

        api.apply_documents(vec![(
            TF_PUB,
            json!({"key": "foobar", "invalid-property": 42}),
        )]);

        let (mut arena, mut out) = (Vec::new(), Vec::new());
        let flush = api
            .inner
            .flush_transaction(Default::default(), &mut arena, &mut out);

        insta::assert_display_snapshot!(futures::executor::block_on(flush).unwrap_err(), @r###"
        derived document reduction error: document is invalid: {
          "document": {
            "invalid-property": 42,
            "key": "foobar",
            "values": [
              1000
            ]
          },
          "basic_output": {
            "errors": [
              {
                "absoluteKeywordLocation": "https://schema/#/$defs/derived/properties/invalid-property",
                "error": "Invalid(False)",
                "instanceLocation": "/invalid-property",
                "keywordLocation": "#/properties/invalid-property"
              }
            ],
            "valid": false
          }
        }
        "###);

        // Left in an invalid state.
        assert!(matches!(api.inner.state, State::Invalid));
    }

    // Short-hand constants for transform IDs used in the test fixture.
    const TF_INC: i32 = 32;
    const TF_PUB: i32 = 42;
    const TF_RST: i32 = 52;

    struct TestAPI<'e> {
        inner: APIInner<'e>,
        // Hold LambdaTestServer & TempDir for side-effects.
        _do_increment: Option<LambdaTestServer>,
        _do_publish: Option<LambdaTestServer>,
        _do_reset: Option<LambdaTestServer>,
        _tmpdir: tempfile::TempDir,
    }

    impl<'e> TestAPI<'e> {
        fn new(env: &'e rocksdb::Env) -> TestAPI {
            // Combined schema used for test fixtures.
            let schema = json!({
                "$defs": {
                    "source": {
                        "type": "object",
                        "properties": {
                            "key": {"type": "string"},
                            "reset": {"type": "integer"},
                        },
                        "required": ["key"],
                    },
                    "register": {
                        "type": "object",
                        "reduce": {"strategy": "merge"},

                        "oneOf": [
                            {
                                "properties": {
                                    "type": {"const": "set"},
                                    "value": {
                                        "type": "integer",
                                        "reduce": {"strategy": "lastWriteWins"},
                                    },
                                },
                            },
                            {
                                "properties": {
                                    "type": {"const": "add"},
                                    "value": {
                                        "type": "integer",
                                        "reduce": {"strategy": "sum"},
                                    },
                                },
                            },
                        ],
                        "required": ["type", "value"],
                    },
                    "derived": {
                        "$ref": "#/$defs/source",
                        "reduce": {"strategy": "merge"},

                        "properties": {
                            "values": {
                                "type": "array",
                                "items": {"type": "integer"},
                                "reduce": {"strategy": "append"},
                            },
                            "invalid-property": false,
                        },
                        "required": ["values"],
                    },
                }
            });

            // Build and index the schema, then leak for `static lifetime.
            let schema_url = Url::parse("https://schema").unwrap();
            let schema: doc::Schema =
                json::schema::build::build_schema(schema_url.clone(), &schema).unwrap();
            let schema = Box::leak(Box::new(schema));

            let mut schema_index = doc::SchemaIndex::new();
            schema_index.add(schema).unwrap();
            schema_index.verify_references().unwrap();
            let schema_index = Box::leak(Box::new(schema_index));

            let runtime = APIInner::build_runtime();

            // Build a lambda which increments the current register value by one.
            let enter_guard = runtime.enter();
            let do_increment = LambdaTestServer::start_v2(|_source, _register, _previous| {
                // Return two register updates with an effective increment of 1.
                vec![
                    json!({"type": "add", "value": 3}),
                    json!({"type": "add", "value": -2}),
                ]
            });
            // Build a lambda which resets the register from a value of the source document.
            let do_reset = LambdaTestServer::start_v2(|source, _register, _previous| {
                let to = source.pointer("/reset").unwrap().as_i64().unwrap();

                // Emit an invalid register document on seeing value -1.
                if to == -1 {
                    vec![json!({"type": "set", "value": "negative one!"})]
                } else {
                    vec![json!({"type": "set", "value": to})]
                }
            });
            // Build a lambda which joins the source with its previous register.
            let do_publish = LambdaTestServer::start_v2(|source, _register, previous| {
                // Join |src| with the register value before its update.
                let mut doc = source.as_object().unwrap().clone();
                doc.insert(
                    "values".to_owned(),
                    json!([previous.unwrap().pointer("/value").unwrap().clone()]),
                );

                vec![Value::Object(doc)]
            });

            // Assemble transforms for our context.
            let transforms = vec![
                // Transform which increments the register.
                Transform {
                    transform_id: TF_INC,
                    source_schema: schema_url.join("#/$defs/source").unwrap(),
                    update: do_increment.lambda.clone(),
                    publish: Lambda::Noop,
                    index: 0,
                },
                // Transform which publishes the current register.
                Transform {
                    transform_id: TF_PUB,
                    source_schema: schema_url.join("#/$defs/source").unwrap(),
                    update: Lambda::Noop,
                    publish: do_publish.lambda.clone(),
                    index: 1,
                },
                // Transform which resets the register, and publishes its prior value.
                Transform {
                    transform_id: TF_RST,
                    source_schema: schema_url.join("#/$defs/source").unwrap(),
                    update: do_reset.lambda.clone(),
                    publish: do_publish.lambda.clone(),
                    index: 2,
                },
            ];

            let ctx = Context {
                transforms,
                schema_index,

                derivation_id: 1234,
                derivation_name: "a/derived/collection".to_owned(),
                derivation_schema: schema_url.join("#/$defs/derived").unwrap(),
                derivation_key: vec!["/key".into()].into(),

                register_schema: schema_url.join("#/$defs/register").unwrap(),
                register_initial: json!({"value": 1000}),
            };

            let tmpdir = tempfile::TempDir::new().unwrap();
            std::mem::drop(enter_guard);

            let inner =
                APIInner::from_parts(runtime, schema_index, ctx, env, tmpdir.path(), None).unwrap();

            Self {
                inner,
                _do_increment: Some(do_increment),
                _do_publish: Some(do_publish),
                _do_reset: Some(do_reset),
                _tmpdir: tmpdir,
            }
        }

        // Apply a sequence of CONTINUE_TXN documents, followed by an ACK_TXN.
        fn apply_documents(&mut self, documents: Vec<(i32, Value)>) {
            let mut w = Vec::new();

            for (tf_id, doc) in documents {
                self.inner
                    .doc_header(derive_api::DocHeader {
                        transform_id: tf_id,
                        uuid: Some(flow::UuidParts {
                            clock: 0,
                            producer_and_flags: message_flags::CONTINUE_TXN,
                        }),
                        packed_key: doc.pointer("/key").unwrap_or(&Value::Null).pack_to_vec(),
                    })
                    .unwrap();

                serde_json::to_writer(&mut w, &doc).unwrap();
                futures::executor::block_on(self.inner.doc_body(&w)).unwrap();
                w.clear();
            }

            // Send a trailing, empty ACK_TXN.
            self.inner
                .doc_header(derive_api::DocHeader {
                    transform_id: 0,
                    uuid: Some(flow::UuidParts {
                        clock: 0,
                        producer_and_flags: message_flags::ACK_TXN,
                    }),
                    packed_key: Value::Null.pack_to_vec(),
                })
                .unwrap();
            futures::executor::block_on(self.inner.doc_body(b"garbage (not used)")).unwrap();
        }
    }
}
