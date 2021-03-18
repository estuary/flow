use super::combiner;
use super::registers;

mod block;
mod invocation;

use block::{Block, BlockInvoke};
use futures::{stream::FuturesOrdered, StreamExt};
use invocation::Invocation;
use itertools::Itertools;
use protocol::{
    cgo,
    flow::{self, derive_api},
    message_flags,
};
use serde_json::Value;
use std::marker::PhantomData;
use std::task::{Context, Poll};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error("register reduction error")]
    RegisterErr(#[from] registers::Error),
    #[error("lambda returned fewer rows than expected")]
    TooFewRows,
    #[error("lambda returned more rows than expected")]
    TooManyRows,
    #[error("derived document reduction error")]
    Combiner(#[from] combiner::Error),
    #[error("failed to open registers RocksDB")]
    Rocks(#[from] rocksdb::Error),
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error("failed to invoke update lambda")]
    UpdateInvocationError(#[source] anyhow::Error),
    #[error("failed to invoke publish lambda")]
    PublishInvocationError(#[source] anyhow::Error),
    #[error("failed to parse lambda invocation response")]
    LambdaParseError(#[source] serde_json::Error),

    // TODO remove me
    #[error("source document validation error: {0:#}")]
    SourceValidation(doc::FailedValidation),
}

pub struct Pipeline<'e> {
    // Collection being derived.
    collection: flow::CollectionSpec,
    // Transforms of the derivation.
    transforms: Vec<flow::TransformSpec>,
    // Models of update & publish Invocations for each transform. These are kept
    // pristine, and cloned to produce working instances given to new Blocks.
    updates_model: Vec<Invocation>,
    publishes_model: Vec<Invocation>,
    // Next Block currently being constructed.
    next: Block,
    // Trampoline used for lambda Invocations.
    trampoline: cgo::Trampoline,
    // Invocation futures awaiting completion of "update" lambdas.
    await_update: FuturesOrdered<BlockInvoke>,
    // Invocation futures awaiting completion of "publish" lambdas.
    await_publish: FuturesOrdered<BlockInvoke>,
    // Registers updated by "update" lambdas.
    // Pipeline owns Registers, but it's pragmatically public due to
    // unrelated usages in derive_api (clearing registers & checkpoints).
    pub registers: registers::Registers,
    // Combiner of derived documents.
    combiner: combiner::Combiner,
    // Partitions to extract when draining the Combiner.
    partitions: Vec<doc::Pointer>,
    // This instance cannot outlive the RocksDB environment it uses.
    _env: PhantomData<&'e rocksdb::Env>,

    // TODO(johnny): remove me to the extraction API.
    validator: doc::Validator<'static>,
    source_schemas: Vec<url::Url>,
}

impl<'e> Pipeline<'e> {
    // Build a pipeline from a Config.
    pub fn from_config(cfg: derive_api::Config) -> Result<Self, Error> {
        // Re-hydrate a &rocksdb::Env from a provided memory address.
        let env_ptr = cfg.rocksdb_env_memptr as usize;
        let env: &rocksdb::Env = unsafe { std::mem::transmute(&env_ptr) };

        // Re-hydrate a &'static SchemaIndex from a provided memory address.
        let schema_index_memptr = cfg.schema_index_memptr as usize;
        let schema_index: &doc::SchemaIndex = unsafe { std::mem::transmute(schema_index_memptr) };

        Self::from_config_and_parts(cfg, env, schema_index)
    }

    fn from_config_and_parts(
        cfg: derive_api::Config,
        env: &'e rocksdb::Env,
        schema_index: &'static doc::SchemaIndex,
    ) -> Result<Self, Error> {
        let derive_api::Config {
            derivation,
            local_dir,
            rocksdb_env_memptr,
            schema_index_memptr,
        } = cfg;

        let flow::DerivationSpec {
            collection,
            transforms,
            register_initial_json,
            register_schema_uri,
        } = derivation.unwrap_or_default();

        let collection = collection.unwrap_or_default();

        tracing::debug!(
            ?collection,
            ?local_dir,
            ?register_initial_json,
            ?register_schema_uri,
            ?rocksdb_env_memptr,
            ?schema_index_memptr,
            ?transforms,
            "building from config"
        );

        // Build pristine "model" Invocations that we'll clone for new Blocks.
        let (updates_model, publishes_model): (Vec<_>, Vec<_>) = transforms
            .iter()
            .map(|tf| {
                (
                    Invocation::new(tf.update_lambda.as_ref()),
                    Invocation::new(tf.publish_lambda.as_ref()),
                )
            })
            .unzip();

        let first_block = Block::new(1, &updates_model, &publishes_model);

        // Open RocksDB and build Registers.
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
            &url::Url::parse(&register_schema_uri)?,
            serde_json::from_str(&register_initial_json)?,
        );

        // Build Combiner.
        let combiner = combiner::Combiner::new(
            schema_index,
            &url::Url::parse(&collection.schema_uri)?,
            collection
                .key_ptrs
                .iter()
                .map(|k| doc::Pointer::from_str(k))
                .collect::<Vec<_>>()
                .into(),
        );

        // Identify partitions to extract on combiner drain.
        let partitions = collection
            .projections
            .iter()
            // Projections are already sorted by field, but defensively sort again.
            .sorted_by_key(|proj| &proj.field)
            .filter_map(|proj| {
                if proj.is_partition_key {
                    Some(doc::Pointer::from_str(&proj.ptr))
                } else {
                    None
                }
            })
            .collect();

        // TODO remove me to extraction API.
        let validator = doc::Validator::new(schema_index);
        let source_schemas = transforms
            .iter()
            .map(|tf| {
                url::Url::parse(
                    &tf.shuffle
                        .as_ref()
                        .map(|s| s.source_schema_uri.clone())
                        .unwrap_or_default(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            collection,
            transforms,
            updates_model,
            publishes_model,
            next: first_block,
            trampoline: cgo::Trampoline::new(),
            await_update: FuturesOrdered::new(),
            await_publish: FuturesOrdered::new(),
            registers,
            combiner,
            partitions,
            _env: PhantomData,
            validator,
            source_schemas,
        })
    }

    // Resolve a trampoline task.
    pub fn resolve_task(&self, data: &[u8]) {
        self.trampoline.resolve_task(data)
    }

    // Add a source document to the Pipeline, and return true if it caused the
    // current Block to flush (and false otherwise).
    //
    // TODO(johnny): this only errors due to document validations, which are
    // being moved out of the derive API. Switch to -> bool when able.
    pub fn add_source_document(
        &mut self,
        header: derive_api::DocHeader,
        body: &[u8],
    ) -> Result<bool, Error> {
        let derive_api::DocHeader {
            uuid,
            packed_key,
            transform_index,
        } = header;

        let uuid = uuid.unwrap_or_default();
        let flags = uuid.producer_and_flags & message_flags::MASK;

        if flags != message_flags::ACK_TXN {
            // TODO(johnny): Move source schema validation to read-time, avoiding this parsing.
            doc::Validation::validate(
                &mut self.validator,
                &self.source_schemas[transform_index as usize],
                serde_json::from_slice(body)?,
            )?
            .ok()
            .map_err(Error::SourceValidation)?;

            self.next
                .add_source(transform_index as usize, packed_key, body);
        }

        let dispatch =
            // Dispatch if we're over the size target.
            self.next.num_bytes >= BLOCK_SIZE_TARGET
            // Or if the block is non-empty and this document *isn't* a
            // continuation of an append transaction (e.x., where we can
            // expect a future ACK_TXN to be forthcoming), and there is
            // remaining concurrency.
            || (flags != message_flags::CONTINUE_TXN
                && self.await_update.len() < BLOCK_CONCURRENCY_TARGET
            );

        if dispatch {
            self.flush();
        }

        Ok(dispatch)
    }

    // Flush a partial Block to begin its processing.
    pub fn flush(&mut self) {
        if self.next.num_bytes == 0 {
            return;
        }
        let next = Block::new(self.next.id + 1, &self.updates_model, &self.publishes_model);
        let block = std::mem::replace(&mut self.next, next);
        self.await_update
            .push(block.invoke_updates(&self.trampoline));
    }

    // Poll pending Block invocations, processing all Blocks which immediately resolve.
    // Then, dispatch all started trampoline tasks to the provide vectors.
    pub fn poll_and_trampoline(
        &mut self,
        arena: &mut Vec<u8>,
        out: &mut Vec<cgo::Out>,
    ) -> Result<bool, Error> {
        let waker = futures::task::noop_waker();
        let mut ctx = Context::from_waker(&waker);

        // Process all ready blocks which were awaiting "update" lambda invocation.
        loop {
            match self.await_update.poll_next_unpin(&mut ctx) {
                Poll::Pending => break,
                Poll::Ready(None) => break,
                Poll::Ready(Some(result)) => {
                    let (mut block, tf_register_deltas) =
                        result.map_err(Error::UpdateInvocationError)?;

                    self.update_registers(&mut block, tf_register_deltas)?;

                    tracing::debug!(?block, "completed register updates, starting publishes");

                    self.await_publish
                        .push(block.invoke_publish(&self.trampoline));
                }
            }
        }

        // Process all ready blocks which were awaiting "publish" lambda invocation.
        loop {
            match self.await_publish.poll_next_unpin(&mut ctx) {
                Poll::Pending => break,
                Poll::Ready(None) => break,
                Poll::Ready(Some(result)) => {
                    let (mut block, tf_derived_docs) =
                        result.map_err(Error::PublishInvocationError)?;

                    self.combine_published(&mut block, tf_derived_docs)?;

                    tracing::debug!(?block, "completed publishes");
                }
            }
        }

        self.trampoline
            .dispatch_tasks(derive_api::Code::Trampoline as u32, arena, out);
        let idle = self.trampoline.is_empty();

        // Sanity check: the trampoline is empty / not empty only if
        // awaited futures are also empty / not empty.
        assert_eq!(
            idle,
            self.await_publish.is_empty() && self.await_update.is_empty()
        );

        Ok(idle)
    }

    // Drain the pipeline's combiner into the provide vectors.
    // This may be called only after polling to completion.
    pub fn drain(&mut self, arena: &mut Vec<u8>, out: &mut Vec<cgo::Out>) {
        assert_eq!(self.next.num_bytes, 0);
        assert!(self.trampoline.is_empty());

        crate::combine_api::drain_combiner(
            &mut self.combiner,
            &self.collection.uuid_ptr,
            &self.partitions,
            arena,
            out,
        )
    }

    fn update_registers(
        &mut self,
        block: &mut Block,
        // tf_register_deltas is an array of reducible register deltas
        //  ... for each source document
        //  ... for each transform
        tf_register_deltas: Vec<Vec<Vec<Value>>>,
    ) -> Result<(), Error> {
        // Load all registers in |keys|, so that we may read them below.
        self.registers.load(block.keys.iter())?;
        tracing::trace!(?block, registers = ?self.registers, "loaded registers");

        // Map into a vector of iterators over Vec<Value>.
        let mut tf_register_deltas = tf_register_deltas
            .into_iter()
            .map(|u| u.into_iter())
            .collect_vec();

        // Process documents in sequence, reducing the register updates of each
        // and accumulating register column buffers for future publish invocations.
        for (tf_ind, key) in block.transforms.iter().zip(block.keys.iter()) {
            let tf = &self.transforms[*tf_ind as usize];

            // If this transform has a update lambda, expect that we received zero or more
            // register deltas for this source document. Otherwise behave as if empty.
            let deltas = if tf.update_lambda.is_some() {
                tf_register_deltas[*tf_ind as usize]
                    .next()
                    .ok_or(Error::TooFewRows)?
            } else {
                Vec::new()
            };

            let publish = &mut block.publishes[*tf_ind as usize];
            publish.begin_register(self.registers.read(key));

            // If we have deltas to apply, reduce them and assemble into
            // a future publish invocation body.
            if !deltas.is_empty() {
                self.registers
                    .reduce(key, deltas.into_iter(), tf.rollback_on_register_conflict)?;
                publish.end_register(Some(self.registers.read(key)));
            } else {
                publish.end_register(None);
            }
        }
        tracing::trace!(?block, registers = ?self.registers, "reduced registers");

        // Verify that we precisely consumed expected outputs from each lambda.
        for mut it in tf_register_deltas {
            if let Some(_) = it.next() {
                return Err(Error::TooManyRows);
            }
        }

        Ok(())
    }

    fn combine_published(
        &mut self,
        block: &mut Block,
        // tf_derived_docs is an array of combined published documents
        //  ... for each source document
        //  ... for each transform
        tf_derived_docs: Vec<Vec<Vec<Value>>>,
    ) -> Result<(), Error> {
        // Map into a vector of iterators over Vec<Value>.
        let mut tf_derived_docs = tf_derived_docs
            .into_iter()
            .map(|u| u.into_iter())
            .collect_vec();

        for tf_ind in &block.transforms {
            let tf = &self.transforms[*tf_ind as usize];

            // If this transform has a publish lambda, expect that we received zero or more
            // derived documents for this source document. Otherwise behave as if empty.
            let derived_docs = if tf.publish_lambda.is_some() {
                tf_derived_docs[*tf_ind as usize]
                    .next()
                    .ok_or(Error::TooFewRows)?
            } else {
                Vec::new()
            };

            for doc in derived_docs {
                self.combiner.combine_right(doc)?;
            }
        }
        tracing::trace!(combiner = ?self.combiner, "combined documents");

        // Verify that we precisely consumed expected outputs from each lambda.
        for mut it in tf_derived_docs {
            if let Some(_) = it.next() {
                return Err(Error::TooManyRows);
            }
        }

        Ok(())
    }
}

const BLOCK_SIZE_TARGET: usize = 1 << 16;
const BLOCK_CONCURRENCY_TARGET: usize = 3;
