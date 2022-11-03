use super::registers;
use super::ValidatorGuard;
use crate::{DocCounter, StatsAccumulator};
use anyhow::Context;
use block::{Block, BlockInvoke};
use doc::AsNode;
use futures::{stream::FuturesOrdered, StreamExt};
use invocation::{Invocation, InvokeOutput, InvokeStats};
use itertools::Itertools;
use proto_flow::flow::{self, derive_api};
use proto_gazette::{consumer, message_flags};
use std::rc::Rc;
use std::task;

mod block;
mod invocation;
#[cfg(test)]
mod pipeline_test;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error(transparent)]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error("register error")]
    RegisterErr(#[from] registers::Error),
    #[error("lambda returned fewer rows than expected")]
    TooFewRows,
    #[error("lambda returned more rows than expected")]
    TooManyRows,
    #[error("derived document reduction error")]
    Combiner(#[from] doc::combine::Error),
    #[error("failed to open registers RocksDB")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    Rocks(#[from] rocksdb::Error),
    #[error("failed to invoke update lambda")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    UpdateInvocationError(#[source] anyhow::Error),
    #[error("failed to invoke publish lambda")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    PublishInvocationError(#[source] anyhow::Error),
    #[error("failed to parse lambda invocation response")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    LambdaParseError(#[source] serde_json::Error),
    #[error(transparent)]
    #[serde(serialize_with = "crate::serialize_as_display")]
    Anyhow(#[from] anyhow::Error),
}

pub struct Pipeline {
    // Invocation futures awaiting completion of "update" lambdas.
    await_update: FuturesOrdered<BlockInvoke>,
    // Invocation futures awaiting completion of "publish" lambdas.
    await_publish: FuturesOrdered<BlockInvoke>,
    // Combiner which is accumulating or draining derived documents.
    combiner: doc::Combiner,
    // Statistics of the derived-document combiner.
    combiner_stats: DocCounter,
    // Key components of derived documents.
    document_key_ptrs: Rc<[doc::Pointer]>,
    // Schema against which documents must validate.
    document_schema_guard: ValidatorGuard,
    // JSON pointer to the derived document UUID.
    document_uuid_ptr: doc::Pointer,
    // Next Block currently being constructed.
    next: Block,
    // Partitions to extract when draining the Combiner.
    partitions: Vec<doc::Pointer>,
    // Models of publish Invocations for each transform. These are kept
    // pristine, and cloned to produce working instances given to new Blocks.
    publishes_model: Vec<Invocation>,
    // Initial value of registers which have not yet been written.
    register_initial: serde_json::Value,
    // Schema against which registers must validate.
    register_schema_guard: ValidatorGuard,
    // Registers updated by "update" lambdas.
    // Pipeline owns Registers, but it's pragmatically public due to
    // unrelated usages in derive_api (clearing registers & checkpoints).
    registers: registers::Registers,
    // Trampoline used for lambda Invocations.
    trampoline: cgo::Trampoline,
    // Transforms of the derivation.
    transforms: Vec<flow::TransformSpec>,
    // Statistics of pipeline transformations.
    transforms_stats: Vec<TransformStats>,
    // Models of update & publish Invocations for each transform. These are kept
    // pristine, and cloned to produce working instances given to new Blocks.
    updates_model: Vec<Invocation>,
}

/// Accumulates statistics about an individual transform within the pipeline.
#[derive(Default)]
struct TransformStats {
    input: DocCounter,
    update_lambda: InvokeStats,
    publish_lambda: InvokeStats,
}

impl StatsAccumulator for TransformStats {
    type Stats = derive_api::stats::TransformStats;

    fn drain(&mut self) -> Self::Stats {
        derive_api::stats::TransformStats {
            input: Some(self.input.drain()),
            update: Some(self.update_lambda.drain()),
            publish: Some(self.publish_lambda.drain()),
        }
    }
}

impl Pipeline {
    pub fn from_config_and_parts(
        cfg: derive_api::Config,
        registers: registers::Registers,
        block_id: usize,
    ) -> Result<Self, Error> {
        let derive_api::Config { derivation } = cfg;

        let flow::DerivationSpec {
            collection,
            transforms,
            register_initial_json,
            register_schema_uri: _,
            register_schema_json,
            shard_template: _,
            recovery_log_template: _,
        } = derivation.unwrap_or_default();

        let flow::CollectionSpec {
            key_ptrs: document_key_ptrs,
            projections,
            schema_json: document_schema_json,
            uuid_ptr: document_uuid_ptr,
            ..
        } = collection.unwrap_or_default();

        tracing::debug!(
            ?document_key_ptrs,
            %document_schema_json,
            %document_uuid_ptr,
            %register_initial_json,
            %register_schema_json,
            ?transforms,
            "building from config"
        );

        if document_key_ptrs.is_empty() {
            return Err(anyhow::anyhow!("derived collection key cannot be empty").into());
        }
        if document_uuid_ptr.is_empty() {
            return Err(anyhow::anyhow!("document uuid JSON-pointer cannot be empty").into());
        }

        let document_key_ptrs: Rc<[doc::Pointer]> =
            document_key_ptrs.iter().map(doc::Pointer::from).collect();
        let document_uuid_ptr = doc::Pointer::from(&document_uuid_ptr);

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

        let first_block = Block::new(block_id, &updates_model, &publishes_model);

        // Build Combiner.
        let document_schema_guard =
            ValidatorGuard::new(&document_schema_json).context("parsing collection schema")?;
        let combiner = doc::Combiner::new(
            document_key_ptrs.clone().into(),
            document_schema_guard.schema.curi.clone(),
            tempfile::tempfile().context("opening temporary spill file")?,
        )
        .map_err(Error::Combiner)?;

        // Identify partitions to extract on combiner drain.
        let partitions = projections
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

        let register_schema_guard =
            ValidatorGuard::new(&register_schema_json).context("parsing register schema")?;
        let register_initial = serde_json::from_str(&register_initial_json)
            .context("parsing register initial value")?;

        let mut transforms_stats = Vec::new();
        transforms_stats.resize_with(transforms.len(), TransformStats::default);

        Ok(Self {
            await_publish: FuturesOrdered::new(),
            await_update: FuturesOrdered::new(),
            combiner,
            combiner_stats: DocCounter::default(),
            document_key_ptrs,
            document_schema_guard,
            document_uuid_ptr,
            next: first_block,
            partitions,
            publishes_model,
            register_initial,
            register_schema_guard,
            registers,
            trampoline: cgo::Trampoline::new(),
            transforms,
            transforms_stats,
            updates_model,
        })
    }

    // Consume this Pipeline, returning its Registers and next Block ID.
    // This may only be called in between transactions, after draining.
    pub fn into_inner(self) -> (registers::Registers, usize) {
        assert_eq!(self.next.num_bytes, 0);
        assert!(self.trampoline.is_empty());

        (self.registers, self.next.id)
    }

    // Delegate to load the last persisted checkpoint.
    pub fn last_checkpoint(&self) -> Result<consumer::Checkpoint, registers::Error> {
        assert_eq!(self.next.num_bytes, 0);
        assert!(self.trampoline.is_empty());

        self.registers.last_checkpoint()
    }

    // Delegate to clear held registers.
    pub fn clear_registers(&mut self) -> Result<(), registers::Error> {
        assert_eq!(self.next.num_bytes, 0);
        assert!(self.trampoline.is_empty());

        self.registers.clear()
    }

    // Delegate to prepare the checkpoint for commit.
    pub fn prepare(
        &mut self,
        checkpoint: proto_gazette::consumer::Checkpoint,
    ) -> Result<(), registers::Error> {
        assert_eq!(self.next.num_bytes, 0);
        assert!(self.trampoline.is_empty());

        self.registers.prepare(checkpoint)
    }

    // Resolve a trampoline task.
    pub fn resolve_task(&self, data: &[u8]) {
        self.trampoline.resolve_task(data)
    }

    // Add a source document to the Pipeline, and return true if it caused the
    // current Block to flush (and false otherwise).
    //
    // Currently this never returns an error, but it may in the future if an
    // invocation performed deeper processing of the input |body|
    // (e.x. by deserializing into Deno V8 types).
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
        self.transforms_stats[transform_index as usize]
            .input
            .increment(body.len() as u32);

        let uuid = uuid.unwrap_or_default();
        let flags = uuid.producer_and_flags & message_flags::MASK;

        if flags != message_flags::ACK_TXN {
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
            .push_back(block.invoke_updates(&self.trampoline));
    }

    // Poll pending Block invocations, processing all Blocks which immediately resolve.
    // Then, dispatch all started trampoline tasks to the provide vectors.
    pub fn poll_and_trampoline(
        &mut self,
        arena: &mut Vec<u8>,
        out: &mut Vec<cgo::Out>,
    ) -> Result<bool, Error> {
        let waker = futures::task::noop_waker();
        let mut ctx = task::Context::from_waker(&waker);

        // Process all ready blocks which were awaiting "update" lambda invocation.
        loop {
            match self.await_update.poll_next_unpin(&mut ctx) {
                task::Poll::Pending => break,
                task::Poll::Ready(None) => break,
                task::Poll::Ready(Some(result)) => {
                    let (mut block, update_outputs) =
                        result.map_err(Error::UpdateInvocationError)?;

                    for (transform_index, result) in update_outputs.iter().enumerate() {
                        self.transforms_stats[transform_index]
                            .update_lambda
                            .add(&result.stats);
                    }
                    self.update_registers(&mut block, update_outputs)?;

                    tracing::debug!(?block, "completed register updates, starting publishes");

                    self.await_publish
                        .push_back(block.invoke_publish(&self.trampoline));
                }
            }
        }

        // Process all ready blocks which were awaiting "publish" lambda invocation.
        loop {
            match self.await_publish.poll_next_unpin(&mut ctx) {
                task::Poll::Pending => break,
                task::Poll::Ready(None) => break,
                task::Poll::Ready(Some(result)) => {
                    let (mut block, publish_outputs) =
                        result.map_err(Error::PublishInvocationError)?;

                    for (transform_index, result) in publish_outputs.iter().enumerate() {
                        self.transforms_stats[transform_index]
                            .publish_lambda
                            .add(&result.stats);
                    }

                    self.combine_published(&mut block, publish_outputs)?;
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

    // Drain a chunk of the pipeline's combiner into the provided vectors.
    // This may be called only after the pipeline has been flushed
    // and then polled to completion.
    pub fn drain_chunk(
        mut self,
        target_length: usize,
        arena: &mut Vec<u8>,
        out: &mut Vec<cgo::Out>,
    ) -> Result<(Self, bool), Error> {
        assert_eq!(self.next.num_bytes, 0);
        assert!(self.trampoline.is_empty());

        let mut drainer = match self.combiner {
            doc::Combiner::Accumulator(accumulator) => accumulator.into_drainer()?,
            doc::Combiner::Drainer(d) => d,
        };

        let more = crate::combine_api::drain_chunk(
            &mut drainer,
            target_length,
            &self.document_key_ptrs,
            &self.partitions,
            &mut self.document_schema_guard.validator,
            arena,
            out,
            &mut self.combiner_stats,
        )?;

        if more {
            self.combiner = doc::Combiner::Drainer(drainer);
            return Ok((self, true));
        }

        self.combiner = doc::Combiner::Accumulator(drainer.into_new_accumulator()?);

        // Send a final message with the stats for this transaction.
        cgo::send_message(
            derive_api::Code::DrainedStats as u32,
            &derive_api::Stats {
                output: Some(self.combiner_stats.drain()),
                registers: Some(self.registers.stats.drain()),
                transforms: self
                    .transforms_stats
                    .iter_mut()
                    .map(TransformStats::drain)
                    .collect(),
            },
            arena,
            out,
        );

        Ok((self, false))
    }

    fn update_registers(
        &mut self,
        block: &mut Block,
        // tf_register_deltas is an array of reducible register deltas
        //  ... for each source document
        //  ... for each transform
        tf_register_deltas: Vec<InvokeOutput>,
    ) -> Result<(), Error> {
        // Load all registers in |keys|, so that we may read them below.
        self.registers.load(block.keys.iter())?;
        tracing::trace!(?block, registers = ?self.registers, "loaded registers");

        // Map into a vector of iterators over Vec<Value>.
        let mut tf_register_deltas = tf_register_deltas
            .into_iter()
            .map(|u| u.parsed.into_iter())
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
            publish.begin_register(self.registers.read(key, &self.register_initial));

            // If we have deltas to apply, reduce them and assemble into
            // a future publish invocation body.
            if !deltas.is_empty() {
                self.registers.reduce(
                    key,
                    &self.register_schema_guard.schema.curi,
                    &self.register_initial,
                    deltas.into_iter(),
                    &mut self.register_schema_guard.validator,
                )?;
                publish.end_register(Some(self.registers.read(key, &self.register_initial)));
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
        tf_derived_docs: Vec<InvokeOutput>,
    ) -> Result<(), Error> {
        // Map into a vector of iterators over Vec<Value>.
        let mut tf_derived_docs = tf_derived_docs
            .into_iter()
            .map(|u| u.parsed.into_iter())
            .collect_vec();

        let memtable = match &mut self.combiner {
            doc::Combiner::Accumulator(accumulator) => accumulator.memtable()?,
            _ => panic!("implementation error: combiner is draining, not accumulating"),
        };

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
                // TODO(johnny): Deserialize into HeapNode directly, without serde_json::Value.
                // We'd need to first update the owned InvokeOutput type to bytes::Bytes
                // or similar first, which will make sense to do with Deno and/or using hyper
                // to directly invoke lambdas from rust.
                let mut doc = doc::HeapNode::from_node(doc.as_node(), memtable.alloc());

                if let Some(node) = self
                    .document_uuid_ptr
                    .create_heap_node(&mut doc, memtable.alloc())
                {
                    *node = doc::HeapNode::String(doc::HeapString(
                        memtable
                            .alloc()
                            .alloc_str(crate::combine_api::UUID_PLACEHOLDER),
                    ));
                }

                memtable.combine_right(doc, &mut self.document_schema_guard.validator)?;
            }
        }
        tracing::trace!(block = %block.transforms.len(), total = ?memtable.len(), "combined documents");

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

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_pipeline_stats() {
        let mut stats = Vec::new();
        stats.resize_with(3, TransformStats::default);

        stats[2].input.increment(42);
        stats[2].input.increment(42);
        stats[0].update_lambda.add(&InvokeStats {
            output: DocCounter::new(5, 999),
            total_duration: Duration::from_secs(2),
        });
        stats[0].update_lambda.add(&InvokeStats {
            output: DocCounter::new(1, 1),
            total_duration: Duration::from_secs(1),
        });
        stats[2].publish_lambda.add(&InvokeStats {
            output: DocCounter::new(3, 8192),
            total_duration: Duration::from_secs(3),
        });

        let actual: Vec<_> = stats.iter_mut().map(TransformStats::drain).collect();
        insta::assert_yaml_snapshot!(actual);
    }
}
