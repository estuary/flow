use super::Task;
use super::state::{Deltas, DrainStoresComplete, ScanComplete};
use crate::{Accumulator, patches, proto};
use anyhow::Context;
use bytes::{BufMut, Bytes};
use futures::{FutureExt, SinkExt, StreamExt, future::BoxFuture};
use proto_flow::materialize;
use shuffle::log::{FrontierScan, Reader, Remainder};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::AtomicBool;
use tokio::sync::mpsc;
use xxhash_rust::xxh3::xxh3_128;

/// Shard-side materialization reactor for one joined leader session.
pub(super) struct Actor {
    pub controller_tx: mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    pub leader_tx: mpsc::UnboundedSender<proto::Materialize>,
    pub connector_tx: futures::channel::mpsc::Sender<materialize::Request>,
    pub db: Option<(crate::shard::RocksDB, Vec<String>)>,
    pub accumulator: Option<Accumulator>,
    pub task: Option<Task>,
    pub committed_max_keys: BTreeMap<u32, Bytes>,
    pub disable_load_optimization: bool,
    load_keys: LoadKeySet,
    pub shuffle_reader: Option<Reader>,
    pub shuffle_remainders: VecDeque<Remainder>,
    pub persist_fut: Option<
        BoxFuture<
            'static,
            anyhow::Result<((crate::shard::RocksDB, Vec<String>), proto::Persisted)>,
        >,
    >,
    pub drain_state: Option<DrainState>,
    pub deltas: Deltas,
}

pub(super) struct DrainState {
    pub drainer: doc::combine::Drainer,
    pub parser: simd_doc::Parser,
    pub task: Task,
    pub summary: DrainStoresComplete,
}

const DRAIN_CHUNK_STORES: usize = 1;

type LoadKeySet = std::collections::HashSet<u128, std::hash::BuildHasherDefault<IdentHasher>>;

#[derive(Default)]
struct IdentHasher(u64);

impl std::hash::Hasher for IdentHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0 = u64::from_ne_bytes(bytes[..8].try_into().unwrap());
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

impl Actor {
    pub fn new(
        controller_tx: mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
        leader_tx: mpsc::UnboundedSender<proto::Materialize>,
        connector_tx: futures::channel::mpsc::Sender<materialize::Request>,
        db: crate::shard::RocksDB,
        binding_state_keys: Vec<String>,
        task: Task,
        committed_max_keys: BTreeMap<u32, Bytes>,
        disable_load_optimization: bool,
        shuffle_reader: Reader,
    ) -> anyhow::Result<Self> {
        let accumulator =
            Accumulator::new(task.combine_spec()?).context("building materialize combiner")?;

        Ok(Self {
            controller_tx,
            leader_tx,
            connector_tx,
            db: Some((db, binding_state_keys)),
            accumulator: Some(accumulator),
            task: Some(task),
            committed_max_keys,
            disable_load_optimization,
            load_keys: LoadKeySet::default(),
            shuffle_reader: Some(shuffle_reader),
            shuffle_remainders: VecDeque::new(),
            persist_fut: None,
            drain_state: None,
            deltas: Deltas::default(),
        })
    }

    #[tracing::instrument(level = "debug", err(Debug, level = "warn"), skip_all)]
    pub async fn serve<R, C>(
        &mut self,
        controller_rx: &mut R,
        leader_rx: &mut tonic::Streaming<proto::Materialize>,
        connector_rx: &mut C,
    ) -> anyhow::Result<()>
    where
        R: futures::Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
        C: futures::Stream<Item = tonic::Result<materialize::Response>> + Send + Unpin + 'static,
    {
        let mut stop_forwarded = false;

        loop {
            tokio::select! {
                biased;

                result = maybe_fut(&mut self.persist_fut) => {
                    if let Some(result) = result {
                        let (db, persisted) = result.context("RocksDB Persist")?;
                        self.db = Some(db);
                        _ = self.leader_tx.send(proto::Materialize {
                            persisted: Some(persisted),
                            ..Default::default()
                        });
                    }
                }
                msg = leader_rx.next() => {
                    let Some(msg) = msg else {
                        anyhow::bail!("leader stream EOF before Stopped");
                    };
                    let msg = msg.map_err(crate::status_to_anyhow).context("leader stream")?;
                    if self.on_leader_message(msg).await? {
                        return Ok(());
                    }
                }
                response = connector_rx.next() => {
                    let Some(response) = response else {
                        anyhow::bail!("connector stream EOF during materialization session");
                    };
                    self.on_connector_resp(response?).await?;
                }
                _ = std::future::ready(()), if self.drain_state.is_some() => {
                    self.drive_drain().await?;
                }
                msg = controller_rx.next(), if !stop_forwarded => {
                    match msg {
                        None => {
                            _ = self.leader_tx.send(proto::Materialize {
                                stop: Some(proto::Stop {}),
                                ..Default::default()
                            });
                            stop_forwarded = true;
                        }
                        Some(Err(status)) => {
                            return Err(crate::status_to_anyhow(status)
                                .context("controller stream error during session"));
                        }
                        Some(Ok(msg)) if msg.stop.is_some() => {
                            _ = self.leader_tx.send(proto::Materialize {
                                stop: Some(proto::Stop {}),
                                ..Default::default()
                            });
                            stop_forwarded = true;
                        }
                        Some(Ok(msg)) if msg.close_now.is_some() => {
                            _ = self.leader_tx.send(proto::Materialize {
                                close_now: Some(proto::CloseNow {}),
                                ..Default::default()
                            });
                        }
                        Some(Ok(msg)) => {
                            anyhow::bail!(
                                "unexpected controller message during session: {msg:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    async fn on_leader_message(&mut self, msg: proto::Materialize) -> anyhow::Result<bool> {
        if msg.stopped.is_some() {
            _ = self.controller_tx.send(Ok(proto::Materialize {
                stopped: Some(proto::Stopped {}),
                ..Default::default()
            }));
            return Ok(true);
        } else if let Some(load) = msg.load {
            let Some(proto) = load.frontier else {
                return Ok(false);
            };
            let frontier =
                shuffle::Frontier::decode(proto).context("invalid Frontier delta on L:Load")?;
            let scan = self.scan_frontier(frontier).await?;
            self.fold_scan_complete(scan);
            let loaded = proto::materialize::Loaded {
                combiner_usage_bytes: self.deltas.combiner_usage_bytes,
                max_key_deltas: std::mem::take(&mut self.deltas.max_key_deltas),
                binding_read: std::mem::take(&mut self.deltas.binding_read),
                binding_loaded: std::mem::take(&mut self.deltas.binding_loaded),
            };
            _ = self.leader_tx.send(proto::Materialize {
                loaded: Some(loaded),
                ..Default::default()
            });
        } else if let Some(flush) = msg.flush {
            self.connector_tx
                .send(materialize::Request {
                    flush: Some(materialize::request::Flush {
                        connector_state_patches_json: flush.connector_patches_json,
                    }),
                    ..Default::default()
                })
                .await
                .context("send C:Flush")?;
        } else if msg.store.is_some() {
            let task = self.task.take().context("start drain: missing task")?;
            let accumulator = self
                .accumulator
                .take()
                .context("start drain: missing accumulator")?;
            let (drainer, parser) = accumulator
                .into_drainer()
                .context("preparing combiner drain")?;
            self.load_keys.clear();
            self.drain_state = Some(DrainState {
                drainer,
                parser,
                task,
                summary: DrainStoresComplete::default(),
            });
        } else if let Some(start_commit) = msg.start_commit {
            self.connector_tx
                .send(materialize::Request {
                    start_commit: Some(materialize::request::StartCommit {
                        runtime_checkpoint: start_commit.connector_checkpoint,
                        connector_state_patches_json: start_commit.connector_patches_json,
                    }),
                    ..Default::default()
                })
                .await
                .context("send C:StartCommit")?;
        } else if let Some(acknowledge) = msg.acknowledge {
            self.connector_tx
                .send(materialize::Request {
                    acknowledge: Some(materialize::request::Acknowledge {
                        connector_state_patches_json: acknowledge.connector_patches_json,
                    }),
                    ..Default::default()
                })
                .await
                .context("send C:Acknowledge")?;
        } else if let Some(persist) = msg.persist {
            let nonce = persist.nonce;
            for (binding, key) in &persist.max_keys {
                self.committed_max_keys.insert(*binding, key.clone());
            }

            let (db, binding_state_keys) = self
                .db
                .take()
                .context("received L:Persist while a Persist is already in flight")?;
            self.persist_fut = Some(
                async move {
                    let db = db.persist(&persist, &binding_state_keys).await?;
                    Ok(((db, binding_state_keys), proto::Persisted { nonce }))
                }
                .boxed(),
            );
        } else {
            anyhow::bail!("unexpected leader message: {msg:?}");
        }

        Ok(false)
    }

    async fn on_connector_resp(&mut self, resp: materialize::Response) -> anyhow::Result<()> {
        if let Some(loaded) = &resp.loaded {
            let entry = self
                .deltas
                .binding_loaded
                .entry(loaded.binding)
                .or_default();
            entry.docs_total += 1;
            entry.bytes_total += loaded.doc_json.len() as u64;

            self.process_loaded(&resp).context("processing C:Loaded")?;
        } else if let Some(flushed) = resp.flushed {
            let binding_loaded = std::mem::take(&mut self.deltas.binding_loaded);
            _ = self.leader_tx.send(proto::Materialize {
                flushed: Some(proto::materialize::Flushed {
                    connector_patches_json: patches::encode_connector_state(flushed.state),
                    binding_loaded,
                }),
                ..Default::default()
            });
        } else if let Some(started_commit) = resp.started_commit {
            _ = self.leader_tx.send(proto::Materialize {
                started_commit: Some(proto::materialize::StartedCommit {
                    connector_patches_json: patches::encode_connector_state(started_commit.state),
                }),
                ..Default::default()
            });
        } else if let Some(acknowledged) = resp.acknowledged {
            _ = self.leader_tx.send(proto::Materialize {
                acknowledged: Some(proto::materialize::Acknowledged {
                    connector_patches_json: patches::encode_connector_state(acknowledged.state),
                }),
                ..Default::default()
            });
        } else {
            anyhow::bail!("unexpected connector response: {resp:?}");
        }

        Ok(())
    }

    fn fold_scan_complete(&mut self, scan: ScanComplete) {
        for (binding, docs_and_bytes) in scan.binding_read {
            let entry = self.deltas.binding_read.entry(binding).or_default();
            entry.docs_total += docs_and_bytes.docs_total;
            entry.bytes_total += docs_and_bytes.bytes_total;
        }
        for (binding, key) in scan.max_key_deltas {
            let entry = self.deltas.max_key_deltas.entry(binding).or_default();
            if *entry < key {
                *entry = key;
            }
        }
        self.deltas.combiner_usage_bytes = scan.combiner_usage_bytes;

        for (binding, clock) in scan.first_source_clock {
            self.deltas
                .first_source_clock
                .entry(binding)
                .and_modify(|prev| *prev = (*prev).min(clock))
                .or_insert(clock);
        }
        for (binding, clock) in scan.last_source_clock {
            self.deltas
                .last_source_clock
                .entry(binding)
                .and_modify(|prev| *prev = (*prev).max(clock))
                .or_insert(clock);
        }
    }

    async fn scan_frontier(&mut self, frontier: shuffle::Frontier) -> anyhow::Result<ScanComplete> {
        let task = self.task.take().context("scan_frontier: missing task")?;
        let mut accumulator = self
            .accumulator
            .take()
            .context("scan_frontier: missing accumulator")?;
        let reader = self
            .shuffle_reader
            .take()
            .context("scan_frontier: missing shuffle reader")?;
        let remainders = std::mem::take(&mut self.shuffle_remainders);

        let result = self
            .scan_inner(&task, &mut accumulator, reader, remainders, frontier)
            .await;

        self.task = Some(task);
        self.accumulator = Some(accumulator);
        result
    }

    async fn scan_inner(
        &mut self,
        task: &Task,
        accumulator: &mut Accumulator,
        reader: Reader,
        remainders: VecDeque<Remainder>,
        frontier: shuffle::Frontier,
    ) -> anyhow::Result<ScanComplete> {
        let mut scan =
            FrontierScan::new(frontier, reader, remainders).context("constructing FrontierScan")?;
        let mut summary = ScanComplete::default();
        let mut c_loads: Vec<materialize::Request> = Vec::new();

        while scan.advance_block().context("advancing FrontierScan")? {
            {
                let memtable = accumulator.memtable().context("acquiring memtable")?;
                let alloc = memtable.alloc();

                for entry in scan.block_iter() {
                    let binding_index = entry.meta.binding.to_native() as usize;
                    let binding = task.bindings.get(binding_index).with_context(|| {
                        format!("scanned binding index {binding_index} out of range")
                    })?;

                    let archived = entry.doc.doc.get();
                    let mut key_buf = bytes::BytesMut::new();
                    doc::Extractor::extract_all(archived, &binding.key_extractors, &mut key_buf);
                    let key_packed = key_buf.split().freeze();
                    let binding_index_u32 = binding_index as u32;

                    let prev_max = self
                        .committed_max_keys
                        .get(&binding_index_u32)
                        .cloned()
                        .unwrap_or_default();
                    if key_packed > prev_max {
                        let entry = summary.max_key_deltas.entry(binding_index_u32).or_default();
                        if *entry < key_packed {
                            *entry = key_packed.clone();
                        }
                    }
                    let known_absent = !prev_max.is_empty() && key_packed > prev_max;

                    let mut hash_buf = Vec::with_capacity(4 + key_packed.len());
                    hash_buf.put_u32(binding_index_u32);
                    hash_buf.extend_from_slice(&key_packed);
                    let key_hash = xxh3_128(&hash_buf);
                    if !binding.delta_updates
                        && (!known_absent || self.disable_load_optimization)
                        && self.load_keys.insert(key_hash)
                    {
                        c_loads.push(materialize::Request {
                            load: Some(materialize::request::Load {
                                binding: binding_index_u32,
                                key_json: Bytes::new(),
                                key_packed: key_packed.clone(),
                            }),
                            ..Default::default()
                        });
                    }

                    let schema_valid =
                        entry.meta.flags.to_native() & shuffle::FLAGS_SCHEMA_VALID != 0;
                    memtable
                        .add_embedded(
                            entry.meta.binding.to_native(),
                            &entry.doc.packed_key_prefix,
                            entry.doc.doc.to_heap(alloc),
                            false,
                            schema_valid,
                        )
                        .context("MemTable::add_embedded")?;

                    let docs_and_bytes = summary.binding_read.entry(binding_index_u32).or_default();
                    docs_and_bytes.docs_total += 1;
                    docs_and_bytes.bytes_total += entry.doc.source_byte_length.to_native() as u64;

                    let clock = entry.meta.clock.to_native();
                    summary
                        .first_source_clock
                        .entry(binding_index_u32)
                        .and_modify(|prev| *prev = (*prev).min(clock))
                        .or_insert(clock);
                    summary
                        .last_source_clock
                        .entry(binding_index_u32)
                        .and_modify(|prev| *prev = (*prev).max(clock))
                        .or_insert(clock);
                }
            }

            for request in c_loads.drain(..) {
                self.connector_tx
                    .send(request)
                    .await
                    .context("send C:Load")?;
            }
            tokio::task::yield_now().await;
        }

        let (_, reader, remainders) = scan.into_parts();
        self.shuffle_reader = Some(reader);
        self.shuffle_remainders = remainders;
        Ok(summary)
    }

    fn process_loaded(&mut self, response: &materialize::Response) -> anyhow::Result<()> {
        let materialize::Response {
            loaded: Some(materialize::response::Loaded { binding, doc_json }),
            ..
        } = response
        else {
            return Ok(());
        };

        let task = self.task.as_ref().context("process_loaded: missing task")?;
        let accumulator = self
            .accumulator
            .as_mut()
            .context("process_loaded: missing accumulator")?;
        let binding_index = *binding as usize;
        let binding_spec = task
            .bindings
            .get(binding_index)
            .ok_or_else(|| anyhow::anyhow!("Loaded binding {binding_index} out of range"))?;
        let (memtable, _alloc, doc) = accumulator
            .parse_json_doc(doc_json)
            .with_context(|| format!("parsing loaded doc for {}", binding_spec.collection_name))?;

        memtable.add(binding_index as u16, doc, true)?;
        Ok(())
    }

    async fn drive_drain(&mut self) -> anyhow::Result<()> {
        let mut state = self
            .drain_state
            .take()
            .expect("drive_drain called without drain_state");
        let mut buf = bytes::BytesMut::new();

        for _ in 0..DRAIN_CHUNK_STORES {
            let Some(drained) = state.drainer.drain_next().context("drain_next")? else {
                self.accumulator = Some(Accumulator::from_drainer(state.drainer, state.parser)?);
                self.task = Some(state.task);
                let first_source_clock = std::mem::take(&mut self.deltas.first_source_clock);
                let last_source_clock = std::mem::take(&mut self.deltas.last_source_clock);
                _ = self.leader_tx.send(proto::Materialize {
                    stored: Some(proto::materialize::Stored {
                        binding_stored: state.summary.binding_stored,
                        first_source_clock,
                        last_source_clock,
                    }),
                    ..Default::default()
                });
                return Ok(());
            };

            let binding_index = drained.meta.binding() as u32;
            let store = build_store(&mut buf, drained, &state.task);
            if let Some(store) = &store.store {
                let entry = state
                    .summary
                    .binding_stored
                    .entry(binding_index)
                    .or_default();
                entry.docs_total += 1;
                entry.bytes_total += store.doc_json.len() as u64
                    + store.key_packed.len() as u64
                    + store.values_packed.len() as u64;
            }

            self.connector_tx
                .send(store)
                .await
                .context("send C:Store")?;
        }

        self.drain_state = Some(state);
        Ok(())
    }

}

fn build_store(
    buf: &mut bytes::BytesMut,
    drained: doc::combine::DrainedDoc,
    task: &Task,
) -> materialize::Request {
    let doc::combine::DrainedDoc { meta, root } = drained;
    let binding_index = meta.binding();
    let binding = &task.bindings[binding_index];
    let truncation_indicator = AtomicBool::new(false);

    doc::Extractor::extract_all_owned_indicate_truncation(
        &root,
        &binding.key_extractors,
        buf,
        &truncation_indicator,
    );
    let key_packed = buf.split().freeze();

    serde_json::to_writer(
        buf.writer(),
        &binding
            .ser_policy
            .on_owned_with_truncation_indicator(&root, &truncation_indicator),
    )
    .expect("document serialization cannot fail");
    let mut doc_json = buf.split().freeze();

    doc::Extractor::extract_all_owned_indicate_truncation(
        &root,
        &binding.value_extractors,
        buf,
        &truncation_indicator,
    );
    let values_packed = buf.split().freeze();

    if !binding.store_document {
        doc_json.clear();
    }

    materialize::Request {
        store: Some(materialize::request::Store {
            binding: binding_index as u32,
            delete: meta.deleted(),
            doc_json,
            exists: meta.front(),
            key_json: Bytes::new(),
            key_packed,
            values_json: Bytes::new(),
            values_packed,
        }),
        ..Default::default()
    }
}

async fn maybe_fut<T>(opt: &mut Option<BoxFuture<'static, T>>) -> Option<T> {
    match opt.as_mut() {
        Some(fut) => {
            let result = fut.await;
            *opt = None;
            Some(result)
        }
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto_flow::flow;
    use proto_flow::materialize::response;

    fn make_actor() -> (
        Actor,
        mpsc::UnboundedReceiver<proto::Materialize>,
        futures::channel::mpsc::Receiver<materialize::Request>,
    ) {
        let (leader_tx, leader_rx) = mpsc::unbounded_channel();
        let (connector_tx, connector_rx) = futures::channel::mpsc::channel(8);
        let (controller_tx, _controller_rx) = mpsc::unbounded_channel();

        (
            Actor {
                controller_tx,
                leader_tx,
                connector_tx,
                db: None,
                accumulator: None,
                task: None,
                committed_max_keys: BTreeMap::new(),
                disable_load_optimization: false,
                load_keys: LoadKeySet::default(),
                shuffle_reader: None,
                shuffle_remainders: VecDeque::new(),
                persist_fut: None,
                drain_state: None,
                deltas: Deltas::default(),
            },
            leader_rx,
            connector_rx,
        )
    }

    #[tokio::test]
    async fn acknowledge_round_trip_forwards_patches() {
        let (mut actor, mut leader_rx, mut connector_rx) = make_actor();
        let patches = Bytes::from_static(br#"[{"ok":true}]"#);

        actor
            .on_leader_message(proto::Materialize {
                acknowledge: Some(proto::materialize::Acknowledge {
                    connector_patches_json: patches.clone(),
                }),
                ..Default::default()
            })
            .await
            .unwrap();

        let request = connector_rx.next().await.unwrap();
        assert_eq!(
            request.acknowledge.unwrap().connector_state_patches_json,
            patches
        );

        actor
            .on_connector_resp(materialize::Response {
                acknowledged: Some(response::Acknowledged {
                    state: Some(flow::ConnectorState {
                        updated_json: Bytes::from_static(br#"{"done":true}"#),
                        merge_patch: true,
                    }),
                }),
                ..Default::default()
            })
            .await
            .unwrap();

        let response = leader_rx.recv().await.unwrap();
        assert_eq!(
            response.acknowledged.unwrap().connector_patches_json,
            Bytes::from_static(
                br#"[{"done":true}
]"#
            )
        );
    }

}
