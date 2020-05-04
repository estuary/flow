use super::{Error, NodeJsHandle, RecordBatch};
use bytes::Bytes;
use futures::{
    channel::mpsc,
    Stream, Sink, SinkExt, StreamExt,
};
use pin_utils::pin_mut;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio;
use url::Url;
use uuid::Uuid;

pub struct TxnCtx {
    loopback: Url,
    // Index of source collection name => transform_ids to invoke.
    src_to_transforms: BTreeMap<String, Vec<i64>>,

    node: NodeJsHandle,
    // TODO(johnny): SQLite context?
}

impl TxnCtx {
    pub fn new(lo: Url, node: NodeJsHandle) -> TxnCtx {
        TxnCtx {
            loopback: lo,
            node,
            src_to_transforms: BTreeMap::new(),
        }
    }
}

struct Txn {
    ctx: Arc<Box<TxnCtx>>,
    node_streams: BTreeMap<i64, (hyper::body::Sender, tokio::task::JoinHandle<()>)>,
    next_uuid: Uuid,
    input_chunk: Bytes,
}

// Big fat event loop:
//  - Creates mpsc from node transforms.
//  - multiplexes over mpsc and input data:
//      - On data:
//        - Parses into Envelope AND validates contained doc schema (do this always?).
//        - For each transform of Env collection:
//        - If node:
//           - if not started, start & spawn read loop when sends into mpsc (or pass in & use forward?).
//           - Write message into txn sender.
//        - If sqlite:
//           - Queue parsed message into VFS input table.
//        - After parsing all messages in Data chunk, if queued inputs to SQLite transforms,
//          run them and dispatch results (to accumulator).
//
//      - On nodejs response data:
//        - dispatch data (as &[u8]).
//        - Parse into Value AND validate derived schema.
//          - extract composite key _hash_.
//          - extract partitioned fields.
//          - combine into accumulator
//

impl Txn {
    fn new(ctx: Arc<Box<TxnCtx>>, next_uuid: Uuid) -> Txn {
        Txn {
            ctx,
            node_streams: BTreeMap::new(),
            next_uuid,
            input_chunk: Bytes::new(),
        }
    }

    async fn input_batch(
        &mut self,
        batch: RecordBatch,
        derived_tx: &(impl Sink<Result<RecordBatch, Error>> + Send + Clone + 'static),
    ) -> Result<(), Error> {

        let tx = self.nodejs_stream(8, derived_tx).await?;
        tx.send_data(batch.bytes().clone()).await?;

        Ok(())
    }

    async fn nodejs_stream(
        &mut self,
        transform_id: i64,
        out_tx: &(impl Sink<Result<RecordBatch, Error>> + Send + Clone + 'static),
    ) -> Result<&mut hyper::body::Sender, Error>
    {
        let entry = self.node_streams.entry(transform_id);

        use std::collections::btree_map::Entry;
        if let Entry::Occupied(occ) = entry {
            return Ok(&mut occ.into_mut().0);
        }
        // We must start a new stream.

        let (tx, rx) = self.ctx.node.transform(transform_id, &self.ctx.loopback).await?;
        let out_tx_clone = out_tx.clone();

        let join_handle = tokio::spawn(async move {
            pin_utils::pin_mut!(rx);
            pin_utils::pin_mut!(out_tx_clone);

            while let Some(batch) = rx.next().await {
                out_tx_clone.send(batch).await;
            }
        });
        Ok(&mut entry.or_insert((tx, join_handle)).0)
    }

    async fn input_eof(&mut self) -> Result<(), Error> {
        Ok(())
    }

    /*
    fn queue_input(&mut self, mut chunk: Bytes) {
        // Join |chunk| with any remainder from the last input chunk.
        if !self.input_chunk.is_empty() {
            chunk = BytesMut::from_iter(self.input_chunk.iter().chain(chunk.into_iter())).freeze();
        }
        self.input_chunk = chunk;
    }


    async fn drain_input<O, OF, NTX>(
        &mut self,
        _output: O,
        _node_out_tx: &NTX,
    ) -> Result<bool, Error>
    where
        O: FnMut(&[u8]) -> OF, // MAYBE this is another borrowed OutputRecord type or something.
        OF: Future<Output = ()>,
        NTX: Sink<Result<Bytes, Error>> + Clone,
    {
        let mut de = serde_json::Deserializer::from_slice(&self.input_chunk);

        let env = SourceEnvelope::deserialize(&mut de);
        let env = match env {
            Err(err) if err.is_eof() => return Ok(false),
            Err(err) => return Err(err.into()),
            Ok(v) => v,
        };

        // A little hack-y, but we need the reader byte offset at the end of parsing
        // this doc and it's not exposed outside of StreamDeserializer.
        let mut de = serde_json::Deserializer::from_slice(&self.input_chunk).into_iter::<RawDocument>();
        let doc = match de.next() {
            None => return Ok(false),
            Some(Err(err)) if err.is_eof() => return Ok(false),
            Some(Err(err)) => return Err(err.into()),
            Some(Ok(v)) => v,
        };
        let offset = de.byte_offset();



        // CAREFUL, this can deadlock, because we aren't reading node output and could
        // hit the window limit when sending node input. We need fully separate processing
        // of the node output path.

        // After dropping env & doc...
        self.input_chunk.advance(offset);

        /*
        let de = serde_json::Deserializer::from_slice(&self.chunk).into_iter::<SourceEnvelope>();
        for env in stream {
            let env = env?;

            let transforms = self.ctx.src_to_transforms
                .get(&env.collection)
                .ok_or_else(|| Error::UnknownSourceCollection(env.collection.into_string()))?;

            for t_id in transforms.iter() {
                self.node_stream(*t_id).send_data(Bytes::env.value)
            }


        }
         */
        Ok(false)
    }

    async fn input_eof(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn next_node_output<O, OF>(&mut self, _data: Bytes, _output: O) -> Result<(), Error>
    where
        O: FnMut(&[u8]) -> OF, // MAYBE this is another borrowed OutputRecord type or something.
        OF: Future<Output = ()>,
    {
        Ok(())
    }
     */
}

pub async fn txn_run(
    input: impl Stream<Item = Result<RecordBatch, Error>> + Send + 'static,
    ctx: Arc<Box<TxnCtx>>,
    seq_from: Uuid,
) -> impl Stream<Item = Result<RecordBatch, Error>> {
    // TODO(johnny) Verify we're in an allowed execution state.

    let mut txn = Txn::new(ctx, seq_from);
    let (mut derived_tx, mut derived_rx) = mpsc::channel::<Result<RecordBatch, Error>>(8);

    // Consume |input| in an async task.
    let consume_handle = tokio::spawn(async move {
        pin_mut!(input);

        while let Some(batch) = input.next().await {
            let batch = batch?;
            txn.input_batch(batch, &mut derived_tx).await?;
        }
        txn.input_eof().await?;
        Result::<(), Error>::Ok(())
    });

    async_stream::stream! {
        while let Some(batch) = derived_rx.next().await {
            yield batch;
        }
        // Forward a terminal error of |consume_handle|, if any.
        if let Err(err) = consume_handle.await {
            yield Result::<RecordBatch, Error>::Err(err.into());
        }
    }
}
