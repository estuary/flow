use super::{parse_record_batch, Error, NodeJsHandle, RecordBatch};
use bytes::Bytes;
use futures::{Sink, SinkExt, StreamExt};
use http_body::Body;
use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio;
use tokio::task::JoinHandle;
use url::Url;

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

pub struct Txn<DTX> {
    ctx: Arc<Box<TxnCtx>>,
    node_transforms: BTreeMap<i64, (hyper::body::Sender, JoinHandle<Result<(), Error>>)>,
    derived_tx: DTX,
}

impl<DTX, E> Txn<DTX>
where
    DTX: Sink<RecordBatch, Error = E> + Clone + Send + Sync + 'static,
    Error: From<E>,
{
    pub fn new(ctx: Arc<Box<TxnCtx>>, dtx: DTX) -> Txn<DTX> {
        Txn {
            ctx,
            node_transforms: BTreeMap::new(),
            derived_tx: dtx,
        }
    }

    pub async fn push_source_docs(&mut self, batch: RecordBatch) -> Result<(), Error> {
        let tx = self.nodejs_stream(8).await?;

        let batch = batch.to_bytes();
        log::info!("processing source record batch: {:?}", batch);
        tx.send_data(batch).await?;

        Ok(())
    }

    pub async fn input_eof(&mut self) -> Result<(), Error> {
        Ok(())
    }

    pub async fn nodejs_stream(
        &mut self,
        transform_id: i64,
    ) -> Result<&mut hyper::body::Sender, Error> {
        let entry = self.node_transforms.entry(transform_id);

        // Is a transform stream already started?
        if let BTreeEntry::Occupied(entry) = entry {
            let entry = entry.into_mut();
            // TODO Check if JoinHandle as failed.
            return Ok(&mut entry.0);
        }

        // Nope. We must begin one.
        let (sender, body) = self
            .ctx
            .node
            .start_transform(transform_id, &self.ctx.loopback)
            .await?;

        let dtx = self.derived_tx.clone();

        // Spawn a read-loop which pushes derived records to |derived_tx|.
        let read_loop_handle = tokio::spawn(async move {
            match Self::nodejs_read_loop(body, dtx).await {
                Err(err) => {
                    log::error!("NodeJS read-loop failed: {:?}", err);
                    Err(err)
                }
                Ok(()) => {
                    log::info!("NodeJS read-loop finished");
                    Ok(())
                }
            }
        });
        Ok(&mut entry.or_insert((sender, read_loop_handle)).0)
    }

    async fn nodejs_read_loop(body: hyper::Body, dtx: DTX) -> Result<(), Error> {
        let mut rem = Bytes::new();
        pin_utils::pin_mut!(body, dtx);

        while let Some(bytes) = body.next().await {
            if let Some(batch) = parse_record_batch(&mut rem, Some(bytes?))? {
                dtx.send(batch).await?;
            }
        }
        parse_record_batch(&mut rem, None)?;

        if let Some(trailers) = body.trailers().await? {
            // TODO - inspect for errors, and propagate.
            log::info!("got trailers! {:?}", trailers);
        } else {
            log::error!("missing expected trailers!");
            return Err(Error::NoSuccessTrailerRenameMe);
        }

        Result::<(), Error>::Ok(())
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
