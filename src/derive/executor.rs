use super::{Error, NodeJsHandle, RecordBatch};
use crate::specs::derive::{RawDocument, SourceEnvelope};
use bytes::{Buf, Bytes, BytesMut};
//use futures::channel::mpsc;
//use futures::stream::FusedStream;
use futures::future::BoxFuture;
use futures::{Sink, StreamExt};
use serde::Deserialize;
use serde_json::de::Read;
use std::collections::BTreeMap;
use std::future::Future;
use std::iter::FromIterator;
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

struct Txn<'t> {
    ctx: &'t TxnCtx,
    node_streams: BTreeMap<i64, (hyper::body::Sender, BoxFuture<'t, Result<(), Error>>)>,
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

impl<'t> Txn<'t> {
    fn new(ctx: &'t TxnCtx, next_uuid: Uuid) -> Txn<'t> {
        Txn {
            ctx,
            node_streams: BTreeMap::new(),
            next_uuid,
            input_chunk: Bytes::new(),
        }
    }

    /*
    fn queue_input(&mut self, mut chunk: Bytes) {
        // Join |chunk| with any remainder from the last input chunk.
        if !self.input_chunk.is_empty() {
            chunk = BytesMut::from_iter(self.input_chunk.iter().chain(chunk.into_iter())).freeze();
        }
        self.input_chunk = chunk;
    }

    fn node_stream<NTX>(&mut self, t_id: i64, node_out_tx: &NTX) -> &mut hyper::body::Sender
    where
        NTX: Sink<Result<Bytes, Error>> + Clone,
    {
        if let Some((sender, _)) = self.node_streams.get_mut(&t_id) {
            return sender;
        }
        // We must start a new stream.
        let (sender, handle) = self.ctx.node.transform(t_id, &self.ctx.loopback, node_out_tx.clone());
        self.node_streams.insert(t_id, (sender, handle));
        return self.node_stream(t_id, node_out_tx);
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

/*
pub async fn txn_run<I, O, IE, OF>(
    ctx: Arc<Box<TxnCtx>>,
    seq_from: Uuid,
    mut input: I,
) -> (impl Stream<Item=Bytes>, impl Future<Output=Result<(),Error>>)
where
    I: TryStream<Ok = Bytes, Error = IE> + Send + Unpin + FusedStream,
    IE: Into<Error>,
{
    // Verify we're in an allowed execution state.

    let mut txn = Txn::new(&ctx, seq_from);
    let (node_out_tx, node_out_rx) = mpsc::channel::<Result<Bytes, Error>>(8);
    let mut node_out_rx = node_out_rx.fuse();

    // Multiplex over input and output.
    loop {
        futures::select!(
            recv = input.try_next() => match recv {
                Err(err) => return Err(err.into()),
                Ok(Some(data)) => {
                    txn.queue_input(data);
                    while txn.drain_input(&mut output, &node_out_tx).await? {}
                }
                Ok(None) => {
                    txn.input_eof().await?;
                    break;
                }
            },
            recv = node_out_rx.try_next() => match recv? {
                Some(data) => {
                    txn.next_node_output(data, &mut output).await?;
                }
                None => panic!("node_resp_tx shouldn't be dropped yet"),
            }
        );
    }

    // We've consumed all transaction input.
    drop(input);
    // We also won't start any further NodeJS transform request streams.
    // Drop our handle so that |node_out_rx| will end after live requests finish.
    drop(node_out_tx);

    while let Some(data) = node_out_rx.try_next().await? {
        txn.next_node_output(data, &mut output).await?;
    }

    // All done!
    Ok(())
}
 */
