use super::{nodejs, Error, RecordBatch};
use crate::catalog::{self, sql_params};
use crate::doc::{Schema, SchemaIndex, Validator};
use crate::specs::derive::SourceEnvelope;
use estuary_json::de::walk;
use estuary_json::validator::FullContext;
use futures::channel::mpsc;
use std::borrow::Cow;
use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::BTreeMap;
use url::Url;

pub struct TxnCtx {
    source_idx: BTreeMap<String, (Url, Vec<Lambda>)>,
    pub node: nodejs::Service,
    schemas: SchemaIndex<'static>,
}

#[derive(Debug)]
pub enum Lambda {
    NodeJS(i64),
    Sqlite(i64, String /* Prepared statement context? */),
    Remote(i64, Url),
}

impl Lambda {
    fn transform_id(&self) -> i64 {
        match self {
            Lambda::NodeJS(id) => *id,
            Lambda::Sqlite(id, _) => *id,
            Lambda::Remote(id, _) => *id,
        }
    }

    async fn invoke(
        &self,
        ctx: &TxnCtx,
        tx: mpsc::Sender<RecordBatch>,
    ) -> Result<Invocation, Error> {
        Ok(match self {
            Lambda::NodeJS(transform_id) => {
                Invocation::NodeJS(nodejs::Transform::start(&ctx.node, *transform_id, tx).await?)
            }
            _ => panic!("invocations other than NodeJS not implemented"),
        })
    }
}

enum Invocation {
    NodeJS(nodejs::Transform),
}

impl Invocation {
    async fn process(&mut self, batch: RecordBatch) -> Result<(), Error> {
        match self {
            Invocation::NodeJS(transform) => {
                transform
                    .sender
                    .as_mut()
                    .unwrap()
                    .send_data(batch.to_bytes())
                    .await?;
            }
        }
        Ok(())
    }

    async fn drain(self) -> Result<(), Error> {
        match self {
            Invocation::NodeJS(mut transform) => {
                transform.sender = None;
                transform.handle.await.unwrap()?;
            }
        }
        Ok(())
    }
}

pub struct Invoker {
    tx: mpsc::Sender<RecordBatch>,
    entries: BTreeMap<i64, Invocation>,
}

impl Invoker {
    pub fn new(tx: mpsc::Sender<RecordBatch>) -> Invoker {
        Invoker {
            tx,
            entries: BTreeMap::new(),
        }
    }

    pub async fn invoke(
        &mut self,
        ctx: &TxnCtx,
        lambdas: &[Lambda],
        batch: RecordBatch,
    ) -> Result<(), Error> {
        for lambda in lambdas {
            let entry = self.entries.entry(lambda.transform_id());
            let invocation = match entry {
                BTreeEntry::Occupied(occupied) => occupied.into_mut(),
                BTreeEntry::Vacant(vacant) => {
                    vacant.insert(lambda.invoke(ctx, self.tx.clone()).await?)
                }
            };
            invocation.process(batch.clone()).await?;
        }
        Ok(())
    }

    pub async fn drain(self) -> Result<(), Error> {
        // Start concurrent completion of each invocation.
        let futs = self
            .entries
            .into_iter()
            .map(|(_, invocation)| invocation.drain())
            .collect::<Vec<_>>();

        // Wait on each future.
        for fut in futs {
            fut.await?;
        }
        Ok(())
    }
}

impl TxnCtx {
    pub fn new(
        db: &catalog::DB,
        derivation: i64,
        node: nodejs::Service,
        schemas: &'static Vec<Schema>,
    ) -> Result<TxnCtx, catalog::Error> {
        let mut schema_index = SchemaIndex::<'static>::new();
        for schema in schemas.iter() {
            schema_index.add(schema)?;
        }
        schema_index.verify_references()?;

        Ok(TxnCtx {
            source_idx: index_source_schema_and_transforms(db, derivation)?,
            node,
            schemas: schema_index,
        })
    }
}

pub async fn process_source_batch(
    ctx: &TxnCtx,
    invoker: &mut Invoker,
    batch: RecordBatch,
) -> Result<(), Error> {
    let batch = batch.to_bytes();

    // Split records on newline boundaries.
    // TODO(johnny): Convince serde-json to expose Deserializer::byte_offset()?
    // Then it's not necessary to pre-scan for newlines.
    let splits = batch
        .iter()
        .enumerate()
        .filter(|(_, &b)| b == b'\n')
        .map(|(ind, _)| ind + 1);

    let mut first_source = Cow::<str>::Borrowed("");
    let mut first_pivot = 0;
    let mut last_pivot = 0;

    for next_pivot in splits {
        let record = &batch[last_pivot..next_pivot];

        let mut de = serde_json::Deserializer::from_slice(record);
        let env: SourceEnvelope = serde::de::Deserialize::deserialize(&mut de)?;

        if first_source != env.collection {
            if !first_source.is_empty() {
                // Flush the contiguous chunk of records having the prior source collection
                // through their mapped transforms.
                let (_, lambdas) = ctx.source_idx.get(first_source.as_ref()).unwrap();
                let sub_batch = RecordBatch::new(batch.slice(first_pivot..last_pivot));
                invoker.invoke(ctx, lambdas, sub_batch).await?;
            }
            first_source = env.collection.clone();
            first_pivot = last_pivot;
        }

        let (schema_uri, _) = ctx
            .source_idx
            .get(env.collection.as_ref())
            .ok_or_else(|| Error::UnknownSourceCollection(env.collection.to_string()))?;

        // Validate schema.
        let mut validator = Validator::<FullContext>::new(&ctx.schemas, schema_uri)?;
        walk(&mut de, &mut validator)?;

        if validator.invalid() {
            let errors = validator
                .outcomes()
                .iter()
                .filter(|(o, _)| o.is_error())
                .collect::<Vec<_>>();
            log::error!("source doc is invalid for {:?}: {:?}", env, errors);
            Err(Error::SourceValidationFailed)?;
        }

        last_pivot = next_pivot;
    }

    if first_pivot != last_pivot {
        // Flush the remainder.
        let (_, lambdas) = ctx.source_idx.get(first_source.as_ref()).unwrap();
        let sub_batch = RecordBatch::new(batch.slice(first_pivot..last_pivot));
        invoker.invoke(ctx, lambdas, sub_batch).await?;
    }

    Ok(())
}

fn index_source_schema_and_transforms(
    db: &catalog::DB,
    derivation: i64,
) -> Result<BTreeMap<String, (Url, Vec<Lambda>)>, catalog::Error> {
    let mut out = BTreeMap::new();

    let mut stmt = db.prepare(
        "SELECT
            transform_id,            -- 0
            source_name,             -- 1
            lambda_runtime,          -- 2
            lambda_inline,           -- 3 (needed for 'remote')
            lambda_resource_content, -- 4 (needed for 'sqliteFile')
            source_schema_uri        -- 5
        FROM transform_details
            WHERE derivation_id = ?;
    ",
    )?;
    let mut rows = stmt.query(sql_params![derivation])?;

    while let Some(r) = rows.next()? {
        let (tid, rt): (i64, String) = (r.get(0)?, r.get(2)?);

        let transform = match rt.as_str() {
            "nodeJS" => Lambda::NodeJS(tid),
            "remote" => Lambda::Remote(tid, r.get(3)?),
            "sqlite" => Lambda::Sqlite(tid, r.get(3)?),
            "sqliteFile" => Lambda::Sqlite(tid, r.get(4)?),
            rt @ _ => panic!("transform {} has invalid runtime {:?}", tid, rt),
        };
        out.entry(r.get(1)?)
            .or_insert((r.get(5)?, Vec::new()))
            .1
            .push(transform);
    }

    log::info!("indexed sources: {:?}", out);

    Ok(out)
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
