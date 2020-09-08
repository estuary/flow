use super::{nodejs, Error, RecordBatch};
use crate::catalog::{self, sql_params};
use crate::doc::{Pointer, SchemaIndex, Validator};
use crate::specs::derive::SourceEnvelope;
use estuary_json::de::walk;
use estuary_json::validator::FullContext;
use futures::channel::mpsc;
use serde_json;
use std::borrow::Cow;
use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::BTreeMap;
use url::Url;

pub struct Context {
    /// sources is a map from a collection name to it's schema, and
    sources: BTreeMap<String, (Url, Vec<Lambda>)>,
    pub node: nodejs::Service,
    pub schema_index: SchemaIndex<'static>,

    pub derived_schema: Url,
    pub derived_key: Vec<Pointer>,
    pub derived_parts: BTreeMap<String, Pointer>,
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
        ctx: &Context,
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
        ctx: &Context,
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
        // Start draining each invocation, then wait for all to finish.
        let futures = self
            .entries
            .into_iter()
            .map(|(_, invocation)| invocation.drain())
            .collect::<Vec<_>>();

        for f in futures {
            f.await?;
        }
        Ok(())
    }
}

impl Context {
    pub fn new(
        db: &catalog::DB,
        derivation: i64,
        node: nodejs::Service,
        schema_index: SchemaIndex<'static>,
    ) -> Result<Context, catalog::Error> {
        let (derived_schema, derived_key): (Url, serde_json::Value) = db
            .prepare("SELECT schema_uri, key_json FROM collections WHERE collection_id = ?")?
            .query_row(sql_params![derivation], |r| Ok((r.get(0)?, r.get(1)?)))?;

        // TODO(johnny): Have Pointer implement serde::Deserialize? Could clean this up...
        let derived_key: Vec<String> = serde_json::from_value(derived_key)?;
        let derived_key = derived_key.iter().map(|s| s.into()).collect::<Vec<_>>();

        let mut derived_parts = BTreeMap::<String, Pointer>::new();
        let mut stmt = db.prepare("SELECT field, location_ptr FROM projections WHERE collection_id = ? AND is_logical_partition")?;
        let mut rows = stmt.query(sql_params![derivation])?;

        while let Some(r) = rows.next()? {
            let ptr: String = r.get(1)?;
            derived_parts.insert(r.get(0)?, Pointer::from(&ptr));
        }

        Ok(Context {
            sources: index_source_schema_and_transforms(db, derivation)?,
            node,
            schema_index,
            derived_schema,
            derived_key,
            derived_parts,
        })
    }
}

pub async fn process_source_batch(
    ctx: &Context,
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
                let (_, lambdas) = ctx.sources.get(first_source.as_ref()).unwrap();
                let sub_batch = RecordBatch::new(batch.slice(first_pivot..last_pivot));
                invoker.invoke(ctx, lambdas, sub_batch).await?;
            }
            first_source = env.collection.clone();
            first_pivot = last_pivot;
        }

        let (schema_uri, _) = ctx
            .sources
            .get(env.collection.as_ref())
            .ok_or_else(|| Error::UnknownSourceCollection(env.collection.to_string()))?;

        // Validate schema.
        let mut validator = Validator::<FullContext>::new(&ctx.schema_index, schema_uri)?;
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
        let (_, lambdas) = ctx.sources.get(first_source.as_ref()).unwrap();
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
