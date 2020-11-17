use doc::{reduce, SchemaIndex, Validator};
use futures::channel::oneshot;
use json::validator::FullContext;
use prost::Message;
use protocol::consumer::Checkpoint;
use serde_json::Value;
use std::collections::HashMap;
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RocksDB error: {0}")]
    Lambda(#[from] rocksdb::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("protobuf error: {0}")]
    Proto(#[from] prost::DecodeError),
}

pub struct Registers {
    // Backing database of all registers.
    rocks_db: rocksdb::DB,
    validator: Validator<'static, FullContext>,
    schema: Url,
    initial: serde_json::Value,
    cache: HashMap<Box<[u8]>, Option<Value>>,
}

impl std::fmt::Debug for Registers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Registers")
    }
}

impl Registers {
    /// Build a new Registers instance.
    pub fn new(
        rocks_db: rocksdb::DB,
        schema_index: &'static SchemaIndex,
        schema: &Url,
        initial: serde_json::Value,
    ) -> Registers {
        Registers {
            rocks_db,
            schema: schema.clone(),
            validator: Validator::new(schema_index),
            initial,
            cache: HashMap::new(),
        }
    }

    /// Retrieves the last Checkpoint committed into the Registers database,
    /// or a Checkpoint::default() if there has not yet been a committed Checkpoint.
    pub fn last_checkpoint(&self) -> Result<Checkpoint, Error> {
        match self.rocks_db.get_pinned(CHECKPOINT_KEY)? {
            Some(v) => Ok(Checkpoint::decode(v.as_ref())?),
            None => Ok(Checkpoint::default()),
        }
    }

    /// Load the specified register keys into the in-memory cache, from which it may be directly read.
    pub fn load<I>(&mut self, keys: I) -> Result<(), Error>
    where
        I: IntoIterator,
        I::Item: AsRef<[u8]>,
    {
        let cf = self.rocks_db.cf_handle(REGISTERS_CF).unwrap();

        // TODO(johnny): RocksDB has a "multiget" operation which can be substantially
        // faster than sequential "get"'s, that we ought to be using here.
        // It's not currently exposed in the rocksdb crate however.
        for key in keys.into_iter() {
            let key = key.as_ref();

            if self.cache.get(key).is_some() {
                continue;
            }
            let value: Option<serde_json::Value> = match self.rocks_db.get_pinned_cf(cf, key)? {
                Some(pin) => Some(serde_json::from_slice(&pin)?),
                None => None,
            };
            self.cache.insert(key.into(), value);
        }

        Ok(()) // No-op.
    }

    /// Read the current value of a register, which must have been previously loaded.
    pub fn read(&self, key: &[u8]) -> &Value {
        let entry = self.cache.get(key).expect("key must be loaded before read");

        match entry {
            Some(value) => value,
            None => &self.initial,
        }
    }

    /// Reduce some number of values into the register specified by the given key,
    /// which must have been previously loaded.
    pub fn reduce(
        &mut self,
        key: &[u8],
        deltas: impl IntoIterator<Item = Value>,
    ) -> Result<(), reduce::Error> {
        // Obtain a &mut to the pre-loaded Value into which we'll reduce.
        let lhs = self
            .cache
            .get_mut(key)
            .expect("key must be loaded before reduce");

        // If the register doesn't exist, initialize it now.
        if !matches!(lhs, Some(_)) {
            *lhs = Some(self.initial.clone());
        }

        // Apply all register deltas, in order.
        for rhs in deltas.into_iter() {
            *lhs = Some(reduce::reduce(
                &mut self.validator,
                &self.schema,
                lhs.take(),
                rhs,
                true,
            )?);
        }
        Ok(())
    }

    /// Prepare for commit, storing all modified registers with an accompanying Checkpoint.
    /// Returns a one-shot Sender which is used to signal when to commit.
    /// After prepare() returns immediate calls to load(), read(), & reduce() are permitted,
    /// but another call to prepare() may not occur until the Sender is signaled.
    pub fn prepare(&mut self, checkpoint: Checkpoint) -> Result<oneshot::Sender<()>, Error> {
        let cf = self.rocks_db.cf_handle(REGISTERS_CF).unwrap();
        let mut wb = rocksdb::WriteBatch::default();

        // Add |checkpoint| to WriteBatch.
        let mut buffer = Vec::<u8>::with_capacity(checkpoint.encoded_len());
        checkpoint.encode(&mut buffer).unwrap();
        wb.put(CHECKPOINT_KEY, &buffer);

        // Add updated register values to WriteBatch.
        for (key, value) in self.cache.drain() {
            if let Some(value) = value {
                buffer.clear();
                serde_json::to_writer(&mut buffer, &value)?;
                wb.put_cf(cf, key, &buffer);
            }
        }

        let (tx_commit, rx_commit) = oneshot::channel();

        // TODO(johnny): We'll want to put a Recorder barrier in place here before
        // writing the WriteBatch. The barrier should be gated on |rx_commit| being
        // signalled.
        //
        // For now, we have a no-op reader of |rx_commit| which does nothing.
        tokio::spawn(rx_commit);

        self.rocks_db.write(wb)?;
        Ok(tx_commit)
    }

    /// Clear all registers. May only be called in between commits.
    /// This is a testing-centric function, used to clear state between test cases.
    pub fn clear(&mut self) -> Result<(), Error> {
        assert!(self.cache.is_empty());

        let cf = self.rocks_db.cf_handle(REGISTERS_CF).unwrap();
        self.rocks_db
            .delete_range_cf(cf, &[0x00, 0x00, 0x00, 0x00], &[0xff, 0xff, 0xff, 0xff])
            .map_err(Into::into)
    }
}

pub const CHECKPOINT_KEY: &[u8] = b"checkpoint";
pub const REGISTERS_CF: &str = "registers";

#[cfg(test)]
mod test {
    use super::{
        super::test::{build_min_max_schema, build_test_rocks},
        *,
    };
    use serde_json::{json, Map, Value};

    #[tokio::test]
    async fn test_lifecycle() {
        let (_db_dir, db) = build_test_rocks();
        let (schema_index, schema) = build_min_max_schema();
        let mut reg = Registers::new(db, schema_index, &schema, Value::Object(Map::new()));

        assert_eq!(Checkpoint::default(), reg.last_checkpoint().unwrap());

        // Load some registers.
        reg.load(&[b"foo", b"bar", b"baz"]).unwrap();

        // Expect we read the default value.
        assert_eq!(reg.read(b"foo"), &Value::Object(Map::new()));

        // Reduce in a few updates.
        reg.reduce(
            b"foo",
            vec![json!({"min": 3, "max": 3.3}), json!({"min": 4, "max": 4.4})],
        )
        .unwrap();

        reg.reduce(b"baz", vec![json!({"min": 1, "max": 1.1})])
            .unwrap();
        reg.reduce(b"baz", vec![json!({"min": 2, "max": 2.2})])
            .unwrap();

        // Expect registers were updated to reflect reductions.
        assert_eq!(reg.read(b"foo"), &json!({"min": 3, "max": 4.4}));
        assert_eq!(reg.read(b"bar"), &json!({}));
        assert_eq!(reg.read(b"baz"), &json!({"min": 1, "max": 2.2}));

        // Build a Checkpoint fixture, and flush it to the database with modified registers..
        let mut ack_intents = HashMap::new();
        ack_intents.insert("a/journal".to_owned(), b"ack-intent".to_vec());

        let fixture = Checkpoint {
            ack_intents,
            ..Checkpoint::default()
        };
        let tx_commit = reg.prepare(fixture.clone()).unwrap();
        // Expect we can send a "commit" signal without error,
        // though it doesn't do anything (yet).
        tx_commit.send(()).unwrap();

        // Expect the local cache was drained, and values flushed to the DB.
        assert!(reg.cache.is_empty());

        // Expect we see "baz" & "foo" in the DB, but no values were reduced into "bar".
        let it = reg
            .rocks_db
            .iterator_cf(
                reg.rocks_db.cf_handle(REGISTERS_CF).unwrap(),
                rocksdb::IteratorMode::Start,
            )
            .map(|(k, v)| {
                (
                    std::str::from_utf8(&k).unwrap().to_owned(),
                    serde_json::from_slice::<Value>(&v).unwrap(),
                )
            });

        itertools::assert_equal(
            it,
            vec![
                ("baz".to_owned(), json!({"min": 1, "max": 2.2})),
                ("foo".to_owned(), json!({"min": 3, "max": 4.4})),
            ]
            .into_iter(),
        );

        // Clear registers, and expect we no longer see previous persisted versions.
        reg.clear().unwrap();
        reg.load(&[b"foo", b"baz"]).unwrap();
        assert_eq!(reg.read(b"foo"), &json!({}));
        assert_eq!(reg.read(b"baz"), &json!({}));

        // However, we can still restore our persisted checkpoint (different column family).
        assert_eq!(reg.last_checkpoint().unwrap(), fixture);
    }
}
