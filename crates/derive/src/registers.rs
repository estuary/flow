use crate::{DebugJson, StatsAccumulator};

use doc_poc::{self as doc, reduce, AsNode, FailedValidation, Validation, Validator};
use prost::Message;
use proto_flow::flow::derive_api;
use proto_gazette::consumer::Checkpoint;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use url::Url;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("RocksDB error: {0}")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    Rocks(#[from] rocksdb::Error),
    #[error(transparent)]
    #[serde(serialize_with = "crate::serialize_as_display")]
    Json(#[from] serde_json::Error),
    #[error("protobuf error: {0}")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    Proto(#[from] prost::DecodeError),
    #[error("failed to reduce register documents")]
    Reduce(#[from] reduce::Error),
    #[error("document is invalid: {0:#}")]
    FailedValidation(#[from] FailedValidation),
    #[error(transparent)]
    #[serde(serialize_with = "crate::serialize_as_display")]
    SchemaIndex(#[from] json::schema::index::Error),
}

#[derive(Default)]
pub struct RegisterStats(derive_api::stats::RegisterStats);
impl RegisterStats {
    fn inc_created(&mut self) {
        self.0.created += 1;
    }
}

impl StatsAccumulator for RegisterStats {
    type Stats = derive_api::stats::RegisterStats;
    fn drain(&mut self) -> Self::Stats {
        std::mem::replace(&mut self.0, Default::default())
    }
}

pub struct Registers {
    pub stats: RegisterStats,
    // Backing database of all registers.
    rocks_db: rocksdb::DB,
    cache: HashMap<Box<[u8]>, Option<Value>>,
}

impl std::fmt::Debug for Registers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registers")
            .field(
                "cache",
                &self
                    .cache
                    .iter()
                    .map(|(k, v)| (String::from_utf8_lossy(k), DebugJson(v)))
                    .collect::<BTreeMap<_, _>>(),
            )
            .finish()
    }
}

impl Registers {
    /// Build a new Registers instance.
    pub fn new(mut opts: rocksdb::Options, dir: impl AsRef<Path>) -> Result<Registers, Error> {
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let rocks_db = rocksdb::DB::open_cf(
            &opts,
            dir,
            [
                rocksdb::DEFAULT_COLUMN_FAMILY_NAME,
                super::registers::REGISTERS_CF,
            ]
            .iter(),
        )?;

        Ok(Registers {
            rocks_db,
            cache: HashMap::new(),
            stats: RegisterStats::default(),
        })
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
    pub fn read<'a>(&'a self, key: &[u8], initial: &'a Value) -> &Value {
        let entry = self.cache.get(key).expect("key must be loaded before read");

        match entry {
            Some(value) => value,
            None => initial,
        }
    }

    /// Reduce some number of values into the register specified by the given key,
    /// which must have been previously loaded.
    pub fn reduce(
        &mut self,
        key: &[u8],
        schema: &Url,
        initial: &Value,
        deltas: impl IntoIterator<Item = Value>,
        validator: &mut Validator,
    ) -> Result<(), Error> {
        // Obtain a &mut to the pre-loaded Value into which we'll reduce.
        let reg_ref = self
            .cache
            .get_mut(key)
            .expect("key must be loaded before reduce");

        let alloc = doc::HeapNode::new_allocator();
        let dedup = doc::HeapNode::new_deduper(&alloc);

        let mut reg = match reg_ref {
            Some(v) => doc::HeapNode::from_node(v.as_node(), &alloc, &dedup),
            None => {
                // If the register doesn't exist, initialize it now.
                self.stats.inc_created();
                doc::HeapNode::from_node(initial.as_node(), &alloc, &dedup)
            }
        };

        // Apply all register deltas, in order.
        for rhs in deltas.into_iter() {
            // Validate the RHS delta, reduce, and then validate the result.
            let rhs_valid = Validation::validate(validator, schema, &rhs)?.ok()?;
            reg = doc::reduce::reduce(
                doc::LazyNode::Heap(reg),
                doc::LazyNode::Node(&rhs),
                rhs_valid,
                &alloc,
                &dedup,
                true,
            )?;
            Validation::validate(validator, schema, &reg)?.ok()?;
        }

        *reg_ref = Some(serde_json::to_value(reg.as_node()).unwrap());
        Ok(())
    }

    /// Prepare for commit, storing all modified registers with an accompanying Checkpoint.
    /// After prepare() returns, immediate calls to load(), read(), & reduce() are permitted.
    pub fn prepare(&mut self, checkpoint: Checkpoint) -> Result<(), Error> {
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
        self.rocks_db.write(wb)?;

        Ok(())
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

// Checkpoint key is the key encoding under which a marshalled checkpoint is stored.
pub const CHECKPOINT_KEY: &[u8] = b"checkpoint";
pub const REGISTERS_CF: &str = "registers";

#[cfg(test)]
mod test {
    use super::super::test::build_min_max_sum_schema;
    use super::super::ValidatorGuard;
    use super::*;
    use serde_json::{json, Map, Value};

    #[test]
    fn test_lifecycle() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut reg = Registers::new(rocksdb::Options::default(), dir.path()).unwrap();

        let mut guard = ValidatorGuard::new(&build_min_max_sum_schema()).unwrap();
        let initial = json!({});

        assert_eq!(Checkpoint::default(), reg.last_checkpoint().unwrap());

        // Load some registers.
        reg.load(&[b"foo", b"bar", b"baz"]).unwrap();

        // Expect we read the default value.
        assert_eq!(reg.read(b"foo", &initial), &Value::Object(Map::new()));

        // Reduce in a few updates.
        for (key, values) in vec![
            (
                b"foo",
                vec![json!({"min": 3, "max": 3.3}), json!({"min": 4, "max": 4.4})],
            ),
            (b"baz", vec![json!({"min": 1, "max": 1.1})]),
            (b"baz", vec![json!({"min": 2, "max": 2.2})]),
        ] {
            reg.reduce(
                key,
                &guard.schema.curi,
                &initial,
                values,
                &mut guard.validator,
            )
            .unwrap();
        }
        assert_eq!(2, reg.stats.drain().created);
        // Assert that the counter is reset after drain.
        assert_eq!(0, reg.stats.drain().created);

        // Expect registers were updated to reflect reductions.
        assert_eq!(reg.read(b"foo", &initial), &json!({"min": 3, "max": 4.4}));
        assert_eq!(reg.read(b"bar", &initial), &json!({}));
        assert_eq!(reg.read(b"baz", &initial), &json!({"min": 1, "max": 2.2}));

        // Build a Checkpoint fixture, and flush it to the database with modified registers..
        let mut ack_intents = HashMap::new();
        ack_intents.insert("a/journal".to_owned(), b"ack-intent".to_vec());

        let fixture = Checkpoint {
            ack_intents,
            ..Checkpoint::default()
        };
        reg.prepare(fixture.clone()).unwrap();

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
        assert_eq!(reg.read(b"foo", &initial), &json!({}));
        assert_eq!(reg.read(b"baz", &initial), &json!({}));

        // However, we can still restore our persisted checkpoint (different column family).
        assert_eq!(reg.last_checkpoint().unwrap(), fixture);
        // Assert that the created counter is still 0 since we've not added any _new_ keys.
        assert_eq!(0, reg.stats.drain().created);
    }

    #[test]
    fn test_validation() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut reg = Registers::new(rocksdb::Options::default(), dir.path()).unwrap();

        let mut guard = ValidatorGuard::new(&build_min_max_sum_schema()).unwrap();
        let initial = json!({
            "positive": true, // Causes schema to require that reduced sum >= 0.
            "sum": 0,
        });

        reg.load(&[b"key"]).unwrap();

        // Reduce in updates which validate successfully.
        reg.reduce(
            b"key",
            &guard.schema.curi,
            &initial,
            vec![json!({"sum": 1}), json!({"sum": -0.1}), json!({"sum": 1.2})],
            &mut guard.validator,
        )
        .unwrap();

        assert_eq!(
            reg.read(b"key", &initial),
            &json!({"positive": true, "sum": 2.1})
        );

        // Reduce values such that an intermediate reduction doesn't validate,
        let err = reg
            .reduce(
                b"key",
                &guard.schema.curi,
                &initial,
                vec![json!({"sum": 1}), json!({"sum": -4}), json!({"sum": 5})],
                &mut guard.validator,
            )
            .unwrap_err();

        assert!(matches!(err, Error::FailedValidation(_)));
    }
}
