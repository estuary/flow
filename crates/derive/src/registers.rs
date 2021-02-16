use crate::DebugJson;

use doc::{reduce, FailedValidation, SchemaIndex, Validation, Validator};
use prost::Message;
use protocol::consumer::Checkpoint;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RocksDB error: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("protobuf error: {0}")]
    Proto(#[from] prost::DecodeError),
    #[error("failed to reduce register documents")]
    Reduce(#[from] reduce::Error),
    #[error("document is invalid: {0:#}")]
    FailedValidation(#[from] FailedValidation),
    #[error(transparent)]
    SchemaIndex(#[from] json::schema::index::Error),
}

pub struct Registers {
    // Backing database of all registers.
    rocks_db: rocksdb::DB,
    validator: Validator<'static>,
    schema: Url,
    initial: serde_json::Value,
    cache: HashMap<Box<[u8]>, Option<Value>>,
}

impl std::fmt::Debug for Registers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registers")
            .field("schema", &self.schema.as_str())
            .field("initial", &DebugJson(&self.initial))
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
        rollback_on_conflict: bool,
    ) -> Result<bool, Error> {
        // Obtain a &mut to the pre-loaded Value into which we'll reduce.
        let lhs = self
            .cache
            .get_mut(key)
            .expect("key must be loaded before reduce");

        let rollback = if rollback_on_conflict {
            Some(lhs.clone())
        } else {
            None
        };

        // If the register doesn't exist, initialize it now.
        if !matches!(lhs, Some(_)) {
            *lhs = Some(self.initial.clone());
        }

        // Apply all register deltas, in order.
        for rhs in deltas.into_iter() {
            // Validate the RHS delta, reduce, and then validate the result.
            let rhs = Validation::validate(&mut self.validator, &self.schema, rhs)?.ok()?;
            let reduced = reduce::reduce(lhs.take(), rhs, true)?;

            let reduced = Validation::validate(&mut self.validator, &self.schema, reduced)?;
            if !reduced.validator.invalid() {
                *lhs = Some(reduced.document);
                continue;
            }

            // Reduction is invalid.
            match rollback {
                Some(rollback) => {
                    *lhs = rollback;
                    return Ok(false);
                }
                None => {
                    reduced.ok()?;
                    unreachable!("reduced.ok() must error");
                }
            }
        }
        Ok(true)
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
    use super::{super::test::build_min_max_sum_schema, *};
    use serde_json::{json, Map, Value};

    #[test]
    fn test_lifecycle() {
        let (_db_dir, db) = build_test_rocks();
        let (schema_index, schema) = build_min_max_sum_schema();
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
            false,
        )
        .unwrap();

        reg.reduce(b"baz", vec![json!({"min": 1, "max": 1.1})], false)
            .unwrap();
        reg.reduce(b"baz", vec![json!({"min": 2, "max": 2.2})], false)
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
        assert_eq!(reg.read(b"foo"), &json!({}));
        assert_eq!(reg.read(b"baz"), &json!({}));

        // However, we can still restore our persisted checkpoint (different column family).
        assert_eq!(reg.last_checkpoint().unwrap(), fixture);
    }

    #[test]
    fn test_rollback() {
        let (_db_dir, db) = build_test_rocks();
        let (schema_index, schema) = build_min_max_sum_schema();
        let schema_initial = json!({
            "positive": true, // Causes schema to require that reduced sum >= 0.
            "sum": 0,
        });
        let mut reg = Registers::new(db, schema_index, &schema, schema_initial);

        reg.load(&[b"key"]).unwrap();

        // Reduce in updates which validate successfully.
        let applied = reg
            .reduce(
                b"key",
                vec![json!({"sum": 1}), json!({"sum": -0.1}), json!({"sum": 1.2})],
                false,
            )
            .unwrap();
        assert!(applied);

        assert_eq!(reg.read(b"key"), &json!({"positive": true, "sum": 2.1}));

        // Reduce values such that an intermediate reduction doesn't validate,
        // with rollback enabled.
        let applied = reg
            .reduce(
                b"key",
                vec![json!({"sum": 1}), json!({"sum": -4}), json!({"sum": 5})],
                true,
            )
            .unwrap();

        // Expect the register wasn't modified.
        assert!(!applied);
        assert_eq!(reg.read(b"key"), &json!({"positive": true, "sum": 2.1}));

        // Try again. This time, apply without rollback.
        let err = reg
            .reduce(
                b"key",
                vec![json!({"sum": 1}), json!({"sum": -4}), json!({"sum": 5})],
                false,
            )
            .unwrap_err();

        assert!(matches!(err, Error::FailedValidation(_)));

        // Expect register was replaced to an initial state
        // (although the caller has likely bailed out by now).
        assert_eq!(reg.read(b"key"), &json!({"positive": true, "sum": 0}));
    }

    // Builds an empty RocksDB in a temporary directory,
    // initialized with the "registers" column family.
    pub fn build_test_rocks() -> (tempfile::TempDir, rocksdb::DB) {
        let dir = tempfile::TempDir::new().unwrap();

        let mut rocks_opts = rocksdb::Options::default();
        rocks_opts.create_if_missing(true);
        rocks_opts.set_error_if_exists(true);
        rocks_opts.create_missing_column_families(true);

        let db = rocksdb::DB::open_cf(
            &rocks_opts,
            dir.path(),
            [rocksdb::DEFAULT_COLUMN_FAMILY_NAME, REGISTERS_CF].iter(),
        )
        .unwrap();

        (dir, db)
    }
}
