use anyhow::Context;
use prost::Message;
use proto_flow::{flow, runtime::RocksDbDescriptor};
use proto_gazette::consumer;
use std::sync::Arc;
use tokio::runtime::Handle;

/// RocksDB database used for task state.
pub struct RocksDB {
    db: Arc<rocksdb::DB>,
    _tmp: Option<tempfile::TempDir>,
}

impl RocksDB {
    /// Open a RocksDB from an optional descriptor.
    pub async fn open(desc: Option<RocksDbDescriptor>) -> anyhow::Result<Self> {
        let (opts, path, _tmp) = unpack_descriptor(desc)?;

        let db = Handle::current()
            .spawn_blocking(move || Self::open_blocking(opts, path))
            .await
            .unwrap()?;

        Ok(Self {
            db: Arc::new(db),
            _tmp,
        })
    }

    fn open_blocking(
        mut opts: rocksdb::Options,
        path: std::path::PathBuf,
    ) -> anyhow::Result<rocksdb::DB> {
        // RocksDB requires that all column families be explicitly passed in on open
        // or it will fail. We don't currently use column families, but have in the
        // past and may in the future. Flexibly open the DB by explicitly listing,
        // opening, and then ignoring column families we don't care about.
        let column_families = match rocksdb::DB::list_cf(&opts, &path) {
            Ok(cf) => cf,
            // Listing column families will fail if the DB doesn't exist.
            // Assume as such, as we'll otherwise fail when we attempt to open.
            Err(_) => vec![rocksdb::DEFAULT_COLUMN_FAMILY_NAME.to_string()],
        };
        tracing::debug!(column_families=?ops::DebugJson(&column_families), "listed existing rocksdb column families");

        let mut cf_descriptors = Vec::with_capacity(column_families.len());
        for name in column_families {
            let mut cf_opts = rocksdb::Options::default();

            if name == rocksdb::DEFAULT_COLUMN_FAMILY_NAME {
                let state_schema = doc::reduce::merge_patch_schema();

                set_json_schema_merge_operator(
                    &mut cf_opts,
                    &task_state_default_json_schema(&state_schema).to_string(),
                )?;
            }
            cf_descriptors.push(rocksdb::ColumnFamilyDescriptor::new(name, cf_opts));
        }

        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = rocksdb::DB::open_cf_descriptors(&opts, &path, cf_descriptors)
            .context("failed to open RocksDB")?;

        Ok(db)
    }

    /// Perform an async get_opt using a blocking background thread.
    pub async fn get_opt(
        &self,
        key: impl AsRef<[u8]> + Send + 'static,
        ro: rocksdb::ReadOptions,
    ) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        let db = self.db.clone();
        Handle::current()
            .spawn_blocking(move || db.get_opt(key, &ro))
            .await
            .unwrap()
    }

    /// Perform an async multi_get_opt using a blocking background thread.
    pub async fn multi_get_opt<K, I>(
        &self,
        keys: I,
        ro: rocksdb::ReadOptions,
    ) -> Vec<Result<Option<Vec<u8>>, rocksdb::Error>>
    where
        K: AsRef<[u8]> + Send + 'static,
        I: IntoIterator<Item = K> + Send + 'static,
    {
        let db = self.db.clone();

        Handle::current()
            .spawn_blocking(move || db.multi_get_opt(keys, &ro))
            .await
            .unwrap()
    }

    /// Perform an async write_opt using a blocking background thread.
    pub async fn write_opt(
        &self,
        wb: rocksdb::WriteBatch,
        wo: rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        let db = self.db.clone();
        Handle::current()
            .spawn_blocking(move || db.write_opt(wb, &wo))
            .await
            .unwrap()
    }

    /// Load a persisted runtime Checkpoint.
    pub async fn load_checkpoint(&self) -> anyhow::Result<consumer::Checkpoint> {
        match self
            .get_opt(Self::CHECKPOINT_KEY, rocksdb::ReadOptions::default())
            .await
            .context("failed to load runtime checkpoint")?
        {
            Some(v) => Ok(consumer::Checkpoint::decode(v.as_ref())
                .context("failed to decode consumer checkpoint")?),
            None => Ok(consumer::Checkpoint::default()),
        }
    }

    /// Load the persisted, last-applied task specification.
    pub async fn load_last_applied<S>(&self) -> anyhow::Result<Option<S>>
    where
        S: prost::Message + serde::Serialize + Default,
    {
        match self
            .get_opt(Self::LAST_APPLIED, rocksdb::ReadOptions::default())
            .await
            .context("failed to load the last-applied task specification")?
        {
            Some(v) => {
                let spec = S::decode(v.as_ref())
                    .context("failed to decode the last-applied task specification")?;
                tracing::debug!(spec=?ops::DebugJson(&spec), "loaded the last-applied task specification");
                Ok(Some(spec))
            }
            None => {
                tracing::debug!("did not find a last-applied task specification");
                Ok(None)
            }
        }
    }

    /// Load a persisted connector state.
    /// If it doesn't exist, it's durably initialized with value `initial`.
    pub async fn load_connector_state(
        &self,
        initial: models::RawValue,
    ) -> anyhow::Result<models::RawValue> {
        if let Some(state) = self
            .get_opt(Self::CONNECTOR_STATE_KEY, rocksdb::ReadOptions::default())
            .await
            .context("failed to load connector state")?
        {
            let state = String::from_utf8(state).context("decoding connector state as UTF-8")?;
            let state = models::RawValue::from_string(state).context("decoding state as JSON")?;

            tracing::debug!(state=?ops::DebugJson(&state), "loaded a persisted connector state");
            return Ok(state);
        };

        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(true);

        let mut wb = rocksdb::WriteBatch::default();
        wb.put(Self::CONNECTOR_STATE_KEY, initial.get());

        self.write_opt(wb, wo)
            .await
            .context("put-ing initial connector state")?;

        tracing::debug!(state=?ops::DebugJson(&initial), "initialized a new persisted connector state");

        Ok(initial)
    }

    // Key encoding under which the last-applied specification is stored.
    pub const LAST_APPLIED: &'static str = "last-applied";
    // Key encoding under which a marshalled checkpoint is stored.
    pub const CHECKPOINT_KEY: &'static str = "checkpoint";
    // Key encoding under which a connector state is stored.
    pub const CONNECTOR_STATE_KEY: &'static str = "connector-state";
}

// Enqueues a MERGE or PUT to the WriteBatch for this `state` update.
pub fn queue_connector_state_update(
    state: &flow::ConnectorState,
    wb: &mut rocksdb::WriteBatch,
) -> anyhow::Result<()> {
    let flow::ConnectorState {
        merge_patch,
        updated_json,
    } = state;

    let updated: models::RawValue =
        serde_json::from_slice(updated_json).context("failed to decode connector state as JSON")?;

    if *merge_patch {
        wb.merge(RocksDB::CONNECTOR_STATE_KEY, updated.get());
    } else {
        wb.put(RocksDB::CONNECTOR_STATE_KEY, updated.get());
    }
    tracing::debug!(updated=?ops::DebugJson(updated), %merge_patch, "applied an updated connector state");

    Ok(())
}

// Unpack a RocksDbDescriptor into its rocksdb::Options and path.
// If the descriptor does not include an explicit path, a TempDir to use is
// created and returned.
fn unpack_descriptor(
    desc: Option<RocksDbDescriptor>,
) -> anyhow::Result<(
    rocksdb::Options,
    std::path::PathBuf,
    Option<tempfile::TempDir>,
)> {
    Ok(match desc {
        Some(RocksDbDescriptor {
            rocksdb_path,
            rocksdb_env_memptr,
        }) => {
            tracing::debug!(
                ?rocksdb_path,
                ?rocksdb_env_memptr,
                "opening hooked RocksDB database"
            );
            let mut opts = rocksdb::Options::default();

            if rocksdb_env_memptr != 0 {
                // Re-hydrate the provided memory address into rocksdb::Env wrapping
                // an owned *mut librocksdb_sys::rocksdb_env_t.
                let env = unsafe {
                    rocksdb::Env::from_raw(rocksdb_env_memptr as *mut librocksdb_sys::rocksdb_env_t)
                };
                opts.set_env(&env);
            }
            (opts, std::path::PathBuf::from(rocksdb_path), None)
        }
        None => {
            let dir = tempfile::TempDir::new().context("failed to create RocksDB tempdir")?;
            let opts = rocksdb::Options::default();

            tracing::debug!(
                rocksdb_path = ?dir.path(),
                "opening temporary RocksDB database"
            );

            (opts, dir.path().to_owned(), Some(dir))
        }
    })
}

// RocksDB merge operator schema which uses `state_schema` for keys matching "connector-state".
fn task_state_default_json_schema(state_schema: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "oneOf": [
            {
                "items": [
                    {"const": RocksDB::CONNECTOR_STATE_KEY},
                    state_schema,
                ]
            }
        ],
        "reduce": {"strategy": "merge"}
    })
}

// Set a reduction merge operator using the given `schema`.
fn set_json_schema_merge_operator(opts: &mut rocksdb::Options, schema: &str) -> anyhow::Result<()> {
    // Check that we can build a validator for `schema`.
    let bundle = doc::validation::build_bundle(schema.as_bytes())?;
    let _validator = doc::Validator::new(bundle)?;

    let schema_1 = schema.to_owned();
    let schema_2 = schema.to_owned();

    let full_merge_fn = move |key: &[u8],
                              initial: Option<&[u8]>,
                              operands: &rocksdb::merge_operator::MergeOperands|
          -> Option<Vec<u8>> {
        match do_merge(true, initial, key, operands, &schema_1) {
            Ok(ok) => Some(ok),
            Err(err) => {
                tracing::error!(%err, "error within RocksDB full-merge operator");
                if cfg!(debug_assertions) {
                    eprintln!("(debug) full-merge error: {err:?}");
                }
                None
            }
        }
    };
    let partial_merge_fn = move |key: &[u8],
                                 initial: Option<&[u8]>,
                                 operands: &rocksdb::merge_operator::MergeOperands|
          -> Option<Vec<u8>> {
        match do_merge(false, initial, key, operands, &schema_2) {
            Ok(ok) => Some(ok),
            Err(err) => {
                tracing::error!(%err, "error within RocksDB partial-merge operator");
                if cfg!(debug_assertions) {
                    eprintln!("(debug) partial-merge error: {err:?}");
                }
                None
            }
        }
    };
    opts.set_merge_operator("json-schema", full_merge_fn, partial_merge_fn);

    Ok(())
}

fn do_merge(
    full: bool,
    initial: Option<&[u8]>,
    key: &[u8],
    operands: &rocksdb::merge_operator::MergeOperands,
    schema: &str,
) -> anyhow::Result<Vec<u8>> {
    let bundle = doc::validation::build_bundle(schema.as_bytes()).unwrap();
    let validator = doc::Validator::new(bundle).unwrap();
    let spec = doc::combine::Spec::with_one_binding(full, [], "connector state", Vec::new(), None, validator);
    let memtable = doc::combine::MemTable::new(spec);

    let key = String::from_utf8_lossy(key);
    let key = doc::BumpStr::from_str(&key, memtable.alloc());

    let add_merge_op = |op: &[u8]| -> anyhow::Result<()> {
        // Split on newline, and parse ordered JSON documents that are added to `memtable`.
        for op_bytes in op.split(|c| *c == b'\n') {
            let mut de = serde_json::Deserializer::from_slice(op_bytes);
            let op = doc::HeapNode::from_serde(&mut de, memtable.alloc()).with_context(|| {
                format!(
                    "couldn't parse document as JSON: {}",
                    String::from_utf8_lossy(op_bytes)
                )
            })?;

            let doc = doc::HeapNode::new_array(
                memtable.alloc(),
                [doc::HeapNode::String(key), op].into_iter(),
            );
            memtable.add(0, doc, false)?;
        }
        Ok(())
    };

    if let Some(initial) = initial {
        add_merge_op(initial)?;
    }
    for op in operands {
        add_merge_op(op)?;
    }

    // `ptr` plucks out the second element of the reduced array.
    let ptr = doc::Pointer(vec![doc::ptr::Token::Index(1)]);

    let mut out = Vec::new();
    for (index, drained) in memtable.try_into_drainer()?.enumerate() {
        let doc::combine::DrainedDoc { meta: _, root } = drained?;
        let doc::OwnedNode::Heap(root) = root else {
            unreachable!()
        };
        let node = ptr.query(root.get()).unwrap();

        if index != 0 {
            out.push(b'\n');
        }
        serde_json::to_writer(&mut out, &doc::SerPolicy::noop().on(node)).unwrap();
    }

    Ok(out)
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn connector_state_merge() {
        let mut wb = rocksdb::WriteBatch::default();
        let db = RocksDB::open(None).await.unwrap();

        for doc in [
            r#"{"a":"b","n":null}"#,
            r#"{"a":"c","nn":null}"#,
            r#"{"d":"e","ans":42}"#,
        ] {
            wb.merge(RocksDB::CONNECTOR_STATE_KEY, doc);
        }
        db.write_opt(wb, Default::default()).await.unwrap();

        let state = db.load_connector_state(Default::default()).await.unwrap();
        assert_eq!(state.get(), r#"{"a":"c","ans":42,"d":"e","n":null}"#);
    }
}
