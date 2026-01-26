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
    // Holds outputs of prior iterations, which are now inputs of the next one.
    let mut input_storage: Vec<Vec<u8>>;

    // Collect all input documents (initial + operands, each newline-delimited).
    let mut inputs: Vec<&[u8]> = Vec::new();
    if let Some(initial) = initial {
        for doc_bytes in initial.split(|c| *c == b'\n') {
            inputs.push(doc_bytes);
        }
    }
    for op in operands {
        for doc_bytes in op.split(|c| *c == b'\n') {
            inputs.push(doc_bytes);
        }
    }

    let mut iteration = 0usize;
    let mut mb_threshold = 32; // Start with 32MB threshold, and double as needed.
    let mut prev_batches = usize::MAX;

    loop {
        let mut offset = 0;
        let mut outputs: Vec<Vec<u8>> = Vec::new();

        // Process all inputs in batches constrained to `mb_threshold`.
        while offset != inputs.len() {
            let (output, consumed) =
                do_merge_bounded(full, &key, &inputs[offset..], schema, mb_threshold)?;

            outputs.push(output);
            offset += consumed;
        }

        tracing::debug!(
            iteration,
            inputs = inputs.len(),
            batches = outputs.len(),
            prev_batches,
            mb_threshold,
            "do_merge iteration complete"
        );

        // Stop when we have a single batch output.
        if outputs.len() <= 1 {
            return Ok(outputs.into_iter().next().unwrap_or_default());
        }

        // Batch count didn't decrease: double the memory threshold to make progress
        if outputs.len() >= prev_batches {
            mb_threshold = mb_threshold.saturating_mul(2);

            if mb_threshold > 1024 {
                anyhow::bail!(
                    "merge operation would exceed maximum memory threshold \
                     (MB threshold {mb_threshold}, iteration {iteration}, batches {}, input_docs {})",
                    outputs.len(),
                    inputs.len()
                );
            }
        }

        // Iterate again with batch outputs as new inputs
        prev_batches = outputs.len();
        input_storage = outputs;
        inputs = input_storage
            .iter()
            .flat_map(|batch| batch.split(|c| *c == b'\n'))
            .collect();

        iteration += 1;
    }
}

/// Process documents from a slice into a MemTable until `mb_threshold` is
/// reached or all `docs` are consumed. Returns newline-separated reduced
/// documents and the count of documents consumed.
fn do_merge_bounded(
    full: bool,
    key: &[u8],
    inputs: &[&[u8]],
    schema: &str,
    mb_threshold: usize,
) -> anyhow::Result<(Vec<u8>, usize)> {
    let bundle = doc::validation::build_bundle(schema.as_bytes())?;
    let validator = doc::Validator::new(bundle)?;
    let spec =
        doc::combine::Spec::with_one_binding(full, [], "connector state", Vec::new(), validator);
    let memtable = doc::combine::MemTable::new(spec);

    let key = String::from_utf8_lossy(key);
    let key = doc::BumpStr::from_str(&key, memtable.alloc());
    let mut consumed = 0usize;

    for op_bytes in inputs {
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
        consumed += 1;

        let bytes_used = memtable
            .alloc()
            .allocated_bytes()
            .saturating_sub(memtable.alloc().chunk_capacity());

        if bytes_used > mb_threshold * 1024 * 1024 {
            break;
        }
    }

    let mut out = Vec::new();
    for (index, drained) in memtable.try_into_drainer()?.enumerate() {
        let doc::combine::DrainedDoc { meta: _, root } = drained?;
        let doc::OwnedNode::Heap(root) = root else {
            unreachable!()
        };

        if index != 0 {
            out.push(b'\n');
        }

        // Extract just the document (index 1) from [key, doc] arrays
        let doc::HeapNode::Array(_, array) = root.get() else {
            unreachable!()
        };
        let node = &array[1];

        serde_json::to_writer(&mut out, &doc::SerPolicy::noop().on(node))
            .expect("serialization cannot fail");
    }

    Ok((out, consumed))
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

    /// Verify merge batching handles many operands that would exceed memory
    /// threshold. Connectors may emit many small merge-patch updates, and this
    /// can turn into many merge operands (hundreds of thousands). We handle this
    /// through iterative merges of batches of inputs, iteratively reducing until
    /// convergence.
    #[tokio::test]
    async fn test_merge_many_operands_batched() {
        // Initialize tracing to see merge operator logs
        let _ = tracing_subscriber::fmt()
            .with_env_filter("runtime=debug")
            .with_writer(std::io::stderr)
            .try_init();

        let db = RocksDB::open(None).await.unwrap();

        // Generate many merge operations that will exceed the batch memory threshold.
        // Each document is a merge-patch updating a unique key in a "cursors" object,
        // similar to how some connectors track per-partition state.
        let num_operands = 50_000;
        let mut wb = rocksdb::WriteBatch::default();

        for i in 0..num_operands {
            // Each merge-patch sets a unique cursor key with some payload.
            // The payload size ensures we'll exceed the 32MB initial threshold and trigger batching.
            let doc = format!(
                r#"{{"cursors":{{"partition_{:05}":{{"offset":{},"timestamp":"2024-01-01T00:00:00Z","metadata":"padding_to_increase_document_size_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}}}}}}"#,
                i,
                i * 1000
            );
            wb.merge(RocksDB::CONNECTOR_STATE_KEY, &doc);
        }
        db.write_opt(wb, Default::default()).await.unwrap();

        // Force a compaction to trigger the merge operator with all operands at once.
        // This is what happens during recovery or when RocksDB decides to compact.
        db.db.compact_range::<&[u8], &[u8]>(None, None);

        // Load the state - this will also trigger a full merge if not already compacted.
        let state = db
            .load_connector_state(models::RawValue::from_string("{}".to_string()).unwrap())
            .await
            .expect("batched merge should handle many operands");

        // Verify the merged state contains all cursor entries.
        let parsed: serde_json::Value = serde_json::from_str(state.get()).unwrap();
        let cursors = parsed.get("cursors").expect("should have cursors object");
        let cursors_obj = cursors.as_object().expect("cursors should be an object");

        assert_eq!(
            cursors_obj.len(),
            num_operands,
            "all cursor partitions should be present after merge"
        );

        // Spot-check a few entries.
        assert_eq!(cursors_obj["partition_00000"]["offset"], 0);
        assert_eq!(cursors_obj["partition_00100"]["offset"], 100_000);
        assert_eq!(cursors_obj["partition_49999"]["offset"], 49_999_000);
    }
}
