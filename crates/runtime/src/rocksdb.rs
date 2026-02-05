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
        match do_merge_rocks(true, initial, key, operands, &schema_1) {
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
        match do_merge_rocks(false, initial, key, operands, &schema_2) {
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

fn do_merge_rocks(
    full: bool,
    initial: Option<&[u8]>,
    key: &[u8],
    operands: &rocksdb::merge_operator::MergeOperands,
    schema: &str,
) -> anyhow::Result<Vec<u8>> {
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

    do_merge(full, key, inputs, schema, 32 * 1024 * 1024, usize::MAX)
}

/// Iteratively reduce `inputs` in batches, re-merging batch outputs until a
/// single result remains. `batch_byte_target` is the initial allocator memory per batch,
/// and `batch_op_target` is the initial documents per batch (useful for testing).
fn do_merge(
    full: bool,
    key: &[u8],
    inputs: Vec<&[u8]>,
    schema: &str,
    mut batch_byte_target: usize,
    mut batch_op_target: usize,
) -> anyhow::Result<Vec<u8>> {
    const MAX_BYTE_THRESHOLD: usize = 1 << 30; // 1 GiB

    // Shadow `inputs` to decouple the reference lifetime from the caller,
    // allowing reassignment to borrow from local `input_storage` in the loop.
    let mut inputs: Vec<&[u8]> = inputs;
    let mut input_storage: Vec<Vec<u8>>;

    let mut iteration = 0usize;
    let mut prev_batches = usize::MAX;

    loop {
        let mut offset = 0;
        let mut outputs: Vec<Vec<u8>> = Vec::new();

        // Only the first batch of each iteration uses `full` reduction.
        // The initial/base value is always in the first batch. Subsequent batches
        // contain only operands and must use associative (non-full) reduction to
        // preserve null deletion markers in merge-patch schemas.
        let mut is_first_batch = true;

        // Process all inputs in batches constrained to `byte_threshold` and `max_count`.
        while offset != inputs.len() {
            let batch_full = full && is_first_batch;
            is_first_batch = false;

            let (output, consumed) = do_merge_bounded(
                batch_full,
                key,
                &inputs[offset..],
                schema,
                batch_byte_target,
                batch_op_target,
            )?;

            outputs.push(output);
            offset += consumed;
        }

        tracing::debug!(
            iteration,
            inputs = inputs.len(),
            batches = outputs.len(),
            prev_batches,
            batch_byte_target,
            "do_merge iteration complete"
        );

        // Stop when we have a single batch output.
        if outputs.len() <= 1 {
            return Ok(outputs.into_iter().next().unwrap_or_default());
        }

        // Batch count didn't decrease: double thresholds to make progress.
        // Bail if both thresholds are already at their caps — we've exhausted
        // our escalation budget and still can't converge.
        if outputs.len() >= prev_batches {
            if batch_byte_target >= MAX_BYTE_THRESHOLD && batch_op_target >= 1_024 * 1_024 {
                anyhow::bail!(
                    "merge operation failed to converge \
                     (batch_byte_target {batch_byte_target}, batch_op_target {batch_op_target}, \
                     iteration {iteration}, batches {}, input_docs {})",
                    outputs.len(),
                    inputs.len()
                );
            }
            batch_byte_target = batch_byte_target.saturating_mul(2).min(MAX_BYTE_THRESHOLD);
            batch_op_target = batch_op_target.saturating_mul(2);
        }

        // Iterate again with batch outputs (which are a newline-separated
        // remainder of non-associative merge operands) as new inputs.
        // The first output descends from the original base value, so
        // `is_first_batch` will correctly be `true` for it on the next iteration.
        prev_batches = outputs.len();
        input_storage = outputs;
        inputs = input_storage
            .iter()
            .flat_map(|batch| batch.split(|c| *c == b'\n'))
            .collect();

        iteration += 1;
    }
}

/// Process documents from a slice into a MemTable until `byte_threshold` is
/// reached, `max_count` documents are consumed, or all `inputs` are consumed.
/// Returns newline-separated reduced documents and the count consumed.
fn do_merge_bounded(
    full: bool,
    key: &[u8],
    inputs: &[&[u8]],
    schema: &str,
    byte_threshold: usize,
    max_count: usize,
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

        if bytes_used > byte_threshold || consumed >= max_count {
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

    fn test_schema() -> String {
        let state_schema = doc::reduce::merge_patch_schema();
        task_state_default_json_schema(&state_schema).to_string()
    }

    /// Compute expected result via `json_patch::merge`, then verify `do_merge_batched`
    /// matches for both full and partial-then-full merges at each batch size.
    fn check_merge(base: &str, ops: &[&str], max_counts: &[usize]) {
        let schema = test_schema();
        let key = RocksDB::CONNECTOR_STATE_KEY.as_bytes();

        let mut expected: serde_json::Value = serde_json::from_str(base).unwrap();
        for op in ops {
            json_patch::merge(&mut expected, &serde_json::from_str(op).unwrap());
        }

        let op_bytes: Vec<&[u8]> = ops.iter().map(|o| o.as_bytes()).collect();
        let mut all_inputs: Vec<&[u8]> = vec![base.as_bytes()];
        all_inputs.extend_from_slice(&op_bytes);

        for &mc in max_counts {
            // Full merge.
            let result = do_merge(true, key, all_inputs.clone(), &schema, usize::MAX, mc).unwrap();
            let actual: serde_json::Value = serde_json::from_slice(&result).unwrap();
            assert_eq!(actual, expected, "full merge at max_count={mc}");

            // Partial merge of just operands, then full merge with base.
            let partial = do_merge(false, key, op_bytes.clone(), &schema, usize::MAX, mc).unwrap();
            let mut final_inputs: Vec<&[u8]> = vec![base.as_bytes()];
            for doc in partial.split(|c| *c == b'\n') {
                final_inputs.push(doc);
            }
            let result =
                do_merge(true, key, final_inputs, &schema, usize::MAX, usize::MAX).unwrap();
            let actual: serde_json::Value = serde_json::from_slice(&result).unwrap();
            assert_eq!(actual, expected, "partial+full merge at max_count={mc}");
        }
    }

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
        // Uncomment to initialize tracing and see merge operator logs.
        /*
        let _ = tracing_subscriber::fmt()
            .with_env_filter("runtime=debug")
            .with_writer(std::io::stderr)
            .try_init();
        */

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
        let cursors = parsed["cursors"]
            .as_object()
            .expect("cursors should be an object");
        assert_eq!(cursors.len(), num_operands);

        // Spot-check a few entries.
        assert_eq!(cursors["partition_00000"]["offset"], 0);
        assert_eq!(cursors["partition_00100"]["offset"], 100_000);
        assert_eq!(cursors["partition_49999"]["offset"], 49_999_000);
    }

    /// Null deletion markers must survive batching. Historical bug: when a non-first
    /// batch used `full=true`, LWW delete stripped null markers within the batch
    /// instead of preserving them for re-merge with the base value.
    /// Also verifies the partial merge path preserves null markers.
    #[test]
    fn test_null_deletion_survives_batching() {
        // batch1=[base,op1], batch2=[op2,op3_null] at max_count=2.
        check_merge(
            r#"{"a":1,"x":10}"#,
            &[r#"{"b":2}"#, r#"{"a":5}"#, r#"{"a":null}"#],
            &[2, 3, usize::MAX],
        );
        // Multiple interleaved deletions across batch boundaries.
        check_merge(
            r#"{"a":1,"b":2,"c":3}"#,
            &[
                r#"{"e":5}"#,
                r#"{"a":5}"#,
                r#"{"a":null}"#,
                r#"{"b":5}"#,
                r#"{"b":null}"#,
            ],
            &[2, 3, usize::MAX],
        );
    }

    /// Quickcheck for merge-patch-relevant JSON: small key alphabet to force collisions,
    /// high null frequency to exercise deletion markers, and controlled nesting depth.
    #[derive(Clone, Debug)]
    struct MergePatchValue(serde_json::Value);

    impl quickcheck::Arbitrary for MergePatchValue {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self(gen_merge_patch_value(g, 3))
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            match &self.0 {
                serde_json::Value::Object(map) => {
                    let entries: Vec<(String, MergePatchValue)> = map
                        .iter()
                        .map(|(k, v)| (k.clone(), MergePatchValue(v.clone())))
                        .collect();
                    Box::new(entries.shrink().map(|es| {
                        let map: serde_json::Map<String, serde_json::Value> =
                            es.into_iter().map(|(k, v)| (k, v.0)).collect();
                        MergePatchValue(serde_json::Value::Object(map))
                    }))
                }
                serde_json::Value::Null => quickcheck::empty_shrinker(),
                _ => Box::new(std::iter::once(MergePatchValue(serde_json::Value::Null))),
            }
        }
    }

    fn gen_range(g: &mut quickcheck::Gen, range: std::ops::Range<u64>) -> u64 {
        <u64 as quickcheck::Arbitrary>::arbitrary(g) % (range.end - range.start) + range.start
    }

    fn gen_merge_patch_value(g: &mut quickcheck::Gen, depth: usize) -> serde_json::Value {
        let choices = if depth > 0 { 10 } else { 7 };
        match gen_range(g, 0..choices) {
            0 | 1 | 2 => serde_json::Value::Null, // ~30% null
            3 => serde_json::Value::Bool(<bool as quickcheck::Arbitrary>::arbitrary(g)),
            4 => serde_json::json!(<u8 as quickcheck::Arbitrary>::arbitrary(g)),
            5 => serde_json::json!(<String as quickcheck::Arbitrary>::arbitrary(g)),
            6 => serde_json::json!([<u8 as quickcheck::Arbitrary>::arbitrary(g)]),
            _ => {
                let keys = ["a", "b", "c", "d", "e"];
                let num_keys = gen_range(g, 1..6) as usize;
                let mut map = serde_json::Map::new();
                for _ in 0..num_keys {
                    let ki = gen_range(g, 0..keys.len() as u64) as usize;
                    map.insert(keys[ki].to_string(), gen_merge_patch_value(g, depth - 1));
                }
                serde_json::Value::Object(map)
            }
        }
    }

    #[derive(Clone, Debug)]
    struct MergePatchSequence {
        base: MergePatchValue,
        operands: Vec<MergePatchValue>,
    }

    impl quickcheck::Arbitrary for MergePatchSequence {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let n = gen_range(g, 1..21) as usize;
            // Connector state is always a JSON object at the top level.
            let gen_obj = |g: &mut quickcheck::Gen| loop {
                let v = gen_merge_patch_value(g, 3);
                if v.is_object() {
                    return MergePatchValue(v);
                }
            };
            Self {
                base: gen_obj(g),
                operands: (0..n).map(|_| gen_obj(g)).collect(),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let base = self.base.clone();
            let operands = self.operands.clone();
            Box::new(operands.shrink().map(move |ops| MergePatchSequence {
                base: base.clone(),
                operands: ops,
            }))
        }
    }

    /// Fuzz: `do_merge_batched` at various batch sizes must match `json_patch::merge`,
    /// for both full merges and partial-then-full merges.
    #[test]
    fn fuzz_batched_merge_matches_reference() {
        fn prop(seq: MergePatchSequence) -> bool {
            let schema = test_schema();
            let key = RocksDB::CONNECTOR_STATE_KEY.as_bytes();

            let mut expected = seq.base.0.clone();
            for op in &seq.operands {
                json_patch::merge(&mut expected, &op.0);
            }

            let base_bytes = serde_json::to_vec(&seq.base.0).unwrap();
            let op_bytes: Vec<Vec<u8>> = seq
                .operands
                .iter()
                .map(|op| serde_json::to_vec(&op.0).unwrap())
                .collect();

            let mut all_inputs: Vec<&[u8]> = vec![&base_bytes];
            all_inputs.extend(op_bytes.iter().map(|v| v.as_slice()));
            let ops_only: Vec<&[u8]> = op_bytes.iter().map(|v| v.as_slice()).collect();

            for mc in [2, 3, 5, usize::MAX] {
                // Full merge.
                let result =
                    do_merge(true, key, all_inputs.clone(), &schema, usize::MAX, mc).unwrap();
                if serde_json::from_slice::<serde_json::Value>(&result).unwrap() != expected {
                    return false;
                }

                if ops_only.is_empty() {
                    continue;
                }

                // Partial merge of operands, then full merge with base.
                let partial =
                    do_merge(false, key, ops_only.clone(), &schema, usize::MAX, mc).unwrap();
                let mut final_inputs: Vec<&[u8]> = vec![&base_bytes];
                for doc in partial.split(|c| *c == b'\n') {
                    final_inputs.push(doc);
                }
                let result =
                    do_merge(true, key, final_inputs, &schema, usize::MAX, usize::MAX).unwrap();
                if serde_json::from_slice::<serde_json::Value>(&result).unwrap() != expected {
                    return false;
                }
            }
            true
        }

        quickcheck::QuickCheck::new()
            .tests(1000)
            .max_tests(2000)
            .quickcheck(prop as fn(MergePatchSequence) -> bool);
    }
}
