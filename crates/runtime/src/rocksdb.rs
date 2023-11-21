use anyhow::Context;
use prost::Message;
use proto_flow::runtime::RocksDbDescriptor;
use proto_gazette::consumer;

/// RocksDB database used for task state.
pub struct RocksDB {
    db: rocksdb::DB,
    _path: std::path::PathBuf,
    _tmp: Option<tempfile::TempDir>,
}

impl std::ops::Deref for RocksDB {
    type Target = rocksdb::DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl RocksDB {
    /// Open a RocksDB from an optional descriptor.
    /// If a descriptor isn't provided, then a tempdir is used instead.
    pub fn open(desc: Option<RocksDbDescriptor>) -> anyhow::Result<Self> {
        let (mut opts, path, _tmp) = match desc {
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
                        rocksdb::Env::from_raw(
                            rocksdb_env_memptr as *mut librocksdb_sys::rocksdb_env_t,
                        )
                    };
                    opts.set_env(&env);
                }
                (opts, std::path::PathBuf::from(rocksdb_path), None)
            }
            None => {
                let dir = tempfile::TempDir::new().unwrap();
                let opts = rocksdb::Options::default();

                tracing::debug!(
                    rocksdb_path = ?dir.path(),
                    "opening temporary RocksDB database"
                );

                (opts, dir.path().to_owned(), Some(dir))
            }
        };

        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let column_families = match rocksdb::DB::list_cf(&opts, &path) {
            Ok(cf) => cf,
            // Listing column families will fail if the DB doesn't exist.
            // Assume as such, as we'll otherwise fail when we attempt to open.
            Err(_) => vec![rocksdb::DEFAULT_COLUMN_FAMILY_NAME.to_string()],
        };

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

        let db = rocksdb::DB::open_cf_descriptors(&opts, &path, cf_descriptors)
            .context("failed to open RocksDB")?;

        // TODO(johnny): Handle migration from a JSON state file here ?

        Ok(Self {
            db,
            _path: path,
            _tmp,
        })
    }

    /// Load a persisted runtime Checkpoint.
    pub fn load_checkpoint(&self) -> anyhow::Result<consumer::Checkpoint> {
        match self.db.get_pinned(Self::CHECKPOINT_KEY)? {
            Some(v) => Ok(consumer::Checkpoint::decode(v.as_ref())
                .context("failed to decode consumer checkpoint")?),
            None => Ok(consumer::Checkpoint::default()),
        }
    }

    /// Load a persisted connector state, as a String of encoded JSON.
    pub fn load_connector_state(&self) -> anyhow::Result<Option<String>> {
        let state = self
            .db
            .get_pinned(Self::CONNECTOR_STATE_KEY)
            .context("failed to load connector state")?;

        // If found, decode and attach to `open`.
        if let Some(state) = state {
            let state = String::from_utf8(state.to_vec()).context("decoding state as UTF-8")?;
            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

    // Key encoding under which a marshalled checkpoint is stored.
    pub const CHECKPOINT_KEY: &str = "checkpoint";
    // Key encoding under which a connector state is stored.
    pub const CONNECTOR_STATE_KEY: &str = "connector-state";
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
    let bundle = doc::validation::build_bundle(schema)?;
    let _validator = doc::Validator::new(bundle)?;

    let schema_1 = schema.to_owned();
    let schema_2 = schema.to_owned();

    let full_merge_fn = move |key: &[u8],
                              initial: Option<&[u8]>,
                              operands: &mut rocksdb::merge_operator::MergeOperands|
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
                                 operands: &mut rocksdb::merge_operator::MergeOperands|
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
    operands: &mut rocksdb::merge_operator::MergeOperands,
    schema: &str,
) -> anyhow::Result<Vec<u8>> {
    let bundle = doc::validation::build_bundle(schema).unwrap();
    let validator = doc::Validator::new(bundle).unwrap();
    let spec = doc::combine::Spec::with_one_binding(full, [], None, validator);
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

            let doc = doc::HeapNode::Array(doc::BumpVec::with_contents(
                memtable.alloc(),
                [doc::HeapNode::String(key), op].into_iter(),
            ));
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
        serde_json::to_writer(&mut out, &doc::SerPolicy::default().on(node)).unwrap();
    }

    Ok(out)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn connector_state_merge() {
        let db = RocksDB::open(None).unwrap();

        for doc in [
            r#"{"a":"b","n":null}"#,
            r#"{"a":"c","nn":null}"#,
            r#"{"d":"e","ans":42}"#,
        ] {
            db.merge(RocksDB::CONNECTOR_STATE_KEY, doc).unwrap();
        }

        let output = db.get(RocksDB::CONNECTOR_STATE_KEY).unwrap().unwrap();
        let output = String::from_utf8(output).unwrap();

        assert_eq!(output, r#"{"a":"c","ans":42,"d":"e","n":null}"#);
    }
}
