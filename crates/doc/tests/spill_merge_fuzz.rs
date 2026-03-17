#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

use doc::{Extractor, HeapNode, Validator, combine};
use json::schema::build::build_schema;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

#[derive(thiserror::Error, Debug)]
pub enum FuzzError {
    #[error(transparent)]
    Combine(#[from] combine::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("full-reduction mismatch: saw actual {actual} but expected {expect}")]
    Mismatch {
        expect: serde_json::Value,
        actual: serde_json::Value,
    },
    #[error("saw actual {0} but no expected keys remain")]
    Unexpected(serde_json::Value),
    #[error("MemDrainer and SpillDrainer disagree: mem={mem}, spill={spill}")]
    DrainerMismatch {
        mem: serde_json::Value,
        spill: serde_json::Value,
    },
}

fn make_spec() -> combine::Spec {
    let ser_policy = doc::SerPolicy::noop();

    combine::Spec::with_bindings(
        // Binding 0: full reduction. Binding 1: associative (non-full) reduction.
        [true, false].into_iter().map(|is_full| {
            let schema = build_schema(
                &url::Url::parse("http://example/schema").unwrap(),
                &json!({
                    "type": "object",
                    "properties": {
                        "key": {"type": "integer"},
                        "arr": {
                            "type": "array",
                            "items": { "type": "integer" },
                            "reduce": { "strategy": "append" }
                        }
                    },
                    "required": ["key"],
                    "additionalProperties": false,
                    "reduce": { "strategy": "merge" }
                }),
            )
            .unwrap();

            (
                is_full,
                vec![Extractor::new("/key", &ser_policy)],
                "source-name",
                Validator::new(schema).unwrap(),
            )
        }),
        Vec::new(),
    )
}

/// Serialize a HeapNode into a HeapEmbedded backed by the given allocator.
fn to_embedded<'a>(node: &HeapNode<'_>, alloc: &'a doc::Allocator) -> doc::HeapEmbedded<'a> {
    let archived = node.to_archive();
    let byte_len = archived.len();
    let u64_len = (byte_len + 7) / 8;
    let buf = alloc.alloc_slice_fill_default::<doc::embedded::U64Le>(u64_len);
    unsafe {
        std::ptr::copy_nonoverlapping(archived.as_ptr(), buf.as_mut_ptr() as *mut u8, byte_len);
    }
    unsafe { doc::HeapEmbedded::from_buffer(buf) }
}

/// Add a document to a MemTable via `add_embedded()`.
fn add_as_embedded(
    memtable: &combine::MemTable,
    binding: u16,
    node: &HeapNode<'_>,
    front: bool,
    keys: &[Extractor],
) {
    let embedded = to_embedded(node, memtable.alloc());
    let mut scratch = bytes::BytesMut::new();
    Extractor::extract_all(embedded.get(), keys, &mut scratch);
    let mut packed_prefix = [0u8; 16];
    let copy_len = scratch.len().min(16);
    packed_prefix[..copy_len].copy_from_slice(&scratch[..copy_len]);

    memtable
        .add_embedded(binding, &packed_prefix, embedded, front, false)
        .unwrap();
}

fn run_sequence(seq: Vec<(u8, u8, bool, bool)>) -> Result<(), FuzzError> {
    let ser_policy = doc::SerPolicy::noop();

    // Two parallel MemTables fed identical documents:
    // one is drained directly (MemDrainer), the other is spilled (SpillDrainer).
    let memtable_mem = combine::MemTable::new(make_spec());
    let mut spill = combine::SpillWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
    let mut memtable_spill = combine::MemTable::new(make_spec());

    // Use a small chunk target (128 bytes) to exercise multi-chunk segments,
    // since each archived entry is ~50 bytes.
    let chunk_target = 128;

    // Expected output for the full-reduction binding (binding 0, key < 128).
    let mut expect_full = BTreeMap::new();
    // Track which keys already have a front document (at most one per key).
    let mut has_front = BTreeSet::new();

    let key = vec![Extractor::new("/key", &ser_policy)];

    let mut buf = Vec::new();
    for (i, (seq_key, seq_value, mut is_reduce, use_embedded)) in seq.into_iter().enumerate() {
        // Spill the spill-path memtable periodically to produce multiple segments.
        if i % 15 == 0 {
            let spec = memtable_spill.spill(&mut spill, chunk_target).unwrap();
            memtable_spill = combine::MemTable::new(spec);
        }

        // Each key can have at most one front (is_reduce) document.
        if is_reduce && has_front.contains(&seq_key) {
            is_reduce = false;
        }
        if is_reduce {
            has_front.insert(seq_key);
        }

        buf.clear();
        write!(&mut buf, "{{\"key\":{seq_key},\"arr\":[{seq_value}]}}").unwrap();

        let doc_mem = HeapNode::from_serde(
            &mut serde_json::Deserializer::from_slice(&buf),
            memtable_mem.alloc(),
        )
        .unwrap();
        let doc_spill = HeapNode::from_serde(
            &mut serde_json::Deserializer::from_slice(&buf),
            memtable_spill.alloc(),
        )
        .unwrap();

        let binding = if seq_key < 128 { 0 } else { 1 };

        // Track expected output for the full-reduction binding.
        if binding == 0 {
            expect_full
                .entry(seq_key)
                .and_modify(|(values, reduced): &mut (Vec<u8>, bool)| {
                    if is_reduce {
                        values.insert(0, seq_value);
                        *reduced = true;
                    } else {
                        values.push(seq_value);
                    }
                })
                .or_insert_with(|| (vec![seq_value], is_reduce));
        }

        // Randomly alternate between add() and add_embedded() to verify
        // that behavior is invariant to the manner in which documents are added.
        if use_embedded {
            add_as_embedded(&memtable_mem, binding, &doc_mem, is_reduce, &key);
            add_as_embedded(&memtable_spill, binding, &doc_spill, is_reduce, &key);
        } else {
            memtable_mem.add(binding, doc_mem, is_reduce).unwrap();
            memtable_spill.add(binding, doc_spill, is_reduce).unwrap();
        }
    }

    // Drain memtable_mem directly via MemDrainer (no spill).
    let mut mem_drainer = memtable_mem.try_into_drainer()?;

    // Spill the final memtable_spill and drain via SpillDrainer.
    let spec = memtable_spill.spill(&mut spill, chunk_target).unwrap();
    let (spill, ranges) = spill.into_parts();
    let mut spill_drainer = combine::SpillDrainer::new(spec, spill, &ranges)?;

    let mut expect_it = expect_full.into_iter();

    loop {
        let mem_doc = mem_drainer.drain_next()?;
        let spill_doc = spill_drainer.drain_next()?;

        match (mem_doc, spill_doc) {
            (Some(mem), Some(spill)) => {
                let mem_val = serde_json::to_value(ser_policy.on_owned(&mem.root)).unwrap();
                let spill_val = serde_json::to_value(ser_policy.on_owned(&spill.root)).unwrap();

                // Cross-validate: MemDrainer and SpillDrainer must produce
                // identical Meta and documents for each output position.
                if mem.meta != spill.meta || mem_val != spill_val {
                    return Err(FuzzError::DrainerMismatch {
                        mem: json!([mem_val, format!("{:?}", mem.meta)]),
                        spill: json!([spill_val, format!("{:?}", spill.meta)]),
                    });
                }

                // For the full-reduction binding, also validate correctness
                // against the independently computed expected output.
                if mem.meta.binding() == 0 {
                    let actual = json!([mem_val, mem.meta.front()]);

                    match expect_it.next() {
                        Some((key, (values, reduced))) => {
                            let expect = json!([{"key": key, "arr": values}, reduced]);

                            if actual != expect {
                                return Err(FuzzError::Mismatch { actual, expect });
                            }
                        }
                        None => return Err(FuzzError::Unexpected(actual)),
                    }
                }
            }
            (None, None) => break,
            (Some(mem), None) => {
                let val = serde_json::to_value(ser_policy.on_owned(&mem.root)).unwrap();
                return Err(FuzzError::DrainerMismatch {
                    mem: json!(["extra doc", val]),
                    spill: json!(null),
                });
            }
            (None, Some(spill)) => {
                let val = serde_json::to_value(ser_policy.on_owned(&spill.root)).unwrap();
                return Err(FuzzError::DrainerMismatch {
                    mem: json!(null),
                    spill: json!(["extra doc", val]),
                });
            }
        }
    }

    Ok(())
}

#[quickcheck]
fn test_spill_and_merge_fuzzing(seq: Vec<(u8, u8, bool, bool)>) -> bool {
    match run_sequence(seq) {
        Err(err) => {
            eprintln!("error: {err}");
            false
        }
        Ok(()) => true,
    }
}

// If the above quickcheck test ever fails, it will produce a minimized
// reproduction case that can be put here for debugging.
#[test]
fn test_spill_and_merge_repro() {
    run_sequence(vec![(0, 0, false, false), (0, 0, true, true)]).unwrap()
}
