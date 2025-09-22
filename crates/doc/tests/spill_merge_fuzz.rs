#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

use doc::{combine, Extractor, HeapNode, Validator};
use json::schema::build::build_schema;
use serde_json::json;
use std::collections::BTreeMap;
use std::io::Write;

#[derive(thiserror::Error, Debug)]
pub enum FuzzError {
    #[error(transparent)]
    Combine(#[from] combine::Error),
    #[error("mismatch: saw actual {actual} but expected {expect}")]
    Mismatch {
        expect: serde_json::Value,
        actual: serde_json::Value,
    },
    #[error("saw actual {0} but no expected keys remain")]
    Unexpected(serde_json::Value),
}

fn run_sequence(seq: Vec<(u8, u8, bool)>) -> Result<(), FuzzError> {
    let ser_policy = doc::SerPolicy::noop();
    let spec = combine::Spec::with_bindings(
        std::iter::repeat_with(|| {
            let schema = build_schema(
                url::Url::parse("http://example/schema").unwrap(),
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
                true, // Full reductions.
                vec![Extractor::new("/key", &ser_policy)],
                "source-name",
                None,
                Validator::new(schema).unwrap(),
            )
        })
        .take(2),
        Vec::new(),
    );

    let mut spill = combine::SpillWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
    let chunk_target = 1 << 20;
    let mut memtable = combine::MemTable::new(spec);
    let mut expect = BTreeMap::new();

    let mut buf = Vec::new();
    for (i, (seq_key, seq_value, mut is_reduce)) in seq.into_iter().enumerate() {
        // Produce an empirically reasonable number of spills, given quickcheck's defaults.
        if i % 15 == 0 {
            let spec = memtable.spill(&mut spill, chunk_target).unwrap();
            memtable = combine::MemTable::new(spec);
        }

        buf.clear();
        write!(&mut buf, "{{\"key\":{seq_key},\"arr\":[{seq_value}]}}",).unwrap();

        let doc = HeapNode::from_serde(
            &mut serde_json::Deserializer::from_slice(&buf),
            memtable.alloc(),
        )
        .unwrap();

        expect
            .entry(seq_key)
            .and_modify(|(values, reduced): &mut (Vec<u8>, bool)| {
                if !is_reduce || *reduced {
                    // We can have only one reduced document for a key,
                    // so if it's set already then alter this entry to be a combine.
                    is_reduce = false;
                    values.push(seq_value);
                } else {
                    values.insert(0, seq_value);
                    *reduced = true;
                }
            })
            .or_insert_with(|| (vec![seq_value], is_reduce));

        let binding = if seq_key < 128 { 0 } else { 1 };
        memtable.add(binding, doc, is_reduce).unwrap();
    }

    // Spill final MemTable and begin to drain.
    let spec = memtable.spill(&mut spill, chunk_target).unwrap();
    let (spill, ranges) = spill.into_parts();
    let drainer = combine::SpillDrainer::new(spec, spill, &ranges).unwrap();

    let mut expect_it = expect.into_iter();

    for drained_doc in drainer {
        let drained_doc = drained_doc?;
        let actual = json!([
            ser_policy.on_owned(&drained_doc.root),
            drained_doc.meta.front()
        ]);

        match expect_it.next() {
            Some((key, (values, reduced))) => {
                let expect = json!([{"key": key, "arr": values}, reduced]);
                // eprintln!("key {key} values {values:?}");

                if actual != expect {
                    return Err(FuzzError::Mismatch { actual, expect });
                }
            }
            None => return Err(FuzzError::Unexpected(actual)),
        }
    }

    Ok(())
}

#[quickcheck]
fn test_spill_and_merge_fuzzing(seq: Vec<(u8, u8, bool)>) -> bool {
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
    run_sequence(vec![(0, 0, false), (0, 0, true)]).unwrap()
}
