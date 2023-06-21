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
    let schema = build_schema(
        url::Url::parse("http://schema").unwrap(),
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

    let mut spill = combine::SpillWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
    let chunk_target = (1 << 20)..(1 << 21);
    let mut memtable = combine::MemTable::new(
        vec![Extractor::new("/key")].into(),
        None,
        Validator::new(schema).unwrap(),
    );
    let mut expect = BTreeMap::new();

    let mut buf = Vec::new();
    for (i, (seq_key, seq_value, mut is_reduce)) in seq.into_iter().enumerate() {
        // Produce an empirically reasonable number of spills, given quickcheck's defaults.
        if i % 15 == 0 {
            let (key, schema, validator) =
                memtable.spill(&mut spill, chunk_target.clone()).unwrap();
            memtable = combine::MemTable::new(key, schema, validator);
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

        memtable.add(doc, is_reduce).unwrap();
    }

    // Spill final MemTable and begin to drain.
    let (key, schema, validator) = memtable.spill(&mut spill, chunk_target.clone()).unwrap();
    let (spill, ranges) = spill.into_parts();
    let mut drainer = combine::SpillDrainer::new(key, schema, spill, &ranges, validator).unwrap();

    let mut count = 0;
    let mut expect_it = expect.into_iter();

    while drainer.drain_while(|node, reduced| {
        count += 1;

        let actual = json!([node, reduced]);

        match expect_it.next() {
            Some((key, (values, reduced))) => {
                let expect = json!([{"key": key, "arr": values}, reduced]);
                // eprintln!("key {key} values {values:?}");

                if actual == expect {
                    Ok(count % 13 == 0) // Restart drain_while() periodically.
                } else {
                    Err(FuzzError::Mismatch { actual, expect })
                }
            }
            None => Err(FuzzError::Unexpected(actual)),
        }
    })? {}

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
