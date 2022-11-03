#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

use doc::{combine, Annotation, HeapNode, Pointer};
use json::schema::{build::build_schema, index::IndexBuilder};
use json::validator::{SpanContext, Validator};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::Write;
use std::rc::Rc;

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

#[quickcheck]
fn test_spill_and_merge_fuzzing(seq: Vec<(u8, u8)>) -> bool {
    let url = url::Url::parse("http://schema").unwrap();
    let schema = build_schema::<Annotation>(
        url,
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
    let key: Rc<[Pointer]> = vec!["/key".into()].into();

    let mut index = IndexBuilder::new();
    index.add(&schema).unwrap();
    index.verify_references().unwrap();
    let index = index.into_index();
    let mut val = Validator::<Annotation, SpanContext>::new(&index);

    let mut spill = combine::SpillWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
    let mut memtable = combine::MemTable::new(key.clone(), schema.curi.clone());
    let mut expect = BTreeMap::new();

    let mut buf = Vec::new();
    for (seq_key, seq_value) in seq {
        // Produce an empirically reasonable number of spills, given quickcheck's defaults.
        if memtable.len() > 15 {
            memtable.spill(&mut spill).unwrap();
            memtable = combine::MemTable::new(key.clone(), schema.curi.clone());
        }

        buf.clear();
        write!(&mut buf, "{{\"key\":{seq_key},\"arr\":[{seq_value}]}}",).unwrap();

        let doc = HeapNode::from_serde(
            &mut serde_json::Deserializer::from_slice(&buf),
            memtable.alloc(),
            memtable.dedup(),
        )
        .unwrap();

        memtable.combine_right(doc, &mut val).unwrap();

        expect
            .entry(seq_key)
            .and_modify(|a: &mut Vec<u8>| a.push(seq_value))
            .or_insert_with(|| vec![seq_value]);
    }

    // Spill final MemTable and begin to drain.
    memtable.spill(&mut spill).unwrap();
    let (spill, ranges) = spill.into_parts();
    let mut drainer =
        combine::SpillDrainer::new(key.clone(), schema.curi.clone(), spill, &ranges).unwrap();

    let mut count = 0;
    let mut expect_it = expect.into_iter();

    loop {
        let res = drainer.drain_while(&mut val, |node, _reduced| {
            count += 1;

            let actual = serde_json::to_value(&node).unwrap();

            match expect_it.next() {
                Some((key, values)) => {
                    let expect = json!({"key": key, "arr": values});
                    // eprintln!("key {key} values {values:?}");

                    if actual == expect {
                        Ok(count % 27 == 0) // Restart drain_while() periodically.
                    } else {
                        Err(FuzzError::Mismatch { actual, expect })
                    }
                }
                None => Err(FuzzError::Unexpected(actual)),
            }
        });

        match res {
            Err(err) => {
                eprintln!("error: {err}");
                return false;
            }
            Ok(true) => continue,
            Ok(false) => break,
        }
    }

    true
}
