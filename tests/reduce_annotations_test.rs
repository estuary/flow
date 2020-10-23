#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

use estuary::doc::{
    extract_reduce_annotations, reduce_new::Cursor, validate, FullContext, Schema, SchemaIndex,
    Validator,
};
use estuary_json::{schema::build::build_schema, validator::Context, Location};
use itertools::{EitherOrBoth, Itertools};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use url::Url;

#[test]
fn test_validate_then_reduce() {
    let schema = json!({
        "properties": {
            "min": {
                "type": "integer",
                "reduce": {"strategy": "minimize"}
            },
            "max": {
                "type": "number",
                "reduce": {"strategy": "maximize"}
            },
            "sum": {
                "type": "number",
                "reduce": {"strategy": "sum"}
            },
            "lww": {
                "type": "string",
                "reduce": {"strategy": "lastWriteWins"}
            },
            "fww": {
                "type": "string",
                "reduce": {"strategy": "firstWriteWins"}
            },
            "nodes": {
                "type": "array",
                "items": {"$ref": "#"},
                "reduce": {
                    "strategy": "merge",
                    "key": ["/k"]
                }
            }
        },

        // If/then which resets the LHS if presented with an empty object.
        // Otherwise, a deep-merge is performed.
        "if": { "type": "object", "maxProperties": 0 },
        "then": { "reduce": {"strategy": "lastWriteWins"} },
        "else": { "reduce": {"strategy": "merge"} },
    });

    let curi = Url::parse("https://example/schema").unwrap();
    let schema: Schema = build_schema(curi.clone(), &schema).unwrap();

    let mut index = SchemaIndex::new();
    index.add(&schema).unwrap();
    index.verify_references().unwrap();
    let mut validator = Validator::<FullContext>::new(&index);

    let cases = vec![
        (json!({"lww": "one"}), json!({"lww": "one"})),
        // lww updates with each write. Initialize fww.
        (
            json!({"fww": "two", "lww": "two"}),
            json!({"fww": "two", "lww": "two"}),
        ),
        // fww ignores a subsequent update.
        (
            json!({"fww": "ignored"}),
            json!({"fww": "two", "lww": "two"}),
        ),
        // Initialize min, max, & sum.
        (
            json!({"min": 42, "max": 42, "sum": 42}),
            json!({"fww": "two", "lww": "two", "min": 42, "max": 42, "sum": 42}),
        ),
        // They accumulate values as expected.
        (
            json!({"min": 5, "max": 5, "sum": 5}),
            json!({"fww": "two", "lww": "two", "min": 5, "max": 42, "sum": 47}),
        ),
        (
            json!({"min": 49, "max": 49.5, "sum": 49}),
            json!({"fww": "two", "lww": "two", "min": 5, "max": 49.5, "sum": 96}),
        ),
        // Trigger reset if/then case.
        (json!({}), json!({})),
        // Initialize a nested fixture.
        (
            json!({"nodes": [{"k": "a", "sum": 1}, {"k": "c", "sum": 1}]}),
            json!({"nodes": [{"k": "a", "sum": 1}, {"k": "c", "sum": 1}]}),
        ),
        // Recursive nodes are deep merged keyed on "k" property.
        (
            json!({"nodes": [{"k": "a", "sum": 2}, {"k": "b", "sum": 2}]}),
            json!({"nodes": [{"k": "a", "sum": 3}, {"k": "b", "sum": 2}, {"k": "c", "sum": 1}]}),
        ),
        // Multiple levels of nesting.
        (
            json!({"nodes": [
                {"k": "a", "nodes": [{"k": "ab", "sum": 1}]}
            ]}),
            json!({"nodes": [
                {"k": "a", "sum": 3, "nodes": [{"k": "ab", "sum": 1}]},
                {"k": "b", "sum": 2},
                {"k": "c", "sum": 1}
            ]}),
        ),
        (
            json!({"nodes": [
                {"k": "a", "nodes": [
                    {"k": "aa", "sum": 1},
                    {"k": "ab", "sum": 2},
                ]},
                {"k": "c", "sum": 32, "nodes": [
                    {"k": "cc", "fww": "foo"},
                ]}
            ]}),
            json!({"nodes": [
                {"k": "a", "sum": 3, "nodes": [
                    {"k": "aa", "sum": 1},
                    {"k": "ab", "sum": 3},
                ]},
                {"k": "b", "sum": 2},
                {"k": "c", "sum": 33, "nodes": [
                    {"k": "cc", "fww": "foo"}
                ]},
            ]}),
        ),
    ];

    let mut lhs: Option<Value> = None;
    for (rhs, expect) in cases {
        let span = validate(&mut validator, &curi, &rhs).unwrap();
        let tape = extract_reduce_annotations(span, validator.outcomes());
        let tape = &mut tape.as_slice();

        let cursor = match &lhs {
            Some(lhs) => Cursor::Both {
                tape,
                loc: Location::Root,
                lhs: lhs.clone(),
                rhs,
                prune: true,
            },
            None => Cursor::Right {
                tape,
                loc: Location::Root,
                rhs,
                prune: true,
            },
        };

        let reduced = cursor.reduce().unwrap();
        assert!(tape.is_empty());
        assert_eq!(&reduced, &expect);
        lhs = Some(reduced);
    }
}

#[quickcheck]
fn test_qc_set_array(mut seq: Vec<(bool, Vec<u8>, Vec<u8>)>) -> bool {
    if seq.len() < 2 {
        return true; // Reduction needs two documents.
    }

    // Collect reducible set documents, as well as the final expected value.
    let mut docs = Vec::new();
    let mut expect: Vec<(u8, u32)> = Vec::new();

    for iter in &mut seq {
        let (is_intersect, int_or_rem, add) = iter;

        // Inputs must be sorted and de-duplicated.
        int_or_rem.sort();
        int_or_rem.dedup();
        add.sort();
        add.dedup();

        // Intersect / remove into |expect|.
        expect =
            itertools::merge_join_by(expect.into_iter(), int_or_rem.iter(), |(l, _), r| l.cmp(r))
                .filter_map(|eob| match eob {
                    EitherOrBoth::Both(l, _) if *is_intersect => Some(l),
                    EitherOrBoth::Left(l) if !*is_intersect => Some(l),
                    _ => None,
                })
                .collect();

        // Add into |expect|.
        expect = itertools::merge_join_by(expect.into_iter(), add.iter(), |(l, _), r| l.cmp(r))
            .filter_map(|eob| match eob {
                EitherOrBoth::Left((n, c)) => Some((n, c)),
                EitherOrBoth::Both((n, c), _) => Some((n, c + 1)),
                EitherOrBoth::Right(n) => Some((*n, 1)),
            })
            .collect();

        let int_or_rem = int_or_rem.iter().map(|v| json!([v])).collect::<Vec<_>>();
        let add = add.iter().map(|v| json!([v, 1])).collect::<Vec<_>>();

        if *is_intersect {
            docs.push(json!({
                "intersect": int_or_rem,
                "add": add,
            }));
        } else {
            docs.push(json!({
                "remove": int_or_rem,
                "add": add,
            }));
        }
    }

    let schema = json!({
        "$defs": {
            "entry": {
                "type": "array",
                "items": [
                    { "type": "integer" },
                    {
                        "type": "integer",
                        "reduce": { "strategy": "sum" },
                    },
                ],
                "reduce": { "strategy": "merge" },
            }
        },
        "properties": {
            "add": { "items": { "$ref": "#/$defs/entry" } }
        },
        "reduce": {
            "strategy": "set",
            "key": ["/0"],
        },
    });

    let curi = Url::parse("https://example/schema").unwrap();
    let schema: Schema = build_schema(curi.clone(), &schema).unwrap();

    let mut index = SchemaIndex::new();
    index.add(&schema).unwrap();
    index.verify_references().unwrap();
    let mut validator = Validator::<FullContext>::new(&index);

    let actual: TestArray =
        serde_json::from_value(reduce_tree(&mut validator, &curi, docs)).unwrap();
    actual.add == expect
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct TestArray {
    add: Vec<(u8, u32)>,
}

#[quickcheck]
fn test_qc_set_map(seq: Vec<(bool, Vec<u8>, Vec<u8>)>) -> bool {
    if seq.len() < 2 {
        return true; // Reduction needs two documents.
    }

    // Collect reducible set documents, as well as the final expected value.
    let mut docs = Vec::new();
    let mut expect: BTreeMap<String, u32> = BTreeMap::new();

    for iter in &seq {
        let (is_intersect, int_or_rem, add) = iter;

        // Project integer keys to strings.
        // Inputs must be sorted and de-duplicated.
        let mut int_or_rem = int_or_rem
            .into_iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>();
        int_or_rem.sort();
        int_or_rem.dedup();

        let mut add = add.into_iter().map(|n| n.to_string()).collect::<Vec<_>>();
        add.sort();
        add.dedup();

        // Intersect / remove into |expect|.
        expect =
            itertools::merge_join_by(expect.into_iter(), int_or_rem.iter(), |(l, _), r| l.cmp(r))
                .filter_map(|eob| match eob {
                    EitherOrBoth::Both(l, _) if *is_intersect => Some(l),
                    EitherOrBoth::Left(l) if !*is_intersect => Some(l),
                    _ => None,
                })
                .collect();

        // Add into |expect|.
        expect = itertools::merge_join_by(expect.into_iter(), add.iter(), |(l, _), r| l.cmp(r))
            .filter_map(|eob| match eob {
                EitherOrBoth::Left((n, c)) => Some((n, c)),
                EitherOrBoth::Both((n, c), _) => Some((n, c + 1)),
                EitherOrBoth::Right(n) => Some((n.clone(), 1)),
            })
            .collect();

        let int_or_rem = int_or_rem
            .iter()
            .map(|v| (v.to_string(), Value::Null))
            .collect::<Map<_, _>>();
        let add = add
            .iter()
            .map(|v| (v.to_string(), json!(1)))
            .collect::<Map<_, _>>();

        if *is_intersect {
            docs.push(json!({
                "intersect": int_or_rem,
                "add": add,
            }));
        } else {
            docs.push(json!({
                "remove": int_or_rem,
                "add": add,
            }));
        }
    }

    let schema = json!({
        "properties": {
            "add": {
                "additionalProperties": {
                    "type": "integer",
                    "reduce": { "strategy": "sum" },
                }
            }
        },
        "reduce": {
            "strategy": "set",
        },
    });

    let curi = Url::parse("https://example/schema").unwrap();
    let schema: Schema = build_schema(curi.clone(), &schema).unwrap();

    let mut index = SchemaIndex::new();
    index.add(&schema).unwrap();
    index.verify_references().unwrap();
    let mut validator = Validator::<FullContext>::new(&index);

    let actual: TestMap = serde_json::from_value(reduce_tree(&mut validator, &curi, docs)).unwrap();
    actual.add == expect
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct TestMap {
    add: BTreeMap<String, u32>,
}

fn reduce_tree<C: Context>(
    validator: &mut Validator<C>,
    curi: &Url,
    mut docs: Vec<Value>,
) -> Value {
    // Iteratively reduce |docs| by walking it in chunked windows, producing
    // a new Value for each chunk. Intuitively, we're reducing |docs| by
    // interpreting it as a tree and ascending from leaf to root (as opposed
    // to walking it from left-to-right). This is more work overall,
    // but maximally tests the associative correctness of set operations.
    // Note that |expect| is the result of left-to-right application.
    while docs.len() > 1 {
        docs = docs
            .into_iter()
            .chunks(2)
            .into_iter()
            .enumerate()
            .map(|(n, chunk)| {
                let mut lhs: Option<Value> = None;

                for rhs in chunk {
                    let span = validate(validator, curi, &rhs).unwrap();
                    let tape = extract_reduce_annotations(span, validator.outcomes());
                    let tape = &mut tape.as_slice();

                    let cursor = match &lhs {
                        Some(lhs) => Cursor::Both {
                            tape,
                            loc: Location::Root,
                            lhs: lhs.clone(),
                            rhs,
                            prune: n == 0,
                        },
                        None => Cursor::Right {
                            tape,
                            loc: Location::Root,
                            rhs,
                            prune: n == 0,
                        },
                    };

                    lhs = Some(cursor.reduce().unwrap());
                    assert!(tape.is_empty());
                }
                lhs.unwrap()
            })
            .collect();
    }

    docs.into_iter().next().unwrap()
}
