use estuary_json::{
    de,
    schema::{self, index},
    validator,
};
use estuary::{doc, doc::reduce};
use serde_json::{json, Value};
use url::Url;

#[test]
fn test_validate_then_reduce() {
    let scm = json!({
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
        "reduce": {"strategy": "merge"}
    });

    let uri = Url::parse("https://example/schema").unwrap();
    let scm: doc::Schema = schema::build::build_schema(uri.clone(), &scm).unwrap();

    let mut idx = index::Index::new();
    idx.add(&scm).unwrap();
    idx.verify_references().unwrap();

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
        // Reset |into|.
        (Value::Null, Value::Null),
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

    let mut into = Value::Null;
    for (i, (doc, expect)) in cases.into_iter().enumerate() {
        let mut val =
            doc::Validator::<validator::FullContext>::new(&idx, &uri)
                .unwrap();

        let _out = de::walk(&doc, &mut val).unwrap();
        assert_eq!(val.invalid(), false);

        let strats = doc::extract_reduce_annotations(val.outcomes());
        println!("strategies: {:?}", strats);

        reduce::Reducer {
            at: 0,
            val: doc,
            into: &mut into,
            created: i == 0,
            idx: &strats,
        }
        .reduce();

        assert_eq!(&expect, &into);
    }
}
