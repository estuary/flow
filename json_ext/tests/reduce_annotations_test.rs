use estuary_json::{
    de,
    schema::{self, index},
    validator,
};
use estuary_json_ext::{self as ejx, reduce};
use serde_json::{json, Value};
use url::Url;

#[test]
fn test_validate_and_reduce() {
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
    let scm: schema::Schema<ejx::Annotation> = schema::build::build_schema(uri.clone(), &scm).unwrap();

    let mut idx = index::Index::new();
    idx.add(&scm).unwrap();
    idx.verify_references().unwrap();

    let mut into = Value::Null;

    do_reduce(json!({
        "fww": "first",
        "lww": "first",
        "nodes": [{"k": "a", "sum": 10}, {"k": "b", "sum": 20}],
    }), &mut into, &idx, &uri);

    println!("into: {}", into);

    do_reduce(json!({
        "fww": "second",
        "lww": "second",
        "nodes": [{"k": "b", "sum": 33}],
    }), &mut into, &idx, &uri);

    println!("into: {}", into);

    do_reduce(json!({
        "nodes": [{"k": "a", "sum": 1.34159, "min": 5}, {"k": "c", "max": 12.34}],
    }), &mut into, &idx, &uri);

    println!("into: {}", into);

    do_reduce(json!({
        "nodes": [{"k": "b", "sum": -10.5}, {"k": "c", "max": 11}],
    }), &mut into, &idx, &uri);

    println!("into: {}", into);

    assert_eq!(into, json!({
        "fww": "first",
        "lww": "second",
        "nodes": [
            {"k": "a", "min": 5, "sum": 11.34159},
            {"k": "b", "sum": 42.5},
            {"k": "c", "max": 12.34},
        ],
    }));
}

fn do_reduce(doc: Value, into: &mut Value, idx: &index::Index<ejx::Annotation>, curi: &url::Url) {
    let mut val =
        validator::Validator::<ejx::Annotation, validator::FullContext>::new(idx, curi).unwrap();

    let _out = de::walk(&doc, &mut val).unwrap();
    assert_eq!(val.invalid(), false);

    let strats = ejx::extract_reduce_annotations(val.outcomes());
    println!("strategies: {:?}", strats);

    reduce::Reducer{
        at: 0,
        val: doc,
        into: into,
        created: false,
        idx: &strats,
    }.reduce();
}