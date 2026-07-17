use serde_json::json;

// Run skim_projections over a collection model, returning a compact view of
// each projection's redact inference plus any validation errors. This mirrors
// the flow-web `skim_collection_projections` entry point used by the dashboard.
fn skim(model: serde_json::Value) -> (Vec<(String, String, String)>, Vec<String>) {
    let model: models::CollectionDef = serde_json::from_value(model).unwrap();

    let scope_url = url::Url::parse("flow://collection/acmeCo/test").unwrap();
    let scope = json::Scope::new(&scope_url);
    let collection = models::Collection::new("acmeCo/test");
    let mut errors = tables::Errors::new();

    let projections =
        validation::collection::skim_projections(scope, &collection, &model, &mut errors);

    let projections = projections
        .into_iter()
        .map(|p| {
            let redact = p
                .inference
                .as_ref()
                .map(|i| i.redact().as_str_name().to_string())
                .unwrap_or_default();
            (p.field, p.ptr, redact)
        })
        .collect();

    let errors = errors
        .into_iter()
        .map(|err| format!("{:#}", err.error))
        .collect();

    (projections, errors)
}

// The repro from the issue: a redact annotation on the write schema of a
// collection with a standalone read schema (no $ref to flow://write-schema).
// Redaction runs at write time against the write schema, so the annotation is
// in force and must surface on the projection's inference.
#[test]
fn test_write_schema_redact_with_standalone_read_schema() {
    let (projections, errors) = skim(json!({
        "key": ["/id"],
        "writeSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "secret": {"type": "string", "redact": {"strategy": "block"}}
            },
            "required": ["id"]
        },
        "readSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "secret": {"type": "string"}
            },
            "required": ["id"]
        }
    }));

    insta::assert_debug_snapshot!((projections, errors));
}

// A single schema plays both roles: the annotation already surfaces today.
#[test]
fn test_single_schema_redact() {
    let (projections, errors) = skim(json!({
        "key": ["/id"],
        "schema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "secret": {"type": "string", "redact": {"strategy": "sha256"}}
            },
            "required": ["id"]
        }
    }));

    insta::assert_debug_snapshot!((projections, errors));
}

// A read schema which $refs flow://write-schema: the annotation flows through
// the inlined reference and already surfaces today.
#[test]
fn test_ref_style_read_schema_redact() {
    let (projections, errors) = skim(json!({
        "key": ["/id"],
        "writeSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "secret": {"type": "string", "redact": {"strategy": "block"}}
            },
            "required": ["id"]
        },
        "readSchema": {
            "allOf": [{"$ref": "flow://write-schema"}]
        }
    }));

    insta::assert_debug_snapshot!((projections, errors));
}

// `block` at a location which must exist is reported for a write-schema
// annotation just as it is for a single schema. A redact strategy on a key
// location, however, is checked against the read schema only: connector
// -managed write schemas may annotate a key which flow://relaxed-write-schema
// then strips from the read path, so `sha256` on key /id here is deliberately
// not an error.
#[test]
fn test_write_schema_redact_errors_with_standalone_read_schema() {
    let (_projections, errors) = skim(json!({
        "key": ["/id"],
        "writeSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "string", "redact": {"strategy": "sha256"}},
                "message": {"type": "string", "redact": {"strategy": "block"}}
            },
            "required": ["id", "message"]
        },
        "readSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "message": {"type": "string"}
            },
            "required": ["id"]
        }
    }));

    insta::assert_debug_snapshot!(errors);
}
