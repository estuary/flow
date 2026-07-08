// Helper shared by the materialize and derive task builders for read-side
// schema handling. See estuary/flow#3133. Per-task flag reading lives in
// `labels::shard_flag_enabled`.

/// Return `read_schema_json` with `date`/`date-time`/`time` `format` keywords
/// stripped from its inlined inferred schema, leaving the rest of the bundle
/// untouched. See [`models::Schema::relax_inferred_datetime_formats`].
pub fn relax_inferred_datetime_formats(
    read_schema_json: &bytes::Bytes,
) -> anyhow::Result<bytes::Bytes> {
    let schema = models::Schema::new(models::RawValue::from_str(std::str::from_utf8(
        read_schema_json,
    )?)?);
    let relaxed = schema.relax_inferred_datetime_formats()?;
    Ok(bytes::Bytes::from(relaxed.get().to_string()))
}

#[cfg(test)]
mod test {
    use super::*;

    // A read-schema bundle whose inlined inferred schema tags a field
    // `format: date-time`, shaped as the control plane assembles it.
    const READ_SCHEMA: &str = r#"{
        "$defs": {
            "flow://inferred-schema": {
                "$id": "flow://inferred-schema",
                "type": "object",
                "properties": { "ts": { "type": "string", "format": "date-time" } }
            }
        },
        "allOf": [ { "$ref": "flow://inferred-schema" } ]
    }"#;

    fn is_valid(read_schema_json: &bytes::Bytes, doc: &str) -> bool {
        let mut validator =
            doc::Validator::new(doc::validation::build_bundle(read_schema_json).unwrap()).unwrap();

        let alloc = doc::HeapNode::new_allocator();
        let mut de = serde_json::Deserializer::from_str(doc);
        let node = doc::HeapNode::from_serde(&mut de, &alloc).unwrap();

        validator.is_valid(&node)
    }

    #[test]
    fn test_relax_inferred_datetime_read_side_behavior() {
        let strict = bytes::Bytes::from(READ_SCHEMA);
        let relaxed = relax_inferred_datetime_formats(&strict).unwrap();

        // A space-separated (non-RFC3339) timestamp — the historical, already
        // stored shape from #3133.
        let legacy = r#"{"ts": "2026-06-17 12:46:17.375663+00:00"}"#;
        // An RFC3339-conformant timestamp.
        let conforming = r#"{"ts": "2026-06-17T12:46:17.375663+00:00"}"#;

        // Flag OFF (strict): legacy value is rejected on read; this is the
        // regression from #3116 that the flag exists to relieve.
        assert!(!is_valid(&strict, legacy));
        assert!(is_valid(&strict, conforming));

        // Flag ON (relaxed): the legacy value is tolerated, while conforming
        // values still validate.
        assert!(is_valid(&relaxed, legacy));
        assert!(is_valid(&relaxed, conforming));
    }
}
