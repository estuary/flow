// Helpers shared by the materialize and derive task builders for read-side
// schema handling. See estuary/flow#3133.

/// Return whether the shard feature-flag `flag` (a fully-qualified flag label
/// such as [`labels::RELAX_INFERRED_DATETIME_FLAG`]) is set to "true" within a
/// task's `shard_template` labels. This mirrors the Go-side `LabelSet.ValueOf`
/// check used by the v2 runtime dispatch, but reads the flag at the Rust
/// task-build layer where read-side validators are constructed.
pub fn shard_flag_enabled(
    shard_template: Option<&proto_gazette::consumer::ShardSpec>,
    flag: &str,
) -> bool {
    shard_template
        .and_then(|shard| shard.labels.as_ref())
        .and_then(|set| labels::maybe_one(set, flag).ok())
        .map(|value| value == "true")
        .unwrap_or(false)
}

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

    fn shard_with_flag(value: Option<&str>) -> proto_gazette::consumer::ShardSpec {
        let labels =
            value.map(|value| labels::build_set([(labels::RELAX_INFERRED_DATETIME_FLAG, value)]));
        proto_gazette::consumer::ShardSpec {
            labels,
            ..Default::default()
        }
    }

    #[test]
    fn test_shard_flag_enabled() {
        let flag = labels::RELAX_INFERRED_DATETIME_FLAG;

        let on = shard_with_flag(Some("true"));
        assert!(shard_flag_enabled(Some(&on), flag));

        // A value other than "true" does not enable the flag.
        let off = shard_with_flag(Some("false"));
        assert!(!shard_flag_enabled(Some(&off), flag));

        // Flag absent, no labels, or no shard template at all.
        let none = shard_with_flag(None);
        assert!(!shard_flag_enabled(Some(&none), flag));
        assert!(!shard_flag_enabled(None, flag));
    }

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
