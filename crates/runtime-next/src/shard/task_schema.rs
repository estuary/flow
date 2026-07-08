// Read-side schema handling shared by the V2 materialize and derive task
// builders. Per-task flag reading lives in `labels::shard_flag_enabled`; the
// relaxation itself is single-sourced in `models`.

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
