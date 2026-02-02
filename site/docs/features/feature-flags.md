# Feature Flags

Feature flags are advanced configuration options that modify connector behavior for non-standard or complex situations. They're intentionally not prominently exposed because most users don't need them—they exist for edge cases like migrating existing tables, preserving data during backfills, or controlling schema inference behavior.

**Important:** Feature flags have specific caveats and trade-offs. Contact [Estuary support](mailto:support@estuary.dev) before enabling any feature flag to ensure it's appropriate for your use case.

## Setting Feature Flags

Some connectors expose feature flags directly in the web app under **Endpoint Config → Advanced → Feature Flags**. For connectors that don't expose them in the UI, or to set flags via spec files, use the `advanced.feature_flags` field as a comma-separated string:

```yaml
materializations:
  acmeCo/my-materialization:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-postgres:dev
        config:
          address: localhost:5432
          database: mydb
          user: flow_user
          password: secret
          advanced:
            feature_flags: "allow_existing_tables_for_new_bindings,retain_existing_data_on_backfill"
```

## Materialization Feature Flags

### allow_existing_tables_for_new_bindings

Allows materializations to write to tables that already exist in the destination, even for newly added bindings.

- **Default:** Disabled. New bindings fail if the target table already exists.
- **Use case:** Migrating data from another system into Estuary-managed tables, or re-creating a materialization that was previously deleted.
- **Caveats:**
  - Enabling this flag disables load-key optimizations for affected bindings, which may impact performance. This is necessary to ensure merge operations work correctly with pre-existing data.
  - The connector cannot verify that existing table schemas are compatible.
  - This flag alone does **not** prevent backfill of the source collection. To avoid backfilling data into the existing table, also configure [`notBefore`](/concepts/materialization/#backfills) or use "Only Changes" mode on the binding.
- **Applies to:** All SQL and warehouse materializers (PostgreSQL, MySQL, Snowflake, BigQuery, Redshift, etc.)

### retain_existing_data_on_backfill

Skips truncating destination tables when a backfill is triggered.

- **Default:** Disabled. Tables are truncated at the start of a backfill to ensure consistency.
- **Use case:** Preserving historical data in the destination when a schema change triggers an automatic backfill.
- **Caveats:**
  - May result in duplicate or inconsistent data if the source collection contains updated versions of previously materialized documents.
  - If collection keys or the destination table schema change in incompatible ways, the connector will still drop and recreate the table even with this flag enabled.
- **Applies to:** Most SQL and warehouse materializers.

### datetime_keys_as_string

Converts datetime columns used as collection keys to string representation instead of native datetime types.

- **Default:** Enabled for new materializations. This is the standard behavior to preserve precision for datetime keys used as unique identifiers.
- **Use case:** Datetime values used as keys often require exact string matching. Native datetime types may lose precision or have inconsistent timezone handling, causing key mismatches.
- **Opt-out:** Use `no_datetime_keys_as_string` if you need the legacy behavior of using native datetime types for key columns.
- **Applies to:** PostgreSQL, MySQL, Snowflake, BigQuery, and other SQL materializers.

### Additional Materialization Flags

| Flag | Description | Applies To |
|------|-------------|------------|
| `uuid_logical_type` | Use UUID logical type in Parquet output instead of fixed-length byte arrays. | Parquet-based connectors (S3, GCS) |
| `objects_and_arrays_as_json` | Store nested objects and arrays as JSON strings instead of STRUCT/ARRAY types. | BigQuery |
| `snowpipe_streaming` | Use Snowpipe Streaming API for lower-latency ingestion. | Snowflake |
| `s3_use_dualstack_endpoints` | Use S3 dual-stack endpoints for IPv6 compatibility. | Redshift |

## Capture Feature Flags

The following flags control schema inference behavior for database captures. These flags were introduced when Estuary migrated to automatic schema inference. New captures have both features enabled by default. Existing captures created before this migration have `no_emit_sourced_schemas` and `no_use_schema_inference` flags set to preserve their original behavior.

### use_schema_inference / no_use_schema_inference

Enables schema inference in addition to the connector's native schema discovery.

- **Default:** Enabled for new captures. Existing captures may have `no_use_schema_inference` set.
- **Behavior:** Adds `x-infer-schema: true` to the collection schema, telling Flow to infer schema from captured documents alongside the connector's discovered column types.
- **Use case:** Automatically adapts to schema changes when new columns are added to source tables. Without this, new columns would be rejected until the capture is manually re-discovered.
- **Caveat:** May capture columns that weren't part of the original discovery. Use `no_use_schema_inference` if you need strict schema control.
- **Applies to:** All SQL database capture connectors.

### emit_sourced_schemas / no_emit_sourced_schemas

Emits schema updates to Flow during capture based on the source database's current schema.

- **Default:** Enabled for new captures. Existing captures may have `no_emit_sourced_schemas` set.
- **Behavior:** The connector periodically sends SourcedSchema messages containing the authoritative schema from the source database. Flow uses these to update collection schemas without requiring manual re-discovery.
- **Use case:** Keeps collection schemas synchronized with source table changes (new columns, type changes) automatically.
- **Caveat:** Schema changes propagate automatically, which may trigger downstream backfills in materializations if incompatible changes occur. Use `no_emit_sourced_schemas` to disable.
- **Applies to:** All SQL database capture connectors.

### Migrating to Schema Inference

If you have an existing capture with `no_emit_sourced_schemas` and `no_use_schema_inference` flags and want to migrate to the new schema inference behavior:

1. Remove `no_emit_sourced_schemas` from the feature flags
2. Save the capture and let it restart
3. Remove `no_use_schema_inference` from the feature flags
4. Edit the capture and refresh all bindings (do not backfill)
5. Save the capture and let it restart

This staged approach ensures schemas are properly synchronized before inference is enabled.

### keyless_row_id

Generates synthetic row identifiers for tables without primary keys.

- **Default:** Enabled when capturing keyless tables.
- **Use case:** Capturing data from tables that lack primary keys while maintaining document identity.
