# Session Context

**Status:** PR #2648 ready for merge

## Session Summary (2026-02-02)

Created 4 new documentation pages covering advanced Estuary features:

1. **Document Metadata Fields** - Explains `_meta` fields added by captures (uuid, op, source) with connector-specific details for PostgreSQL, MySQL, SQL Server, Oracle, MongoDB, and Snowflake.

2. **Feature Flags** - Documents materialization flags (`allow_existing_tables_for_new_bindings`, `retain_existing_data_on_backfill`, `datetime_keys_as_string`) and capture schema inference flags. Added explicit JSON paths for programmatic modification by LLMs.

3. **Custom Column Types** - Documents `castToString` and `DDL` field configurations for overriding default column type mappings.

4. **Schema Evolution Troubleshooting** - Comprehensive guide covering write vs read schemas, auto-inference behavior, NULL values in new columns, schema complexity limits (1000/10000 fields), and common error scenarios.

Reorganized pages from Advanced Features to Using Estuary > Advanced Usage for better discoverability.

## Active Work

- Merge PR #2648: https://github.com/estuary/flow/pull/2648
- All 11 commits are pushed and build passes

## Key Files

- `guides/advanced-usage/metadata-fields.md`
- `guides/advanced-usage/feature-flags.md`
- `guides/advanced-usage/custom-column-types.md`
- `guides/troubleshooting/schema-inference-issues.md`
- `concepts/schema-evolution.md` (expanded)

## Branch Info

- Working branch: `james-doc-updates` (PR #2648)
- Project tracking: `james-projects`
