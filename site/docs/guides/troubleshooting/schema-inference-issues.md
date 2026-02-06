---
sidebar_position: 1
---

# Fixing Schema Inference Issues

## Problem

When using schema inference with sources like Azure Blob Storage, MongoDB, or other loosely-structured systems, Estuary automatically infers field types based on observed data. Sometimes unexpected or incorrect values from upstream can cause issues:

- Bad data may cause the inferred schema to update in ways that break your materialization (e.g., a string field suddenly containing an integer, or numeric bounds exceeding destination limits)
- The inferred type may not match what your downstream systems expect
- Editing the collection schema directly doesn't stick—schema inference may overwrite your changes when new data arrives

## Solutions

### Option 1: Fix the Source Data and Backfill (`dataflow reset`)

If you can fix or remove the bad data upstream, this is the easiest option and leaves a clean dataset. Trigger a backfill with "dataflow reset" selected to re-infer the schema from scratch.

**Trade-offs:**
- You need to fix the source data first
- Depending on dataset size, this could mean a long interruption to your downstream systems while the backfill repopulates

**How to apply:**
1. Fix or remove the problematic data in your source system
2. Go to your capture in the Estuary dashboard
3. Edit the capture and select the affected binding
4. Under **Backfill**, choose **Dataflow reset**
5. Save and Publish

### Option 2: Cast to String (`castToString`)

If you can't fix the source data, the simplest workaround is to cast the field to a string at the materialization level. This works for any connector and converts the value to its string representation.

See [Custom Column Types — castToString](/guides/advanced-usage/custom-column-types#casttostring) for configuration details.

### Option 3: Custom DDL Override (`ddl`)

For more control, use the `ddl` option to specify the exact column type in the destination. The `ddl` value is passed directly to the destination database, so use syntax appropriate for your connector (e.g., `VARCHAR(255)` for Snowflake, `STRING` for BigQuery).

See [Custom Column Types — DDL](/guides/advanced-usage/custom-column-types#ddl) for configuration details and supported connectors.

## Why Schema Keeps Reverting

If you edit the collection's schema directly (under Sources → Collection), schema inference may overwrite your changes when new data arrives that doesn't match your edits.

The `ddl` and `castToString` options are applied at the **materialization** level, so they persist regardless of schema inference changes to the source collection.
