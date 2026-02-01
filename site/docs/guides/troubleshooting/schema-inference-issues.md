---
sidebar_position: 1
---

# Fixing Schema Inference Issues from Bad Upstream Data

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

If you can't fix the source data, the simplest workaround is to cast the field to a string at the materialization level:

```json
{
  "source": "your/collection/path",
  "resource": {
    "schema": "PUBLIC",
    "table": "your_table"
  },
  "fields": {
    "recommended": true,
    "include": {
      "problematic_field": {
        "castToString": true
      }
    }
  }
}
```

This works for any connector and converts the value to its string representation.

### Option 3: Custom DDL Override (`ddl`)

For more control, use the `ddl` option to specify the exact column type in the destination:

```json
{
  "source": "your/collection/path",
  "resource": {
    "schema": "PUBLIC",
    "table": "your_table"
  },
  "fields": {
    "recommended": true,
    "include": {
      "problematic_field": {
        "ddl": "VARCHAR(255)"
      }
    }
  }
}
```

The `ddl` value is passed directly to the destination database, so use syntax appropriate for your connector (e.g., `VARCHAR(255)` for Snowflake, `STRING` for BigQuery).

## How to Apply Options 2 & 3

1. Go to your materialization in the Estuary dashboard
2. Click **Edit** → **Spec Editor** (advanced mode)
3. Find the binding for your collection
4. Add the `fields.include` section with your field configuration
5. **Save and Publish**

**Tip:** First "require" the field in the UI, then add the `ddl` or `castToString` option in the spec editor.

## Why Schema Keeps Reverting

If you edit the collection's schema directly (under Sources → Collection), schema inference may overwrite your changes when new data arrives that doesn't match your edits.

The `ddl` and `castToString` options are applied at the **materialization** level, so they persist regardless of schema inference changes to the source collection.

## Supported Connectors

The `ddl` override is supported by SQL-based materialization connectors including:
- Snowflake
- BigQuery
- Redshift
- PostgreSQL
- MySQL
- SQL Server
- Databricks
