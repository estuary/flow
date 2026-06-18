---
sidebar_position: 3
description: Customize Estuary materialized column types with castToString or DDL field configuration for precise control and to match destination-specific data types.
---

# Customize Materialized Column Types

Estuary automatically maps collection fields to appropriate column types in your destination. However, some scenarios require overriding these defaults. The `castToString` and `DDL` field configurations give you precise control over how individual fields are materialized.

## When to Use Custom Column Types

Common scenarios include:

- **Precision requirements**: Numeric fields that exceed the destination's native integer precision
- **Custom database types**: Using destination-specific types like `DECIMAL(20,5)` or `JSONB`
- **String representation**: Forcing fields to be stored as strings for compatibility with downstream systems
- **Computed columns**: Advanced DDL with expressions or constraints

## Configuration

### Via the Web App

1. Navigate to your materialization and click **Edit Specification**
2. Scroll down to the **Advanced Spec Editor**
3. Find the relevant binding by its `source` collection name
4. Add the field configuration inside the binding's `fields.include` section

:::note
`include` requires the field to exist in the **collection schema**—if the field is removed from the schema, the materialization will fail to publish. However, individual documents don't need to have the field: if a document is missing the field or it's null, the destination column will contain `NULL` for that row.
:::

For example, to cast a field to string:

```json
{
  "fields": {
    "recommended": true,
    "include": {
      "CampaignLineItemId": {
        "castToString": true
      }
    }
  }
}
```

Or to set a custom DDL:

```json
{
  "fields": {
    "recommended": true,
    "include": {
      "your_field": {
        "ddl": "NUMBER(14,6)"
      }
    }
  }
}
```

5. Click **Save and Publish**

### Via YAML

Custom column types are configured per-field in the materialization's `fields` stanza.

**Path:** `materializations.<name>.bindings[].fields.include.<fieldName>`

```yaml
materializations:
  acmeCo/my-materialization:
    bindings:
      - source: acmeCo/my-collection
        resource: { table: my_table }
        fields:
          recommended: true
          include:
            revenue: { castToString: true }
            large_integer: { DDL: "DECIMAL(38,0)" }
```

To add custom column types to an existing binding, locate the binding by its `source` collection name, then add or merge into the `fields.include` object.

### castToString

Actively converts a field's value to its string representation before writing to the destination.

```yaml
fields:
  include:
    myField: { castToString: true }
```

**Use cases:**
- Large integer IDs that exceed 64-bit precision (barcodes, external system IDs)
- High-precision or nanosecond timestamps that need exact preservation
- Numeric fields with values exceeding 64-bit integer range
- Fields that may contain multiple types (numbers, strings, objects)
- Compatibility with systems that expect string-formatted numbers

**Behavior:**
- Values are actively converted to strings: numbers → `"3"`, booleans → `"true"`, objects → `"{\"foo\":\"bar\"}"`
- The destination column is created as a string/text type
- Original type information is preserved in the collection; only the materialized representation changes

**Connector support:** Most SQL and warehouse materialization connectors support `castToString`. However, **Elasticsearch**, **MongoDB**, and **DynamoDB** connectors do not support this option.

### DDL

Specifies custom DDL (Data Definition Language) for the column definition.

```yaml
fields:
  include:
    myField: { DDL: "DECIMAL(20,5)" }
```

**Use cases:**
- Specifying exact numeric precision and scale
- Using destination-specific column types not automatically selected
- Storing complex data as JSON (e.g., `JSON` in BigQuery, `JSONB` in PostgreSQL)
- Adding column constraints or defaults
- Creating computed or generated columns (where supported)

**Behavior:**
- The provided DDL string replaces the automatically generated column type
- The connector uses this DDL verbatim when it creates the column—on initial table creation or when the table is dropped and recreated. It is not used to `ALTER` a column that already exists (see [Changing DDL on an existing column](#changing-ddl-on-an-existing-column))
- **Important:** DDL only changes the column definition—it does NOT transform the data. The burden is on you to ensure the field's data is compatible with the specified column type.
- When you specify custom DDL, the connector disables its normal type validation for that field

**Examples by destination:**

| Destination | Example DDL | Description |
|-------------|-------------|-------------|
| PostgreSQL | `NUMERIC(38,10)` | High-precision decimal |
| PostgreSQL | `JSONB` | Binary JSON storage |
| Snowflake | `NUMBER(14,6)` | Decimal with specific precision |
| BigQuery | `BIGNUMERIC` | Arbitrary precision decimal |
| BigQuery | `JSON` | Native JSON column type |
| MySQL | `DECIMAL(65,30)` | Maximum precision decimal |

**Connector support:** DDL is supported by most SQL and warehouse materialization connectors. The **Iceberg** connector does not support DDL; use the `ignoreStringFormat` option instead for similar functionality.

## Combining Options

You can use `castToString` and `DDL` together, though this is rarely needed:

```yaml
fields:
  include:
    myField: { castToString: true, DDL: "VARCHAR(100)" }
```

When combined:
- The value is first converted to a string
- The string is stored in a column with your custom DDL

## Troubleshooting

### Changing DDL on an existing column

Custom `DDL` is applied **only when the column is created**—on initial table creation or when the table is dropped and recreated. It is never used to `ALTER` a column that already exists.

This matters when you add or change a `DDL` override on a binding whose destination table already exists. [Backfilling the binding](/reference/backfilling-data/#materialization-backfill) does **not** necessarily apply the new type. When the existing columns are still type-compatible with the collection, the backfill runs `TRUNCATE TABLE`, which clears the rows but keeps the existing schema. Because the field's underlying type is unchanged—for example, narrowing `LONGTEXT` to `VARCHAR(255)` or changing numeric precision—the backfill detects no incompatible change and stays on the truncate path, so the new column type is not applied. Any `additional_table_create_sql` (such as an index) does not re-run either, since it only executes when the table is created.

To apply the new DDL, force the table to be dropped and recreated:

1. **Use the `always_drop_tables_on_backfill` feature flag (recommended).**
   1. Add [`always_drop_tables_on_backfill`](./feature-flags.md#always_drop_tables_on_backfill) to the materialization's `advanced.feature_flags`
   2. Keep your `DDL` override
   3. [Backfill the binding](/reference/backfilling-data/#materialization-backfill)

   The connector drops the table, recreates it with the new column type (running any `additional_table_create_sql`), and repopulates the data. Remove the flag afterward to restore the faster truncate behavior; the custom column type and any indexes survive later truncates.
2. **Manually alter or drop the table.** Use your destination's tools to `ALTER` the column type (or `DROP` the table), then [backfill the binding](/reference/backfilling-data/#materialization-backfill) so the data is repopulated.

See [Schema changes during backfill](/reference/backfilling-data/#schema-changes-during-backfill) for the full set of conditions that cause a backfill to drop and recreate versus truncate.

:::caution
When recreating a column with a length-limited type such as `VARCHAR(n)`, make sure `n` exceeds the longest value in that column, or the backfill will fail when it tries to store a value that is too long.
:::

### Invalid DDL errors

If your custom DDL is invalid for the destination:

1. Check the destination's documentation for valid column type syntax
2. Ensure any referenced types or constraints exist in your destination
3. Test the DDL manually in your destination before applying

### Data truncation

When using `castToString` with `DDL` that limits string length:

1. Ensure your DDL length accommodates the longest possible string representation
2. Consider using unlimited length types like `TEXT` if unsure

## Related

- [Customize Materialized Fields](/guides/customize-materialization-fields/) - Field selection basics
- [Feature Flags](./feature-flags.md) - Additional connector configuration options
