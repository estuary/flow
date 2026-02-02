---
sidebar_position: 3
---

# Custom Column Types

Estuary automatically maps collection fields to appropriate column types in your destination. However, some scenarios require overriding these defaults. The `castToString` and `DDL` field configurations give you precise control over how individual fields are materialized.

## When to Use Custom Column Types

Common scenarios include:

- **Precision requirements**: Numeric fields that exceed the destination's native integer precision
- **Custom database types**: Using destination-specific types like `DECIMAL(20,5)` or `JSONB`
- **String representation**: Forcing fields to be stored as strings for compatibility with downstream systems
- **Computed columns**: Advanced DDL with expressions or constraints

## Configuration

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

**Connector support:** Most SQL and warehouse materializers support `castToString`. However, **Elasticsearch**, **MongoDB**, and **DynamoDB** connectors do not support this option.

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
- The connector uses this DDL verbatim when creating or altering the column
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

**Connector support:** DDL is supported by most SQL and warehouse materializers. The **Iceberg** connector does not support DDL; use the `ignoreStringFormat` option instead for similar functionality.

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

## Configuring via the Web App

You can also configure these options through Estuary's web application:

1. Navigate to your materialization and click **Edit**
2. Select the binding you want to modify
3. In the **Field Selection** table, find the field you want to customize
4. Click the field's configuration button to access advanced options
5. Set `castToString` or provide custom `DDL` as needed
6. Click **Save and Publish**

## Troubleshooting

### Column type mismatch errors

If you change the DDL for an existing column, the connector may report a type mismatch. Options:

1. **Backfill the binding**: This recreates the table with the new column type
2. **Manually alter the column**: Use your destination's tools to modify the column type, then republish

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
