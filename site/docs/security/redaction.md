
# Redaction

Redaction is a security feature in Estuary that allows you to block or hash fields from a capture during write time, _before_ documents are fully captured.
Adding redaction prevents fields or unhashed values from being written to disk or surfacing in error messages.

Essentially, it is the earliest point in Estuary when you can remove fields, compared against other options like [derivations](/concepts/derivations) and customizing materialization schemas with [field selection](/guides/customize-materialization-fields/#field-selection-for-materializations).

This makes it an essential tool when working with PII or other sensitive data.

## Redaction Strategies

Estuary lets you choose how you would like to handle redacted fields with different _redaction strategies_.

Redaction strategies include:

* **Block**

   Removes fields entirely from documents.

   Blocking fields is useful when none of your downstream systems need to access any information about the field or its value.

   For example, completely removing a field containing a user SSN (which ideally should already be encrypted in the source system to begin with).

* **Hash**

   Replaces values with SHA-256 hashed versions of the values.

   Hashing fields can be useful when you would like to include stand-in values for sensitive fields in downstream systems but don't want those systems or system users to have access to the unhashed value.

   For example, hashing a user email so analysts can still compile information about a user journey without seeing the user's PII.

## How to Use Redaction

You can redact fields using Estuary's web application. Redaction is surfaced as part of the [capture](/concepts/captures) process.

1. Start by creating a new capture or editing an existing one.

2. In the **Target Collections** section of the capture configuration, find the list of **Bindings**.

3. Select a binding whose fields you would like to edit.

4. On the right-hand side of the table, select the binding's **Collection** tab.

5. Review the **Schema** table. The table provides all available fields and their types, along with an **Actions** column.

6. Click the **Redact** button in the Actions column for any field you would like to redact.

7. Select your desired [redaction strategy](#redaction-strategies) and click **Apply**.

Redacted fields will display a lock icon next to the field name.
Hovering over the lock will indicate which redaction strategy applies to the field.

You can change your redaction configuration later as compliance strategies evolve: simply select the field's **Redact** button again.

If the field no longer needs to be redacted, click the **X** button by the redaction strategy to clear it.
Or you may instead update your chosen redaction strategy. In either case, click **Apply** to save your changes.

### Redacting properties in specification files

Estuary handles redaction via JSON schema annotations.
When managing Estuary resources directly via their specification files, you can therefore redact fields using `redact` annotations.

To avoid conflicts with schema inference and discovery, however, these annotations must be applied to properties at the _top level_ of the write schema, outside the connector or inferred schema.

Properties in this top-level write schema can include a `redact` annotation with a valid [`strategy`](#redaction-strategies): either `block` to remove the field or `sha256` to hash it.
You do not need to include top-level properties for all collection fields, only ones you wish to annotate.

An example collection specification would therefore look like:

```json
{
  "writeSchema": {
    "$defs": {
      "flow://connector-schema": {...}
    },
    "$ref": "flow://connector-schema",
    "properties": {
      "ssn": {
        "redact": {
          "strategy": "block"
        }
      },
      "email": {
        "redact": {
          "strategy": "sha256"
        }
      }
    }
  },
  "readSchema": {...}
}
```
