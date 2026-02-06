---
sidebar_position: 1
---

# Document Metadata Fields

Captures automatically add a `_meta` object to every document they produce. This metadata tracks change operations, source location, and ordering information essential for understanding where data came from and how to process it correctly.

## Standard Fields

All captures include these standard `_meta` fields:

| Field | Type | Description |
|-------|------|-------------|
| `_meta.uuid` | string | V1 UUID uniquely identifying the document |
| `_meta.op` | string | Operation type: `"c"` (create), `"u"` (update), `"d"` (delete) |
| `_meta.source` | object | Source-specific metadata (varies by connector) |

### _meta.uuid and flow_published_at

Every document has a `_meta.uuid` field containing a v1 UUID. This UUID:
- Uniquely identifies each document globally
- Contains an encoded timestamp of when the document was published to the collection

The `flow_published_at` projection is automatically derived from the UUID's timestamp component. It's available in every collection and useful for:
- Tracking when documents were last modified
- Incremental processing in materializations (via time travel `notBefore`/`notAfter`)
- Ordering events chronologically

### Delete Events

Delete events (`op: "d"`) contain only the document key and `_meta` fields—other fields are omitted. When processing deletes, use the key to identify which document was removed. See the [deletions guide](/reference/deletions/) for handling soft and hard deletes.

## Common Source Fields (SQL Captures)

Most SQL database captures share these `_meta.source` fields:

| Field | Type | Description |
|-------|------|-------------|
| `ts_ms` | integer | Unix timestamp in milliseconds when the event was recorded |
| `schema` | string | Database schema name |
| `table` | string | Table name |
| `snapshot` | boolean | `true` if from initial backfill, `false` if from replication log |
| `tag` | string | Custom source tag (if configured in Advanced Options) |

## Connector-Specific Source Fields

### PostgreSQL

Additional fields beyond common:

| Field | Type | Description |
|-------|------|-------------|
| `loc` | array | `[lastCommitEndLSN, eventLSN, currentBeginFinalLSN]` for WAL ordering |
| `txid` | integer | PostgreSQL transaction ID |

### MySQL

Additional fields beyond common:

| Field | Type | Description |
|-------|------|-------------|
| `cursor` | string | Binlog position as `binlog_file:binlog_offset:row_index` |
| `txid` | string | Global transaction ID (if GTIDs enabled) |

### SQL Server (CDC)

Additional fields beyond common:

| Field | Type | Description |
|-------|------|-------------|
| `lsn` | string | Log sequence number in format `00000000:00000000:0001` |
| `seqval` | string | Base64-encoded sequence value for ordering within transaction |
| `updateMask` | string | Bit mask of updated columns |

### SQL Server (Change Tracking)

Additional fields beyond common:

| Field | Type | Description |
|-------|------|-------------|
| `version` | integer | Change Tracking version number |

### Oracle

Additional fields beyond common:

| Field | Type | Description |
|-------|------|-------------|
| `scn` | integer | System Change Number |
| `row_id` | string | Oracle ROWID |
| `rs_id` | string | Record Set ID |
| `ssn` | integer | SQL Sequence Number |

### MongoDB

MongoDB uses a different structure with `db` and `collection` instead of `schema` and `table`:

| Field | Type | Description |
|-------|------|-------------|
| `db` | string | Database name |
| `collection` | string | Collection name |
| `snapshot` | boolean | `true` if from backfill, `false` if from change stream |

MongoDB also supports `_meta.before` containing the pre-image of documents (if pre-images are enabled in the source database).

### Snowflake

Additional fields beyond common:

| Field | Type | Description |
|-------|------|-------------|
| `seq` | integer | Sequence number of staging table |
| `off` | integer | Offset within staging table |

## Example Document

Here's an example document from a PostgreSQL capture showing the complete `_meta` structure:

```json
{
  "id": 12345,
  "name": "Example Record",
  "updated_at": "2024-01-15T10:30:00Z",
  "_meta": {
    "op": "u",
    "source": {
      "ts_ms": 1705315800000,
      "schema": "public",
      "table": "records",
      "snapshot": false,
      "loc": [516715804872, 516715805272, 516715805488]
    }
  }
}
```

## Use Cases

### Filtering by Operation Type

In derivations, filter documents by operation type to handle creates, updates, and deletes differently:

```sql
-- Only process creates and updates, ignore deletes
SELECT * FROM source_collection WHERE _meta.op != 'd'
```

### Ordering Events by Source Timestamp

Use `_meta.source.ts_ms` to order events by when they occurred in the source system:

```sql
SELECT * FROM events ORDER BY _meta.source.ts_ms
```

### Identifying Source Table

In multi-table captures, use schema and table to route documents:

```sql
-- Process only records from the orders table
SELECT * FROM capture WHERE _meta.source.table = 'orders'
```

### Using Source Tags

When capturing from multiple database replicas, configure different tags in Advanced Options to distinguish them:

```sql
-- Filter by source tag
SELECT * FROM capture WHERE _meta.source.tag = 'us-east-replica'
```

### Distinguishing Backfill from Real-time

Use the `snapshot` field to handle backfilled historical data differently from real-time changes:

```sql
-- Only process real-time changes
SELECT * FROM capture WHERE _meta.source.snapshot = false
```
