# Handling Deletions in Estuary Flow

Estuary supports two categories of deletions: **soft deletes** and **hard deletes**. These deletion types determine how documents are marked and treated within the system. Below is an explanation of each category.

Delete events contain a very limited set of fields compared to create and update events.
Only the primary key and meta information are present. The `_meta/op` field will be set to `d`.
For example:

```json
{
  "_meta": {
    "op": "d",
    "source": {
      "loc": [
        516715804872,
        516715805272,
        516715805488
      ],
      "schema": "public",
      "table": "shipments",
      "ts_ms": 1758800000000,
      "txid": 390000000
    },
    "uuid": "abc-123-def-456"
  },
  "id": 1234
}
```

## Soft Deletes

Soft deletes occur when a document is marked as deleted but is not physically removed from the destination. Instead, it is flagged for deletion with a specific metadata field.

- **Flagging Soft Deletes**: The field `_meta/op` is set to `'d'` to indicate that a document has been marked for deletion.
- **Document Retention**: The document remains in the destination even though it is marked as deleted.
- **Filtering Soft Deleted Documents**: To exclude soft-deleted documents from queries, you can filter out documents where `_meta/op = 'd'`. This ensures that soft-deleted documents are ignored without permanently removing them.

## Hard Deletes

Hard deletes go a step further than soft deletes by permanently removing documents from the destination.

- **Flagging Hard Deletes**: Similar to soft deletes, the `_meta/op` field is set to `'d'` for documents that need to be deleted.
- **Document Removal**: Once a document is flagged, a query is issued to physically remove any document marked with `_meta/op = 'd'` from the destination.
- **Supported Materialization Connectors for Hard Deletes**:
  - Snowflake
  - Google BigQuery
  - Databricks
  - Amazon Redshift
  - Elastic
  - PostgreSQL
  - MySQL
  - SQL Server
  - AlloyDB
  - MongoDB
  - MotherDuck
  - TimescaleDB

## Deletions and Derivations

When working with derived collections, since you are filtering and transforming the original data collection, you must take care to ensure delete events are still passed along to the materialization.

There are a couple best practices to consider when writing derivations in conjunction with deletions.

**Filtering**

Since delete events lack most document fields, it can be easy to accidentally filter them out from downstream systems.

You may therefore want to explicitly check the `_meta/op` field as part of any filtering statement in your transformation logic.
For example:

```sql
WHERE $_meta$op != 'd'
AND $created_at > '2025-01-01'
```

**Hard deletions as reduction annotations**

Deletes may sometimes appear without an associated create event for the ID, such as when using multiple filter conditions in your derivation.
If a delete event is the first time Flow has seen a specific ID, the ID will not be properly passed along for hard deletion in the materialization.
This is because Flow uses [**reductions**](/concepts/#reductions) to handle hard deletes.
Essentially, if there isn't already a document for that ID, there is nothing for the new document to _reduce into_.

To handle this edge case, you should ensure there is more than one document for the ID so the deletion reduction can take place.
A simple solution is to emit a delete event twice in your transformation.
These delete event emissions should all be contained in the same lambda statement to avoid further orphaned deletions.

For example, consider this SQL lambda statement:

```sql
-- Perform your desired transformation on the data when the event is not 'd'
SELECT
  $id,
  JSON($flow_document)
WHERE $_meta$op != 'd';

-- In case we run across IDs we haven't seen before, ensure there is already
-- a document in place so that delete reductions will propagate correctly
SELECT $id, $_meta where $_meta$op = 'd';
SELECT $id, $_meta where $_meta$op = 'd';
```
