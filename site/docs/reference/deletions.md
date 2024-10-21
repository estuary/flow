# Handling Deletions in Estuary Flow

Estuary supports two categories of deletions: **soft deletes** and **hard deletes**. These deletion types determine how documents are marked and treated within the system. Below is an explanation of each category.

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
