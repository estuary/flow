---
sidebar_position: 8
---
# Storage mappings

Flow stores the documents that comprise your collections in a cloud storage bucket.
Your **storage mapping** tells Flow which bucket to use.

When you first register for Flow, your storage mapping is Estuary's secure Google Cloud Storage bucket.
Data in Flow's cloud storage bucket is deleted 20 days after collection.

For production workflows, you should [set up your own cloud storage bucket as a storage mapping](../guides/configure-cloud-storage.md).

You may also use apply different storage mappings to different [catalog prefixes](./catalogs.md#namespace) within your organization's prefix.

You can set up a bucket lifecycle policy to manage data retention in your storage mapping;
for example, to remove data after six months.

## Recovery logs

In addition to collection data, Flow uses your storage mapping to temporarily store **recovery logs**.

Flow tasks — captures, derivations, and materializations — use recovery logs to durably store their processing context as a backup.
Recovery logs are an opaque binary log, but may contain user data.

The recovery logs of a task are always prefixed by `recovery/`,
so a task named `acmeCo/produce-TNT` would have a recovery log called `recovery/acmeCo/roduce-TNT`

Flow prunes data from recovery logs once it is no longer required.

:::warning
Deleting data from recovery logs while it is still in use can
cause Flow processing tasks to fail permanently.
:::