---
sidebar_position: 1
---
# Storage mappings

Flow stores the documents that comprise your collections in a cloud storage bucket.
Your **storage mapping** tells Flow which bucket to use.

Every Flow organization (defined by its [catalog prefix](../catalogs.md#namespace)) has a storage mapping defined during setup.
When you're provisioned a prefix, your Estuary account manager will help you [set up your storage mapping](../../getting-started/installation.md#configuring-your-cloud-storage-bucket-for-use-with-flow).
If you have a trial account, your storage mapping is Estuary's secure Google Cloud Storage bucket.

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