# Task shards

Catalog [tasks](../README.md#tasks) — captures, derivations, and materializations —
are executed by one or more task **shards**.

Shards are a fault-tolerant and stateful unit of execution for a catalog task,
which the Flow runtime assigns and runs on a scalable pool of compute resources.
A single task can have many shards,
which allow the task to scale across many machines to
achieve more throughput and parallelism.

Shards are part of the Gazette project.
[See Gazette's Shard concepts page for details](
https://gazette.readthedocs.io/en/latest/consumers-concepts.html#shards).

## Shard splits

When a task is first created, it is initialized with a single shard.
Later and as required, shards may be split into two shards.
This is done by the service operator on your behalf, depending on the size of your task.
Shard splitting doesn't require downtime; your task will continue to run as normal
on the old shard until the split occurs and then shift seamlessly to the new, split shards.

This process can be repeated as needed until your required throughput is achieved.
If you have questions about how shards are split for your tasks, contact your Estuary account representative.

## Transactions

Shards process messages in dynamic **transactions**.

Whenever a message is ready to be processed by the task (when new documents appear at the source endpoint or collection),
a new transaction is initiated.
The transaction will continue so long as further messages are available for processing.
When no more messages are immediately available, the transaction closes.
A new transaction is started whenever the next message is available.

In general, shorter transaction durations decrease latency, while longer transaction durations
increase efficiency.
Flow automatically balances these two extremes to optimize each task,
but it may be useful in some cases to control transaction duration.
For example, materializations to large analytical warehouses may benefit from longer transactions,
which can reduce cost by performing more data reduction before landing data in the warehouse.
Some endpoint systems, like [BigQuery](../../reference/materialization-sync-schedule.md), limit the number of table operations you can perform.
Longer transaction durations ensure that you don't exceed these limits.

You can set the minimum and maximum transaction duration in a task's [shards configuration](../../reference/Configuring-task-shards.md).

## Recovery logs

All task shards have associated state, which is managed in the shard's store.

* Capture tasks must track incremental checkpoints of their endpoint connectors.
* Derivation tasks manage a potentially very large index of registers,
  as well as read checkpoints of sourced collection journals.
* Materialization tasks track incremental checkpoints of their endpoint connectors,
  as well as read checkpoints of sourced collection journals.

Shard stores use
[recovery logs](https://gazette.readthedocs.io/en/latest/consumers-concepts.html#recovery-logs)
to replicate updates and implement transaction semantics.

Recovery logs are regular [journals](./journals.md),
but hold binary data and are not intended for direct use.
However, they can hold your user data.
Recovery logs of [derivations](../derivations.md) hold your derivation register values.

Recovery logs are stored in your cloud storage bucket,
and must have a configured [storage mapping](../storage-mappings.md#recovery-logs).