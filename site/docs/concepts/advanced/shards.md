# Task shards

:::tip
Task shards are an advanced concept of Flow.
You can use Flow without knowing the details of shards,
but this section may help you better understand how Flow works.
:::

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
Later and as required, a shard can be split into two shards.
Once initiated, the split may require up to a few minutes to complete,
but it doesn't require downtime and the selected shard continues
to run until the split occurs.

This process can be repeated as needed until your required throughput is achieved.

:::caution TODO
This section is incomplete.
See `flowctl shards split --help` for further details.
:::

## Recovery logs

:::info
Shard stores and associated states are transparent to you, the Flow user.
This section is informational only, to provide a sense of how Flow works.
:::

All task shards have associated state, which is managed in the shard's store.

* Capture tasks must track incremental checkpoints of their endpoint connectors.
* Derivation tasks manage a potentially very large index of registers,
  as well as read checkpoints of sourced collection journals.
* Materialization tasks track incremental checkpoints of their endpoint connectors,
  as well as read checkpoints of sourced collection journals.

Shard stores use
[recovery logs](https://gazette.readthedocs.io/en/latest/consumers-concepts.html#recovery-logs)
to replicate updates and implement transaction semantics.

Recovery logs are regular [journals](/workspaces/flow/site/docs/concepts/advanced/journals.md),
but hold binary data and are not intended for direct use.
However, they can hold your user data.
Recovery logs of [derivations](../derivations.md) hold your derivation register values.

Recovery logs are stored in your cloud storage bucket,
and must have a configured [storage mapping](../storage-mappings.md#recovery-logs).