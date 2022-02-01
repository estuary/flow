# Logs and statistics

Flow collects logs and statistics of catalog tasks to aid in debugging and refinement of your workflows.

:::caution
Access to statistics is still a work in progress. For now, this documentation deals strictly with logs.
:::

## Logs

Each organization, or tenant, that uses Flow has a `logs` collection under the global `ops` prefix.
For the tenant acmeCo, it would have the name `ops/acmeCo/logs`.

These can be thought of as standard application logs:
they store information about events that occur at runtime.
They’re distinct from [recovery logs](./shards.md#recovery-logs), which track the state of various task shards.

Regardless of how many Flow catalogs your organization has, all logs are stored in the same collection,
which is read-only and [logically partitioned](./projections.md#logical-partitions) on [tasks](../README.md#tasks).
Logs are collected from events that occur within the Flow runtime,
as well as the capture and materialization [connectors](../connectors.md) your catalog is using.

### Log level

You can set the log level for each catalog task to control the level of detail at which logs are collected for that task.
The available levels, listed from least to most detailed, are:

* `error`: Non-recoverable errors from the Flow runtime or connector that are critical to know about
* `warn`: Errors that can be re-tried, but likely require investigation
* `info`: Task lifecycle events, or information you might want to collect on an ongoing basis
* `debug`: Details that will help debug an issue with a task
* `trace`: Maximum level of detail that may yield gigabytes of logs

The default log level is `info`. You can change a task’s log level by adding the `shards` keyword to its definition in the catalog spec:

```yaml
materializations:
  acmeCo/debugMaterialization:
    shards:
      logLevel: debug
    endpoint:
        {}
```

To learn more about working with logs and statistics,
see their [reference documentation](../../../reference/working-logs-stats/).