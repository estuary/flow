# Logs and Statistics

Flow collects logs and statistics of catalog tasks to aid in debugging and refinement of your workflows.

## Logs

Each organization that uses Flow has a `logs` collection under the global `ops` prefix.
For the organization Acme Co, it would have the name `ops/acmeCo/logs`.

These can be thought of as standard application logs:
they store information about events that occur at runtime.
They’re distinct from [recovery logs](./shards.md#recovery-logs), which track the state of various task shards.

Regardless of how many Data Flows your organization has, all logs are stored in the same collection,
which is read-only and [logically partitioned](./projections.md#logical-partitions) on [tasks](../README.md#tasks).
Logs are collected from events that occur within the Flow runtime,
as well as the capture and materialization [connectors](../connectors.md) your Data Flow is using.

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
## Statistics

Each organization that uses Flow has a `stats` collection under the global `ops` prefix.
For the organization Acme Co, it would have the name `ops/acmeCo/stats`.

Regardless of how many Data Flows your organization has, all stats are stored in the same collection,
which is read-only and [logically partitioned](./projections.md#logical-partitions) on [tasks](../README.md#tasks).

A new document is published to the `stats` collection for each task transaction.
Each document includes information about the time and quantity of data inputs and outputs.
Statistics vary by task type (capture, materialization, or derivation).

Use stats to:

* Evaluate the data throughput of a task; for example, a derivation.
* Compare a data throughput of a task between platforms; for example, compare reported data capture by Flow to detected change rate in a source system.
* Access the same information used by Estuary for billing.
* Optimize your tasks for increased efficiency.

[See a detailed table of the properties included in `stats` documents.](/reference/working-logs-stats/#available-statistics)

## Working with logs and statistics

[Learn more about working with logs and statistics](/reference/working-logs-stats)