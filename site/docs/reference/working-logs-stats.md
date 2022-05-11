# Working with logs and statistics

Your [`logs` and `stats` collections](../concepts/advanced/logs-stats.md)
are useful for debugging and monitoring catalog tasks.

:::info Beta
Access to statistics is still a work in progress. For now, this documentation deals strictly with logs.
:::

## Accessing logs

You can access logs in the web application, by materializing them to an external endpoint, or from the command line.

### Accessing logs in the web application

The Flow web application displays logs for each running catalog task.
Navigate to the **Captures** and **Materializations** tabs and select the task of interest.

### Accessing logs from the command line

:::info Beta
The `flowctl logs` subcommand is currently under construction.
Contact [Estuary Support](mailto:support@estuary.dev) for more information.
:::

The `flowctl logs` subcommand allows you to print logs from the command line.
This method allows more flexibility and is ideal for debugging.

You can retrieve logs for any task that is part of a catalog that is currently deployed.

#### Printing logs for a specific task

You can print logs for a given deployed task using the flag `--task` followed by the task name.

```console
flowctl logs --task acmeCo/anvils/capture-one
```

#### Printing all logs for a tenant

You can print all logs for currently deployed catalogs of a given tenant using the flag `--tenant`.

```console
flowctl logs --tenant acmeCo
```

This is the same as printing the entire contents of the collection `ops/acmeCo/logs`.

#### Printing logs by task type

Within a given tenant, you can print logs for all deployed tasks of a given type using the flag `--task-type` followed by one of `capture`, `derivation`, or `materialization`.

```console
flowctl logs --tenant acmeCo --task-type capture
```

### Accessing logs by materialization

You can materialize your `logs` collection to an external system.
This is typically the preferred method if you’d like to continuously work with or monitor logs.
It's easiest to materialize the whole collection, but you can use a [partition selector](../../concepts/materialization/#partition-selectors) to only materialize specific tasks, as the `logs` collection is partitioned on tasks.

:::caution
Be sure to add a partition selector to exclude the logs of the materialization
itself. Otherwise, you could trigger an infinite loop in which the connector
materializes its own logs, logs that event, and so on.
:::

```yaml
acmeCo/anvils/logs:
  endpoint:
    connector:
      image: ghcr.io/estuary/materialize-webhook:dev
      config:
        address: my.webhook.com
  bindings:
    - resource:
        relativePath: /log/wordcount
      source: ops/acmeCo/logs
      # Exclude the logs of this materialization to avoid an infinite loop.
      partitions:
        exclude:
          name: ['acmeCo/anvils/logs']
```
