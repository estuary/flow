# Working with logs and statistics

Your [`logs` and `stats` collections](../concepts/advanced/logs-stats.md)
are useful for debugging and monitoring catalog tasks.

:::info Beta
Access to statistics is still a work in progress. For now, this documentation deals strictly with logs.
:::

## Accessing logs

You can access logs by materializing them to an external endpoint, or from the command line.

For illustrative purposes, the following sections use the word counts example from the [Flow tutorial](../getting-started/flow-tutorials/hello-flow.md).

### Accessing logs from the command line

:::info Beta
Enhancements to this workflow are coming soon.
:::

From the command line, you can access logs by printing the journals that comprise them.
This workflow is ideal if you require logs to debug specific catalog tasks,
as logs are divided into journals per partition, and each partition maps to a task.

Say you work for Acme Co, and you want to print logs for the materialization `materialize-word-counts`.
You need to find the name of the correct journal.
Begin by printing a list of active journals in your environment:

```console
flowctl journals list
```

Each task in the catalog will have a journal in the `logs` collection beginning with `ops/acmeCo/logs`.
Look for the name of the desired task.
In this case, the journal you’re looking for is: `ops/acmeCo/logs/kind=materialization/name=acmeCo%2Fpostgres%2Fmaterialize-word-counts/pivot=00 `

Print the contents of the journal to view the logs:

```console
flowctl journals read -l name=ops/acmeCo/logs/kind=materialization/name=acmeCo%2Fpostgres%2Fmaterialize-word-counts/pivot=00
```

### Accessing logs by materialization

You can materialize your `logs` collection to an external system.
This is typically the preferred message if you’d like to work with logs for all tasks; in other words, the entire collection.

:::caution
Be sure to add a [partition selector](../../concepts/materialization/#partition-selectors) to exclude the logs of the materialization
itself. Otherwise, you could trigger an infinite loop in which the connector
materializes its own logs, logs that event, and so on.
:::

```yaml
acmeCo/postgres/logs:
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
          name: ['acmeCo/postgres/logs']
```
