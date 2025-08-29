---
sidebar_position: 3
slug: /reference/working-logs-stats/
---

# Working with Logs and Statistics

Your [`logs` and `stats` collections](/concepts/advanced/logs-stats)
are useful for debugging and monitoring catalog tasks.

## Accessing logs and statistics

You can access logs and statistics in the Flow web app, by materializing them to an external endpoint, or from the command line.

### Logs and statistics in the Flow web app

You can view a subset of logs and statistics for individual tasks in the Flow web app.

#### Logs

After you publish a new [capture](/guides/create-dataflow/#create-a-capture) or [materialization](/guides/create-dataflow/#create-a-materialization), a pop-up window appears that displays the task's logs.
Once you close the window, you can't regain access to the full logs in the web app.
For a complete view of logs, use [flowctl](#accessing-logs-and-statistics-from-the-command-line).

However, if a task fails, you can view the logs associated with the error(s) that caused the failure.
In the **Details** view of the published capture or materialization, click the name of its shard to display the logs.

#### Statistics

Two statistics are shown for each capture, collection, and materialization:

* **Bytes Written or Read**.
This corresponds to the `bytesTotal` [property of the stats collection](#transaction-information).
* **Docs Written or Read**.
This corresponds to the `docsTotal` [property of the stats collection](#transaction-information).

These fields have slightly different meanings for each Flow entity type:

* For captures, **Bytes Written** and **Docs Written** represent the total data written across all of the
capture's associated collections.
* For collections, **Bytes Written** and **Docs Written** represent the data written to the collection from
its associated capture or derivation.
* For materializations, **Bytes Read** and **Docs Read** represent the total data read from all of the
materialization's associated collections.

### Accessing logs and statistics from the command line

:::caution
The flowctl stats subcommand has been temporarily moved under `flowctl raw` while we build a new and improved version. In the meantime, `flowctl raw stats` functionality may be experimental. If you need any help, please reach out to us via [Slack](https://go.estuary.dev/slack) or [email](mailto:support@estuary.dev).
:::

The `flowctl logs` and `flowctl stats` subcommands allow you to print logs and stats, respectively, from the command line.
This method allows more flexibility and is ideal for debugging.

You can retrieve logs and stats for any published Flow task. For example:

```console
flowctl logs --task acmeCo/anvils/capture-one

flowctl stats --task acmeCo/anvils/capture-one --uncommitted
```

:::info Beta
The `--uncommitted` flag is currently required for `flowctl stats`. This means that all statistics are read, regardless of whether they are about a successfully committed [transaction](/concepts/advanced/shards/#transactions), or a transaction that was rolled back or uncommitted.
In the future, committed reads will be the default.
:::

#### Printing logs or stats since a specific time

To limit output, you can retrieve logs are stats starting at a specific time in the past. For example:

```
flowctl stats --task acmeCo/anvils/materialization-one --since 1h
```

...will retrieve stats from approximately the last hour. The actual start time will always be at the previous [fragment](/concepts/advanced/journals/#fragment-files) boundary, so it can be significantly before the requested time period.

Additional options for `flowctl logs` and `flowctl stats` can be accessed through command-line help.

## Available statistics

Available statistics include information about the amount of data in inputs and outputs of each transaction. They also include temporal information about the transaction. Statistics vary by task type (capture, materialization, or derivation).

A thorough knowledge of Flow's [advanced concepts](/concepts/#advanced-concepts) is necessary to effectively leverage these statistics.

`stats` collection documents include the following properties.

### Shard information

A `stats` document begins with data about the shard processing the transaction.
Each processing shard is uniquely identified by the combination of its `name`, `keyBegin`, and `rClockBegin`.
This information is important for tasks with multiple shards: it allows you to determine whether data throughput is
evenly distributed amongst those shards.

| Property | Description | Data Type | Applicable Task Type |
|---|---|---|---|
| `/shard` | Flow shard information| object | All |
| `/shard/kind` | The type of catalog task. One of `"capture"`, `"derivation"`, or `"materialization"` | string | All |
| `/shard/name` | The name of the catalog task (without the task type prefix) | string | All |
| `/shard/keyBegin` | With `rClockBegin`, this comprises the shard ID. The inclusive beginning of the shard's assigned key range.  | string | All |
| `/shard/rClockBegin` | With `keyBegin`, this comprises the shard ID. The inclusive beginning of the shard's assigned rClock range.  | string | All |

### Transaction information

`stats` documents include information about a transaction: its inputs and outputs,
the amount of data processed, and the time taken.
You can use this information to ensure that your Flow tasks are running efficiently,
and that the amount of data processed matches your expectations.

| Property | Description | Data Type | Applicable Task Type |
|---|---|---|---|
| `/ts` | Timestamp corresponding to the start of the transaction, rounded to the nearest minute | string | All |
| `/openSecondsTotal` | Total time that the transaction was open before starting to commit | number | All |
| `/txnCount` | Total number of transactions represented by this stats document. Used for reduction. | integer | All |
| `/capture` | Capture stats, organized by collection | object | Capture |
| `/materialize` | Materialization stats, organized by collection | object | Materialization |
| `/derive` | Derivation statistics | object | Derivation |
| `/<task-type>/<collection-name>/right/`| Input documents from a the task's source | object | Capture, materialization |
| `/<task-type>/<collection-name>/left/`| Input documents from an external destination; used for [reduced updates](/concepts/materialization/#how-continuous-materialization-works) in materializations | object | Materialization |
| `/<task-type>/<collection-name>/out/`| Output documents from the transaction | object | All |
| `/<task-type>/{}/docsTotal` | Total number of documents| integer| All |
| `/<task-type>/{}/bytesTotal` | Total number of bytes representing the JSON encoded documents | integer | All |
| `/derivations/transforms/transformStats` | Stats for a specific transform of a derivation, which will have an update, publish, or both | object | Derivation |
| `/derivations/transforms/transformStats/input` | The input documents that were fed into this transform | object | Derivation |
| `/derivations/transforms/transformStats/update` | The outputs from update lambda invocations, which were combined into registers | object | Derivation |
| `/derivations/transforms/transformStats/publish` | The outputs from publish lambda invocations. | object | Derivation |
| `/derivations/registers/createdTotal` | The total number of new register keys that were created | integer | Derivation |
