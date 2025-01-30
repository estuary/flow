# Materialization sync schedule

For some systems you might prefer to have data sync'd less frequently to reduce
compute costs in the destination if some delay in new data is acceptable. For
example, if the destination system has a minimum compute charge per-query, you
could reduce your compute charges by running a single large query every 30
minutes rather than many smaller queries every few seconds.

:::note
Syncing data less frequently to your destination system does _not_ affect the
cost for running the materialization connector within Estuary Flow. But it can
reduce the costs incurred in the destination from the actions the connector
takes to load data to it.
:::

These materialization connectors support configuring a sync schedule:
- [materialize-bigquery](Connectors/materialization-connectors/BigQuery.md)
- [materialize-databricks](Connectors/materialization-connectors/databricks.md)
- [materialize-redshift](Connectors/materialization-connectors/amazon-redshift.md)
- [materialize-snowflake](Connectors/materialization-connectors/Snowflake.md)
- [materialize-starburst](Connectors/materialization-connectors/starburst.md)

## How transactions are used to sync data to a destination

Estuary Flow processes data in
[transactions](../concepts/advanced/shards.md#transactions). Materialization
connectors use the [materialization
protocol](Connectors/materialization-protocol.md) to process transactions and
sync data to the destination.

When a materialization is caught up to its source collections, it runs frequent
small transactions to keep the destination up to date. In this case every new
transaction contains the latest data that needs updated. But when a
materialization is backfilling its source collections, it runs larger
transactions to efficiently load the data in bulk to the destination and catch
up to the latest changes.

The sync schedule is configured in terms of these **transactions**: For less
frequent updates, processing of additional transactions is delayed by some
amount of time. This extra delay is only applied when the materialization is
fully caught up - backfills always run as fast as possible. And while a
transaction is delayed, Estuary Flow will continue batching and combining new
documents so that the next transaction contains all of the latest data.

You can read about [how continuous materialization
works](../concepts/materialization.md#how-continuous-materialization-works) for
more background information.

## Configuring a sync schedule

A materialization can be configured to run on a fixed schedule 24/7 or it can
have a faster sync schedule during certain times of the day and on certain days
of the week. The following options are available for configuring the sync
schedule:

| Property               | Title                  | Description                                                                                                                                                                                                                                                                                                                                                                   | Type   |
|------------------------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| `/syncFrequency`       | Sync Frequency         | Frequency at which transactions are executed when the materialization is fully caught up and streaming changes. May be enabled only for certain time periods and days of the week if configured below; otherwise it is effective 24/7. Defaults to 30 minutes if unset.                                                                                                       | string |
| `/timezone`            | Timezone               | Timezone applicable to sync time windows and active days. Must be a valid IANA time zone name or +HH:MM offset.                                                                                                                                                                                                                                                               | string |
| `/fastSyncStartTime`   | Fast Sync Start Time   | Time of day that transactions begin executing at the configured Sync Frequency. Prior to this time transactions will be executed more slowly. Must be in the form of '09:00'.                                                                                                                                                                                                 | string |
| `/fastSyncStopTime`    | Fast Sync Stop Time    | Time of day that transactions stop executing at the configured Sync Frequency. After this time transactions will be executed more slowly. Must be in the form of '17:00'.                                                                                                                                                                                                     | string |
| `/fastSyncEnabledDays` | Fast Sync Enabled Days | Days of the week that the configured Sync Frequency is active. On days that are not enabled, transactions will be executed more slowly for the entire day. Examples: 'M-F' (Monday through Friday, inclusive), 'M,W,F' (Monday, Wednesday, and Friday), 'Su-T,Th-S' (Sunday through Tuesday, inclusive; Thursday through Saturday, inclusive). All days are enabled if unset. | string |

:::warning
Changes to a [materialization's
specification](../concepts/materialization.md#specification) are only applied
after the materialization task has completed and acknowledged all of its
outstanding transactions. This means that if a task is running with a 4 hour
sync frequency, it may take up to 8 hours for a change to the specification to
take effect: 4 hours for the "current" transaction to complete and be
acknowledged, and another 4 hours for the next "pipelined" commit to complete
and be acknowledged.

If you are making changes to a materialization with a **Sync Schedule**
configured and would like those changes to take effect immediately, you can
disable and then re-enable the materialization.
:::

#### Example: Sync data on a fixed schedule

To use the same schedule for syncing data 24/7, set the value of **Sync
Frequency** only and leave the other inputs empty. For example, you might set a
**Sync Frequency** of `15m` to always have your destination sync every 15 minutes
instead of the default 30 minutes.

:::tip
If you want the materialization to always push updated data as fast as possible,
use a **Sync Frequency** of `0s`.
:::

#### Example: Sync data faster during certain times of the day

If you only care about having the most-up-to-date data possible during certain
times of the day, you can set a start and stop time for that time period. The
value you set for **Sync Frequency** will be used during that time period;
otherwise syncs will be performed every 4 hours.

The **Fast Sync Start Time** and **Fast Sync Stop Time** values must be set as
24-hour times, and you must provide a value for **Timezone** that this time
window should use. Timezones must either be [a valid IANA time zone
name](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) or a +HH:MM
offset. Providing a time zone name will ensure local factors like daylight
savings time are considered for the schedule, whereas an offset timezone is
always relative to UTC.

An example configuration data syncs data as fast as possible between the hours
of 9:00AM and 5:00PM in the Eastern Time Zone (ET) would use these values:
- **Sync Frequency**: `0s`
- **Timezone**: `America/New_York`
- **Fast Sync Start Time**: `09:00`
- **Fast Sync Stop Time**: `17:00`

#### Example: Sync data faster only on certain days of the week

You can also set certain days of the week that the fast sync is active. On all
other days, data will be sync'd more slowly all day.

To enable this, set values for **Sync Frequency**, **Timezone**, **Fast Sync
Start Time**, and **Fast Sync Stop Time** as you would for syncing data faster
during certain times of the day, and also provide a value for **Fast Sync
Enabled Days**.

**Fast Sync Enabled Days** is a range of days, where the days of the week are
abbreviated as `(Su)nday`, `(M)onday`, `(T)uesday`, `(W)ednesday`, `(Th)ursday`,
`(F)riday`, `(S)aturday`.

Here are some examples of valid inputs for **Fast Sync Enabled Days**:
- `M-F` to enable fast sync on Monday through Friday.
- `Su, T, Th, S` to enable fast sync on Sunday, Tuesday, Thursday, and Saturday.
- `Su-M,Th-S` to enable fast sync on Thursday through Monday. Note that the days
  of the week must be listed in order, so `Th-M` will not work.

## Timing of syncs

In technical terms, timing of syncs is controlled by the materialization
connector sending a transaction acknowledgement to the Flow runtime at computed
times. Practically this means that at these times the prior transaction will
complete and have its statistics recorded, and the next transaction will begin.

This timing is computed so that it occurs at predictable instants in time. As a
hypothetical example, if you have set a **Sync Frequency** of `15m`, transaction
acknowledgements might be sent at times like `00:00`, `00:15`, `00:30`, `00:45`
and so on, where each acknowledgement is sent at a multiple of the **Sync
Frequency** relative to the hour. This means that if the materialization [task
shard](../concepts/advanced/shards.md) restarts and completes its first
transaction at `00:13`, it will run its next transaction at `00:15` rather than
`00:28`.

In actuality these computed points in time have some amount of
[jitter](https://en.wikipedia.org/wiki/Jitter) applied to them to avoid
overwhelming the system at common intervals, so setting a **Sync Frequency** to
a specific value will ensure that transactions are predictably acknowledged that
often, but makes no assumptions about precisely what time instants the
acknowledgements will occur.

:::info
The `jitter` value is deterministic based on the *compute resource* for the
destination system from the materialization's endpoint configuration. How this
compute resource is identified varies for different systems, but is usually
something like `"account_name" + "warehouse_Name"`.

This means that separate materializations using the same compute resource will
synchronize their usage of that compute resource if they have the same **Sync
Schedule** configured.
:::
