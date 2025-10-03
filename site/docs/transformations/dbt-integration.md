---
slug: /guides/dbt-integration/
---

# dbt Cloud Integration

Estuary Flow offers an integration with dbt Cloud, enabling users to trigger dbt jobs automatically when new data
is available in a materialized view. This integration provides orchestration between the data ingestion and
transformation layers, making real-time data workflows more efficient and automating data transformations.

With the dbt Cloud Job Trigger feature in Estuary Flow, you can:

- Automate transformations with dbt jobs as soon as new data is materialized, ensuring data freshness in your analytics.
- Specify custom job behavior, like replacing or skipping jobs if a trigger is already in progress.
- Define a custom "cause" message to add context for each triggered job.

The integration can be configured when creating or editing a Materialization.

## How to Configure dbt Cloud Integration

Follow these steps to configure the dbt Cloud Job Trigger within an Estuary Flow materialization connector:

### Required Parameters

To configure the dbt Cloud Job Trigger, you’ll need the following information:

- Access URL: The dbt access URL can be found in your dbt Account Settings. Use this URL if your dbt account requires a
  specific access endpoint. For more information, visit go.estuary.dev/dbt-cloud-trigger. If you have not yet migrated
  to the new API, your Access URL is: https://cloud.getdbt.com/
- Job ID: The unique identifier for the dbt job you wish to trigger.
- Account ID: Your dbt account identifier.
- API Key: The dbt API key associated with your account. This allows Estuary Flow to authenticate with dbt Cloud and
  trigger jobs.

### Optional Parameters

- Cause Message: Set a custom message that will appear as the "cause" for each triggered job. This is useful for
  tracking the context of each run, especially in complex workflows. If left empty, it defaults to "Estuary Flow."
- Job Trigger Mode:
    - skip: Skips the trigger if a job is already running (default).
    - replace: Cancels any currently running job and starts a new one.
    - ignore: Initiates a new job regardless of any existing jobs.
- Run Interval: Defines the interval at which the dbt job should run. This interval only triggers if new data has been
  materialized. The default is 30m (30 minutes).

## Use Cases

### Regular Data Transformation on New Data

In scenarios where data arrival may be delayed (for example, a materialization connector's `Sync Frequency` is set to
`1hr`), the dbt Cloud Job Trigger mechanism in Estuary Flow is designed to ensure transformations are consistent without
overwhelming the dbt job queue. Here’s how the process works:

1. Connector Initialization: When the connector starts, it immediately triggers a dbt job. This initial job ensures that
   data is consistent, even if the connector has restarted.

2. Materializing Initial Data: The connector materializes an initial chunk of data and then starts a timer, set to
   trigger the dbt job in a specified interval (`Run Interval`).

3. Handling Subsequent Data Chunks: The connector continues materializing the remaining data chunks and, after
   completing the initial data load, starts a scheduled delay (e.g., 1 hour if no new data is arriving).

4. dbt Job Trigger Timing: The dbt job triggers once the set interval (e.g., 30 minutes) has passed from the initial
   timer, regardless of whether there is backfilled data.

    1. If the data arrival is sparse or infrequent, such as once per day, the default 30-minute interval allows for
       timely but controlled job triggers without excessive job runs.
    2. During periods without backfilling, the 30-minute interval provides a balance—triggering jobs at regular
       intervals while avoiding rapid job initiation and reducing load on the dbt Cloud system.

7. Minimizing Latency: The `Run Interval` ensures that the dbt job runs shortly after the first bulk of data is
   committed, without triggering too frequently, particularly during backfills.

Defaulting to a lower (e.g. 30-minute) interval—supports various use cases, such as cases where connectors don’t use
`Sync Interval` or where data arrival is infrequent.

### Job Management

The default behaviour is to avoid triggering multiple overlapping dbt jobs, set Job Trigger Mode to skip. This way, if a
job is already running, the trigger will not start a new job, helping you manage resources efficiently.

Alternatively, if you need each transformation job to run regardless of current jobs, set Job Trigger Mode to ignore to
initiate a new dbt job each time data is materialized.
