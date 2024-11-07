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

- Job ID: The unique identifier for the dbt job you wish to trigger.
- Account ID: Your dbt account identifier.
- API Key: The dbt API key associated with your account. This allows Estuary Flow to authenticate with dbt Cloud and
  trigger jobs.

### Optional Parameters

- Access URL: The dbt access URL can be found in your dbt Account Settings. Use this URL if your dbt account requires a
  specific access endpoint. For more information, visit go.estuary.dev/dbt-cloud-trigger.
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

Suppose you have a data pipeline that ingests data into a warehouse every 1 hour (configured via a Sync Frequency),
and you want to run a dbt job on the same cadence to transform this data. Configure the `Run Interval` to `1h` to ensure
that the dbt job is triggered automatically after every data ingestion cycle.

### Job Management

If you want to avoid triggering multiple overlapping dbt jobs, set Job Trigger Mode to skip. This way, if a job is
already running, the trigger will not start a new job, helping you manage resources efficiently.

Alternatively, if you need each transformation job to run regardless of current jobs, set Job Trigger Mode to ignore to
initiate a new dbt job each time data is materialized.
