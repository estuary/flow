import ReactPlayer from "react-player";

# NetSuite SuiteAnalytics Connect

This connector captures data from Oracle NetSuite into Estuary collections. It uses the SuiteAnalytics Connect feature in order to both load large amounts of data quickly, as well as introspect the available tables, their schemas, keys, and cursor fields.

In general, SuiteAnalytics Connect is the preferred method to retrieve data from NetSuite.
However, if you don't have SuiteAnalytics, see the [SuiteQL connector](./netsuite-suiteql.md) instead.

<ReactPlayer controls url="https://www.youtube.com/watch?v=CN3RXry0o9k" />

## Supported data resources

Estuary discovers all of the tables to which you grant access during [setup](#setup), including `Transactions`, `Reports`, `Lists`, and `Setup`.

## Sync modes and data loading

Each table binding uses one of two sync modes, determined by the cursor fields available on the table.

### Incremental sync

Tables with a date-time column suitable for change tracking (configured via [`log_cursor`](#bindings)) are synced **incrementally**. Most base record tables — such as `Transaction`, `Account`, `Customer`, `Employee`, `Item`, `Subsidiary`, and `Vendor` — have a `lastmodifieddate` column that the connector discovers automatically.

During incremental sync, the connector queries only for rows modified since the last checkpoint. The polling frequency is controlled by the [`interval`](#bindings) setting (default: 1 hour).

**How to tell if a table is incremental:** Check the binding's `log_cursor` field. If it's set (e.g., `lastmodifieddate`), the table is incremental.

### Full refresh

Tables without a `log_cursor` are synced via **full refresh** — the connector re-reads the entire table on each sync. This is common for linking and junction tables (e.g., `TransactionLine`, `NextTransactionLineLink`) that lack a `lastmodifieddate` column.

There are two full refresh modes:

- **Paginated backfill** — Uses the [`page_cursor`](#bindings) to read the table in ordered pages. Pair this with a [`schedule`](#setting-a-schedule) cron expression to control how often the full refresh runs (e.g., daily).
- **Snapshot backfill** — Set [`snapshot_backfill: true`](#bindings) when no good page cursor exists and the table is small enough for a single query. Snapshot mode manages its own schedule via the `interval` field. Do **not** combine `snapshot_backfill` with a cron `schedule` — this will cause issues with delete emission.

:::tip Switching from full refresh to incremental
If a table has a `lastmodifieddate` column (or similar date-time field) that isn't currently configured as the `log_cursor`, you can add it to enable incremental sync. After updating the binding, trigger a backfill to re-read the table from the new cursor's starting point.
:::

### Delete handling

Estuary captures deletions from NetSuite using the **DeletedRecord** system table, which NetSuite maintains to track when records are removed.

**Base record tables** (such as `Transaction`, `Account`, `Customer`, `Subsidiary`, `Currency`, `CurrencyRate`, `Department`, `Employee`, `Item`, `Vendor`, etc.) support delete tracking. When a record is deleted in NetSuite, it appears in the `DeletedRecord` table, and Estuary emits a deletion event to the corresponding collection.

**Linking and junction tables** (such as `TransactionLine`, `TransactionAccountingLine`, `NextTransactionLineLink`, `NextTransactionAccountingLineLink`) do **not** support delete tracking via `DeletedRecord`. NetSuite does not track deletions for these table types. If a linking table is configured for full refresh (snapshot mode), rows that no longer appear in the source are detected as removals during the next full read.

### Table associations

Linking tables can optionally be loaded as **associations** of a parent table instead of — or in addition to — standalone bindings. For example, `TransactionAccountingLine` can be associated with `Transaction` so that when a transaction is modified, its related accounting lines are also loaded.

See the [`associations`](#table-associations) binding property for configuration details.

## Prerequisites

- Oracle NetSuite [account](https://system.netsuite.com/pages/customerlogin.jsp?country=US)
- Allowed access to all Account permissions options
- A new integration with token-based authentication
- A custom role with access to objects you want to capture. See [setup](#setup).
- A new user assigned to the custom role
- Access token generated for the custom role

## Setup

#### 1. Create a NetSuite account

1. Create an account on the [Oracle NetSuite](https://www.netsuite.com/portal/home.shtml) portal.

2. Confirm your email address.

#### 2. Enable SuiteAnalytics Connect

1. Navigate to **Setup** > **Company** > **Enable Features**.

2. Click the **SuiteCloud** tab.

3. In the **Manage Authentication** section, check the checkbox labeled **TOKEN-BASED AUTHENTICATION**.

4. Save your changes.

5. Next, navigate to **Setup** > **Company** > **Analytics** > **Connectivity** and check the checkbox labeled **SuiteAnalytics Connect**.

6. Save your changes.

#### 3. Find Your Account ID

Find your _Realm_, or Account ID. You'll use this to connect with Estuary.

   1. In your NetSuite portal, go to **Setup** > **Company** > **Company Information**.

   2. Copy your Account ID.

      If you have a production account, it will look like `2345678`. If you're using a sandbox, it'll look like `2345678_SB2`.

#### 4. Generate Consumer Tokens

Create a NetSuite _integration_ to obtain a Consumer Key and Consumer Secret.

   1. Navigate to **Setup** > **Integration** > **Manage Integrations** > **New**.

   2. Give the integration a name, for example, `estuary-rest-integration`.

   3. Make sure the **State** option is enabled.

   4. In the **Authentication** section, check the **Token-Based Authentication** checkbox.

   5. Save your changes.

   Your Consumer Key and Consumer Secret will be shown once. Copy them to a safe place.

#### 5. Set Up a Custom Role

   1. Go to **Setup** > **Users/Roles** > **Manage Roles** > **New**.

   2. Give the role a name, for example, `estuary-integration-role`.

   3. Scroll to the **Permissions** section.

   4. (IMPORTANT) Click **Transactions** and add all the dropdown entities with either **full** or **view** access level.

   5. (IMPORTANT) Click **Reports** and add all the dropdown entities with either **full** or **view** access level.

   6. (IMPORTANT) Click **Lists** and add all the dropdown entities with either **full** or **view** access level.

   7. (IMPORTANT) Click **Setup** and add all the dropdown entities with either **full** or **view** access level.

   8. (IMPORTANT) If you have multiple subsidiaries, make sure to select all of the subsidiaries you want the connector to have access to under the **Role** > **Subsidiary Restrictions** configuration.

   To allow your custom role to reflect future changes, be sure to edit these parameters again when you rename or customize any NetSuite object.

#### 6. Assign the Role to a User

   1. Go to **Setup** > **Users/Roles** > **Manage Users**.

   2. Find the user you want to give access to use with Estuary. In the **Name** column, click the user's name. Then, click the **Edit** button.

   3. Find the **Access** tab.

   4. From the dropdown list, select the role you created previously; for example, `estuary-integration-role`.

   5. Save your changes.

#### 7. Generate User Access Tokens

   1. Go to **Setup** > **Users/Roles** > **Access Tokens** > **New**.

   2. Select an **Application Name**.

   3. Under **User**, select the user you assigned the role previously.

   4. Under **Role**, select the role you assigned to the user previously.

   5. Under **Token Name**, give a descriptive name to the token you are creating, for example `estuary-rest-integration-token`.

   6. Save your changes.

   Your Token ID and Token Secret will be shown once. Copy them to a safe place.

You now have a properly configured account with the correct permissions and all the information you need to connect with Estuary:

- Realm (Account ID)
- Consumer Key
- Consumer Secret
- Token ID
- Token Secret

:::info
You can also authenticate with a username and password, but a consumer/token is recommended for security.
:::

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the NetSuite source connector.

### Properties

#### Endpoint

| Property                      | Title                  | Description                                                                                      | Type   | Required/Default |
| ----------------------------- | ---------------------- | ------------------------------------------------------------------------------------------------ | ------ | ---------------- |
| `/account`                     | Netsuite Account ID    | Netsuite realm/Account ID e.g. 2344535, as for `production` or 2344535_SB1, as for `sandbox` | string | Required         |
| `/suiteanalytics_data_source` | Data Source            | Which NetSuite data source to use. This should generally be `NetSuite2.com`                | string | Required         |
| `/authentication`             | Authentication Details | Credentials to access your NetSuite account                                                      | object | Required         |
| `/authentication/auth_type`   | Authentication Type    | Type of authentication used, either `token` or `user_pass`.                                      | string | `token`          |

##### Token/Consumer Authentication

| Property                          | Title           | Description                                       | Type   | Required/Default |
| --------------------------------- | --------------- | ------------------------------------------------- | ------ | ---------------- |
| `/authentication/consumer_key`    | Consumer Key    | Consumer key associated with your integration.    | string | Required         |
| `/authentication/consumer_secret` | Consumer Secret | Consumer secret associated with your integration. | string | Required         |
| `/authentication/token_id`       | Token ID       | Access token key                                  | string | Required         |
| `/authentication/token_secret`    | Token Secret    | Access token secret                               | string | Required         |

##### Username/Password Authentication

| Property                   | Title    | Description                            | Type   | Required/Default |
| -------------------------- | -------- | -------------------------------------- | ------ | ---------------- |
| `/authentication/username` | Username | Your NetSuite account's email/username | string | Required         |
| `/authentication/password` | Password | Your NetSuite account's password.      | string | Required         |
| `/authentication/role_id`  | Role ID  | The ID of the role you created. Defaults to 3, which is the ID of the administrator role.        | int    | `3`              |

##### Advanced Config options

| Property                     | Title            | Description                                                                                                                                                                                                         | Type | Required/Default |
| ---------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---- | ---------------- |
| `/advanced/connection_limit` | Connection Limit | The maximum number of concurrent data streams to attempt at once.                                                                                                                                                   | int  | 10 Connections   |
| `/advanced/task_limit`       | Task Limit       | The maximum number of concurrent tasks to run at once. A task is either a backfill or incremental load. Backfills can load multiple chunks in parallel, so this must be strictly &lt;= `/advanced/connection_limit` | int  | 5 Tasks          |
| `/advanced/start_date`       | Start Date       | The date that we should attempt to start backfilling from. If not provided, backfill from the beginning.                                                                                                            | date | Not Required     |
| `/advanced/query_idle_timeout_seconds` | Query Idle Timeout | Maximum time to wait for the next row during query execution. Query will timeout if no rows are received within this duration. | [`ISO8601` Duration](https://www.digi.com/resources/documentation/digidocs/90001488-13/reference/r_iso_8601_duration_format.htm) | `PT30M` |

#### Bindings

| Property                                    | Title                   | Description                                                                                                                           | Type                                                                                                                             | Required/Default                    |
| ------------------------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `/name`                                     | Name                    | The name of the table this binding refers to                                                                                          | string                                                                                                                           | Required                            |
| `/interval`                                 | Interval                | How frequently to check for incremental changes                                                                                       | [`ISO8601` Duration](https://www.digi.com/resources/documentation/digidocs/90001488-13/reference/r_iso_8601_duration_format.htm) | `PT1H` (1 Hour)                     |
| `/schedule`         | Schedule     | [Schedule](#setting-a-schedule) to automatically rebackfill this binding. Accepts a cron expression.      | string |   |
| `/log_cursor`                               | Log Cursor              | A date-time column to use for incremental capture of modifications.                                                                   | String                                                                                                                           | Required (Automatically Discovered) |
| `/page_cursor`                              | Page Cursor             | An indexed, non-NULL integer column to use for ordered table backfills. Does not need to be unique, but should have high cardinality. | String                                                                                                                           | Required (Automatically Discovered) |
| `/concurrency`                              | Concurrency             | Maximum number of concurrent connections to use for backfilling.                                                                      | int                                                                                                                              | 1 Connection                        |
| `/query_limit`                              | Query Limit             | Maximum number of rows to fetch in a query. Will be divided between all connections if `/concurrency` > 1                             | int                                                                                                                              | 100,000 Rows                        |
| `/select_columns`         | Manually Selected Columns     | Override the columns to load from the table. If empty, all columns will be loaded. Ideally this should only be set when loading specific columns is necessary, as it won't automatically update when new columns are added or removed. | string array | `[]`  |
| `/snapshot_backfill`     | Single-shot Backfill           | Attempt to backfill using a single-shot query to load all rows. Useful when no good page cursor exists, and the table is of reasonable size. Incremental updates are still possible if a log cursor is defined. | boolean | `false` |

##### Table Associations

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/associations`                             | Associations            | List of associated tables for which related data should be loaded.                                                                    | Array[TableAssociation]                                                                                                          | []                                  |
| `/associations/[n]/child_table_name`        | Foreign Table Name      | The name of the "foreign" table that should be associated with the "parent" binding containing this association                       | String                                                                                                                           | Required                            |
| `/associations/[n]/parent_join_column_name` | Parent Join Column      | The name of the column on the "parent" table to be used as the join key                                                               | String                                                                                                                           | Required                            |
| `/associations/[n]/child_join_column_name`  | Foreign Join Column     | The name of the column on the "foreign" table to be used as the join key                                                              | String                                                                                                                           | Required                            |
| `/associations/[n]/load_during_backfill`    | Load During Backfill    | Whether or not to load associated documents during backfill                                                                           | Boolean                                                                                                                          | False                               |
| `/associations/[n]/load_during_incremental` | Load During Incremental | Whether or not to load associated documents during incremental loads                                                                  | Boolean                                                                                                                          | True                                |

##### Advanced Options

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/advanced` | Advanced | Advanced options to customize your binding. | object |  |
| `/advanced/initial_backfill_cursor` | Initial Backfill Cursor | Manually set the starting cursor value for backfill operations. If specified, the backfill will start from this cursor value instead of the table's minimum value. Useful for partial backfills or resuming from a specific point. Only applies to the initial backfill. | int |  |
| `/advanced/exclude_calculated` | Exclude Calculated Columns | Exclude calculated columns from queries. Keys and cursors are never excluded. | boolean | `false` |
| `/advanced/exclude_custom` | Exclude Custom Columns | Exclude custom columns from queries. Keys and cursors are never excluded. | boolean | `false`|
| `/advanced/exclude_hidden` | Exclude Hidden Columns | Exclude hidden columns from queries. Keys and cursors are never excluded. | boolean | `false` |
| `/advanced/exclude_non_display` | Exclude Non-Display Columns | Exclude non-display columns from queries. Keys and cursors are never excluded. | boolean | `false` |
| `/advanced/extra_columns` | Additional Columns | Columns to include even if they match exclusion criteria. Useful for selectively re-including specific columns that would otherwise be filtered out. Cannot be used with 'Manually Selected Columns'. | string array | `[]` |

### Sample

```yaml
captures:
   ${PREFIX}/${CAPTURE_NAME}:
      endpoint:
         connector:
            image: ghcr.io/estuary/source-netsuite-v2:v4
               config:
                  account: "12345678"
                  authentication:
                     auth_type: token
                     consumer_key: xxx
                     consumer_secret_sops: xxx
                     token_id: xxx
                     token_secret_sops: xxx
                  suiteanalytics_data_source: NetSuite2.com
                  advanced:
                     connection_limit: 20
                     start_date: null
                     task_limit: 10
                     query_idle_timeout_seconds: PT30M
      bindings:
         - resource:
            associations:
               -  child_join_column_name: transaction
                  child_table_name: TransactionAccountingLine
                  parent_join_column_name: id
                  load_during_backfill: false
                  load_during_incremental: true
            interval: PT5M
            name: transaction
            page_cursor: id
            query_limit: 100000
            concurrency: 1
            log_cursor: lastmodifieddate
            select_columns: []
            snapshot_backfill: false
         target: ${PREFIX}/${CAPTURE_NAME}/transaction
    {...}
```

## Special Columns

NetSuite tables can include several special column types:

* Calculated Columns
* Custom Columns
* Hidden Columns
* Non-Display Columns

Ingesting these types of column can slow down queries (calculated columns, for example, require computation on every row) or cause other blockages in the data flow.
Newly discovered bindings will therefore default to excluding calculated, custom, and hidden columns from your collections.

If your bindings allow special column types, newly discovered columns may impact your capture in the future, even if everything currently works as expected.
It can therefore be prudent to select only the subset of NetSuite special columns you need to capture.

To set exclusions for particular special column types, configure the resource's [Advanced Options](#advanced-options).

If you find these exclusions too broad, you can add back individual filtered-out fields using the resource's **Additional Columns** Advanced Option.

You can find out whether a specific column falls under one of these special types in NetSuite's column metadata under the `userdata` field.

## Setting a Schedule

Certain bindings may not be able to load data incrementally. You can instead use the `schedule` field for these bindings.
This allows you to specify a cron expression to rebackfill the binding.

This is helpful when performing periodic full refreshes using the paginated backfill mode.
Schedules are only needed when a binding has a key and page cursor, but no log cursor.

The `schedule` setting should **not** be used in conjunction with **snapshot mode**.
Snapshots manage their own state and run on a schedule set by the `interval` field.
Attempting to backfill a snapshot using a cron `schedule` will cause issues with emitting deletions.
