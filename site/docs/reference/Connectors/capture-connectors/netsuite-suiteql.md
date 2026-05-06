
# Netsuite SuiteQL (Beta)

This connector captures data from Oracle NetSuite into Estuary collections using SuiteQL.

To use SuiteAnalytics Connect to sync your data instead, see the [Netsuite SuiteAnalytics connector](./netsuite-suiteanalytics.md).

## Supported data resources

Currently, this connector supports a subset of NetSuite tables, such as:

* Account
* Customer
* DeletedRecord
* Item
* Term
* Transaction
* TransactionHistory
* TransactionLine
* TransactionShippingAddress
* TransactionStatus

If you need to capture a table that is not yet supported, [contact support](mailto:support@estuary.dev) with the table name(s).
Estuary support will be able to confirm availability and, if needed, add the table(s) to the connector.

## Sync modes and data loading

The SuiteQL connector is **full-refresh only** — it does not support incremental change capture. The SuiteQL REST API does not expose the metadata required for incremental replication (no equivalent of SuiteAnalytics' `OA_TABLES`/`OA_COLUMNS`/`OA_FKEYS`), so each binding is re-read from scratch on every run. Use the [`schedule`](#bindings) cron expression to control how often a binding re-runs and picks up new and changed rows.

If incremental sync is required, use the [SuiteAnalytics connector](./netsuite-suiteanalytics.md) instead.

### Paginated backfill vs. snapshot

The connector picks a mode per binding based on whether the table has a configured key:

- **Paginated backfill** — Tables with a key (specified in the endpoint's `tables` config) are read in ordered pages using a `page_cursor` (defaults to the first key field). Each scheduled run starts a new full backfill.
- **Snapshot** — Tables with no key (for example, `Account`, `DeletedRecord`, `transactionHistory`, `TransactionStatus`) are read as a single query that returns the entire table. Snapshots run on the binding's `interval` (defaulting to once a day) and use `/_meta/row_id` as the collection key.

### Delete handling

The SuiteQL connector does **not** read NetSuite's `DeletedRecord` table to correlate deletions to other collections. (You can capture `DeletedRecord` itself as a snapshot table for raw access to the deletion log, but the connector won't apply those deletions to other bindings.)

- **Snapshot bindings** detect deletions automatically by comparing each run's `/_meta/row_id` set to the previous run's. Any row that disappears is emitted as a deletion.
- **Paginated-backfill bindings** do not infer deletions. Rows that are removed from NetSuite remain in the destination until the next scheduled backfill runs, and even then are not deleted automatically — schedule periodic backfills and run a downstream cleanup query that removes rows older than the most recent backfill start time.

## API constraints

SuiteQL has several hard API limits that shape how the connector operates. Plan your bindings with these in mind:

- **No metadata introspection.** Unlike the SuiteAnalytics ODBC driver, SuiteQL has no way to programmatically discover table schemas, primary keys, or foreign keys. The connector relies on a static list of supported tables and keys; if you need a table that isn't supported, [contact support](mailto:support@estuary.dev).
- **100-column limit.** SuiteQL silently returns zero rows for any query that selects more than 100 columns. To capture wide tables, set the binding's [`columns`](#bindings) field to an explicit list of 100 or fewer columns.
- **100,000-row result limit.** SuiteQL caps query results at 100,000 rows. The connector works around this with paginated subqueries, but each successive page re-scans previously read rows, so very large tables (tens of millions of rows or more) become impractical.
- **Date-only timestamps.** SuiteQL returns date-time columns as date-only strings. The connector emits these as-is — hour, minute, and second information is not available.

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

#### 2. Enable SuiteQL

1. Navigate to **Setup** > **Company** > **Enable Features**.

2. Click the **SuiteCloud** tab.

3. In the **Manage Authentication** section, check the checkbox labeled **TOKEN-BASED AUTHENTICATION**.

4. Save your changes.

5. Next, in the **SuiteTalk (Web Services)** section, check the checkbox labeled **REST WEB SERVICES**.

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

## Configuration

You configure connectors either in Estuary's web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the NetSuite source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/account` | Netsuite Account ID | Netsuite realm/Account ID e.g. 2344535, as for `production` or 2344535_SB1, as for `sandbox` | string | Required |
| `/authentication` | Authentication Details | Credentials to access your NetSuite account | object | Required |
| `/tables` | Tables | List of tables to capture with their keys | array of objects, each containing a string name and array of keys | Defaults to all supported data resources |
| `/advanced` | Advanced | Any advanced options to use for the connector | object |  |

#### Authentication Config

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/authentication/consumer_key` | Consumer Key | Consumer key associated with your integration. | string | Required |
| `/authentication/consumer_secret` | Consumer Secret | Consumer secret associated with your integration. | string | Required |
| `/authentication/token_id` | Token ID | Access token ID | string | Required |
| `/authentication/token_secret` | Token Secret | Access token secret | string | Required |

#### Advanced Config options

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/advanced/connection_limit` | Connection Limit | The maximum number of concurrent data streams to NetSuite. | int | `2` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/name` | Name | The name of the table this binding refers to | string | Required |
| `/interval` | Interval | How often to re-run the binding. Applies to **snapshot** bindings only — paginated-backfill bindings ignore `interval` and run on `schedule` instead. | [`ISO8601` Duration](https://www.digi.com/resources/documentation/digidocs/90001488-13/reference/r_iso_8601_duration_format.htm) | `P1D` (1 day) for snapshots; ignored for paginated bindings |
| `/schedule` | Schedule | Cron expression that triggers a periodic re-backfill. Applies to **paginated-backfill** bindings only — snapshots manage their own cadence via `interval`. | string | `0 0 * * *` (daily at midnight UTC) for paginated bindings; empty for snapshots |
| `/page_cursor` | Page Cursor | Cursor field for paginated backfills | string | Defaults to first key field |
| `/columns` | Columns | List of columns to select. Empty means `SELECT *` (fails silently if >100 columns). | string array | `[]` |
| `/query_limit` | Query Limit | Number of rows to fetch per query page | int | `10000` |

### Sample

```yaml
captures:
   ${PREFIX}/${CAPTURE_NAME}:
      endpoint:
         connector:
            image: ghcr.io/estuary/source-netsuite-suiteql:dev
               config:
                  account: "12345678"
                  authentication:
                     consumer_key: xxx
                     consumer_secret_sops: xxx
                     token_id: xxx
                     token_secret_sops: xxx
                  tables:
                     - name: transaction
                       keys: [id]
                  advanced:
                     connection_limit: 2
      bindings:
         - resource:
            name: transaction
            schedule: "0 0 * * *"
            page_cursor: id
            columns: []
            query_limit: 10000
         target: ${PREFIX}/${CAPTURE_NAME}/transaction
    {...}
```
