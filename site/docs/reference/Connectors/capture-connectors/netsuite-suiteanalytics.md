import ReactPlayer from "react-player";

# NetSuite SuiteAnalytics Connect

This connector captures data from Oracle NetSuite into Flow collections. It relies on the SuiteAnalytics Connect feature in order to both load large amounts of data quickly, as well as introspect the available tables, their schemas, keys, and cursor fields.

[`ghcr.io/estuary/source-netsuite:dev`](https://ghcr.io/estuary/source-netsuite:dev) provides the
latest connector image. You can also follow the link in your browser to see past image versions.

If you don't have SuiteAnalytics Connect, check out our [SuiteTalk REST](../netsuite-suitetalk) connector.

<ReactPlayer controls url="https://www.youtube.com/watch?v=CN3RXry0o9k" />

## Supported data resources

Flow discovers all of the tables to which you grant access during [setup](#setup), including `Transactions`, `Reports`, `Lists`, and `Setup`.

## Prerequisites

- Oracle NetSuite [account](https://system.netsuite.com/pages/customerlogin.jsp?country=US)
- Allowed access to all Account permissions options
- A new integration with token-based authentication
- A custom role with access to objects you want to capture. See [setup](#setup).
- A new user assigned to the custom role
- Access token generated for the custom role

## Setup

**Create a NetSuite account**

1. Create an account on the [Oracle NetSuite](https://www.netsuite.com/portal/home.shtml) portal.

2. Confirm your email address.

**Set up your NetSuite account**

1. Find your _Realm_, or Account ID. You'll use this to connect with Flow.

   1. In your NetSuite portal, go to **Setup** > **Company** > **Company Information**.

   2. Copy your Account ID.

      If you have a production account, it will look like `2345678`. If you're using a sandbox, it'll look like `2345678_SB2`.

2. Create a NetSuite _integration_ to obtain a Consumer Key and Consumer Secret.

   1. Navigate to **Setup** > **Integration** > **Manage Integrations** > **New**.

   2. Give the integration a name, for example, `estuary-rest-integration`.

   3. Make sure the **State** option is enabled.

   4. In the **Authentication** section, check the **Token-Based Authentication** checkbox. You do not need to enable "TBA: authorization flow" or "authorization code grant".

   5. Save your changes.

   Your Consumer Key and Consumer Secret will be shown once. Copy them to a safe place.

3. Set up a role for use with Flow. You can use the bundled "Data Warehouse Integrator" role, instead of creating a new role using the instructions below, if you don't want to manage custom permissions.

   1. Go to **Setup** > **Users/Roles** > **Manage Roles** > **New**.

   2. Give the role a name, for example, `estuary-integration-role`.

   3. Scroll to the **Permissions** section.

   4. (IMPORTANT) Click **Transactions** and add all the dropdown entities with either **full** or **view** access level.

   5. (IMPORTANT) Click **Reports** and add all the dropdown entities with either **full** or **view** access level.

   6. (IMPORTANT) Click **Lists** and add all the dropdown entities with either **full** or **view** access level.

   7. (IMPORTANT) Click **Setup** an add all the dropdown entities with either **full** or **view** access level.

   To allow your custom role to reflect future changes, be sure to edit these parameters again when you rename or customize any NetSuite object.

4. Set up user for use with Flow.

   1. Go to **Setup** > **Users/Roles** > **Manage Users**.

   2. Find the user you want to give access to use with Flow. In the **Name** column, click the user's name. Then, click the **Edit** button.

   3. Find the **Access** tab.

   4. From the dropdown list, select role you created previously; for example, `estuary-integration-role`.

   5. Save your changes.

5. Generate an access token.

   1. Go to **Setup** > **Users/Roles** > **Access Tokens** > **New**.

   2. Select an **Application Name**.

   3. Under **User**, select the user you assigned the role previously.

   4. Under **Role**, select the role you assigned to the user previously.

   5. Under **Token Name**, give a descriptive name to the token you are creating, for example `estuary-rest-integration-token`.

   6. Save your changes.

   Your Token ID and Token Secret will be shown once. Copy them to a safe place.

You now have a properly configured account with the correct permissions and all the information you need to connect with Flow:

- Realm (Account ID)
- Consumer Key
- Consumer Secret
- Token ID
- Token Secret

:::info
You can also authenticate with a username and password, but a consumer/token is recommended for security.
:::

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the NetSuite source connector.

### Properties

#### Endpoint

| Property                      | Title                  | Description                                                                                      | Type   | Required/Default |
| ----------------------------- | ---------------------- | ------------------------------------------------------------------------------------------------ | ------ | ---------------- |
| `/account`                     | Netsuite Account ID    | Netsuite realm/Account ID e.g. 2344535, as for `production` or 2344535_SB1, as for `sandbox` | string | Required         |
| `/role_id`                    | Role ID                | The ID of the role you created. Defaults to 3, which is the ID of the administrator role.        | int    | 3                |
| `/suiteanalytics_data_source` | Data Source            | Which NetSuite data source to use. Options are `NetSuite.com`, or `NetSuite2.com`                | string | Required         |
| `/authentication`             | Authentication Details | Credentials to access your NetSuite account                                                      | object | Required         |

#### Token/Consumer Authentication

| Property                          | Title           | Description                                       | Type   | Required/Default |
| --------------------------------- | --------------- | ------------------------------------------------- | ------ | ---------------- |
| `/authentication/consumer_key`    | Consumer Key    | Consumer key associated with your integration.    | string | Required         |
| `/authentication/consumer_secret` | Consumer Secret | Consumer secret associated with your integration. | string | Required         |
| `/authentication/token_key`       | Token Key       | Access token key                                  | string | Required         |
| `/authentication/token_secret`    | Token Secret    | Access token secret                               | string | Required         |

#### Username/Password Authentication

| Property                   | Title    | Description                            | Type   | Required/Default |
| -------------------------- | -------- | -------------------------------------- | ------ | ---------------- |
| `/authentication/username` | Username | Your NetSuite account's email/username | string | Required         |
| `/authentication/password` | Password | Your NetSuite account's password.      | string | Required         |

#### Advanced Config options

| Property                     | Title            | Description                                                                                                                                                                                                         | Type | Required/Default |
| ---------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---- | ---------------- |
| `/advanced/connection_limit` | Connection Limit | The maximum number of concurrent data streams to attempt at once.                                                                                                                                                   | int  | 10 Connections   |
| `/advanced/task_limit`       | Task Limit       | The maximum number of concurrent tasks to run at once. A task is either a backfill or incremental load. Backfills can load multiple chunks in parallel, so this must be strictly &lt;= `/advanced/connection_limit` | int  | 5 Tasks          |
| `/advanced/start_date`       | Start Date       | The date that we should attempt to start backfilling from. If not provided, backfill from the beginning.                                                                                                            | date | Not Required     |

#### Bindings

| Property                                    | Title                   | Description                                                                                                                           | Type                                                                                                                             | Required/Default                    |
| ------------------------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `/name`                                     | Name                    | The name of the table this binding refers to                                                                                          | string                                                                                                                           | Required                            |
| `/interval`                                 | Interval                | How frequently to check for incremental changes                                                                                       | [`ISO8601` Duration](https://www.digi.com/resources/documentation/digidocs/90001488-13/reference/r_iso_8601_duration_format.htm) | `PT1H` (1 Hour)                     |
| `/log_cursor`                               | Log Cursor              | A date-time column to use for incremental capture of modifications.                                                                   | String                                                                                                                           | Required (Automatically Discovered) |
| `/page_cursor`                              | Page Cursor             | An indexed, non-NULL integer column to use for ordered table backfills. Does not need to be unique, but should have high cardinality. | String                                                                                                                           | Required (Automatically Discovered) |
| `/concurrency`                              | Concurrency             | Maximum number of concurrent connections to use for backfilling.                                                                      | int                                                                                                                              | 1 Connection                        |
| `/query_limit`                              | Query Limit             | Maximum number of rows to fetch in a query. Will be divided between all connections if `/concurrency` > 1                             | int                                                                                                                              | 100,000 Rows                        |
| `/query_timeout`                            | Query Timeout           | Timeout for queries. Typically left as the default as some tables just take a very long time to respond.                              | [`ISO8601` Duration](https://www.digi.com/resources/documentation/digidocs/90001488-13/reference/r_iso_8601_duration_format.htm) | `PT10M` (10 Minutes)                |
| `/associations`                             | Associations            | List of associated tables for which related data should be loaded.                                                                    | Array[TableAssociation]                                                                                                          | []                                  |
| `/associations/[n]/child_table_name`        | Foreign Table Name      | The name of the "foreign" table that should be associated with the "parent" binding containing this association                       | String                                                                                                                           | Required                            |
| `/associations/[n]/parent_join_column_name` | Parent Join Column      | The name of the column on the "parent" table to be used as the join key                                                               | String                                                                                                                           | Required                            |
| `/associations/[n]/child_join_column_name`  | Foreign Join Column     | The name of the column on the "foreign" table to be used as the join key                                                              | String                                                                                                                           | Required                            |
| `/associations/[n]/load_during_backfill`    | Load During Backfill    | Whether or not to load associated documents during backfill                                                                           | Boolean                                                                                                                          | False                               |
| `/associations/[n]/load_during_incremental` | Load During Incremental | Whether or not to load associated documents during incremental loads                                                                  | Boolean                                                                                                                          | True                                |

### Sample

```yaml

captures:
   ${PREFIX}/${CAPTURE_NAME}:
      endpoint:
         connector:
            image: ghcr.io/estuary/source-netsuite-v2:v3
               config:
                  account: "12345678"
                  authentication:
                     auth_type: token
                     consumer_key: xxx
                     consumer_secret_sops: xxx
                     token_id: xxx
                     token_secret_sops: xxx
                  connection_type: suiteanalytics
                  role_id: 3
                  suiteanalytics_data_source: NetSuite2.com
                  advanced:
                     connection_limit: 20
                     cursor_fields: []
                     enable_auto_cursor: false
                     resource_tracing: false
                     start_date: null
                     task_limit: 10
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
            query_timeout: PT10M
            log_cursor: lastmodifieddate
         target: ${PREFIX}/${CAPTURE_NAME}/transaction
    {...}
```

## Connector Details

### Differences from the REST API

If you are used to the NetSuite REST or SOAP API, you'll notice some differences from the API objects available through these APIs.
This is because the underlying NetSuite data warehouse that we integrate with has a different schema than the NetSuite APIs.

For instance, in the connector schema there's a `transaction` table with a `type` column instead of individual tables for sales orders, invoices, transfers, etc. Instead of pulling the `invoices` table you'll want to pull the `transaction` table and filter by `type = "invoice"`. This pattern applies to the `item` table.

### Custom Records & Fields

All custom records and custom fields on standard records are fully supported in the NetSuite SuiteAnalytics connector. Custom
tables are *not* yet supported on the SuiteQL connector.

### Start Data & Table Filtering

The connector's start date is used to filter data across tables to ensure that the connector does not pull data
created before the connector's start date.

However, not all tables have a `createdDate` column. For these tables, this start date filter is not applied. Most tables
without a `createdDate` column are mapping tables (i.e. `AccountSubsidiaryMap`).

### Deleted Records Support

Special functionality is in place to handle the `deletedRecords` table. Records in this table will be marked as deleted in Estuary.

However, not all records which are deleted in NetSuite will be in the `deletedRecords` table. Specifically, line-level records
do *not* appear in the `deletedRecords` table. For example:

* A invoice is deleted in NetSuite
* The related `transaction` record will be added to `deletedRecords`.
* The related `transactionLine` entries will *not* be added to `deletedRecords`.
* Therefore, the deleted transaction lines will *not* be deleted in Estuary.

This also applies to updates to a record which deletes line-level records. For example:

* An invoice is updated in NetSuite, deleting some transaction lines and adding some new ones
* The newly created transaction lines will be added to the `transaction` table.
* The deleted transaction lines will *not* be added to `deletedRecords`.
* Therefore, the deleted transaction lines will *not* be deleted in Estuary.

Because of this NetSuite limitation, line-level records are not be deleted in Estuary.

Here are the most commonly used tables which have line-level records:

* transactionLine
* TransactionAccountingLine. Fields: transactionline

Here are some additional tables we sometimes see used as well:

* AppliedCreditTransactionLineLink. Fields: nextline, previousline
* PreviousTransactionAccountingLineLink. Fields: nextaccountingline, nextline, previousaccountingline, previousline
* NextTransactionAccountingLineLink. Fields: nextaccountingline, nextline, previousaccountingline, previousline
* inventoryAssignment. Fields: transactionline
* NextTransactionLineLink. Fields: nextline, previousline

Here are some rarely-used tables which also references transaction lines:

* blanketPurchaseOrderExpenseMachine. Fields: line
* CheckExpenseMachine. Fields: line
* purchaseOrderExpenseMachine. Fields: line, orderline
* OcrImportJobReviewItem. Fields: purchaseorderline
* ExpenseMachine. Fields: line
* purchaseRequisitionExpenseMachine. Fields: line
* salesOrdered. Fields: tranline
* glLinesAuditLogLine. Fields: line
* vendorBillExpenseMachine. Fields: line, orderline
* CreditCardChargeExpenseMachine. Fields: line
* vendorCreditExpenseMachine. Fields: line, orderline
* MemDocTransactionTemplateAccountingLine. Fields: memdoctransactiontemplateline
* RecSysConversion. Fields: tranline
* salesInvoiced. Fields: tranline
* salesInvoicedPromotionCombinationsMap. Fields: transactionline
* CreditCardRefundExpenseMachine. Fields: line
* vendorReturnAuthorizationExpenseMachine. Fields: line, orderline
* PreviousTransactionLineLink. Fields: nextline, previousline
* TransactionBilling. Fields: transactionline
* OcrImportJobReviewExpense. Fields: purchaseorderline
* salesOrderedPromotionCombinationsMap. Fields: transactionline

### Line Level Record Updates

NetSuite does not reliably update the `createdDate` field on line-level records. This can cause line-level transactions
to become out of date in Estuary. Exactly when this occurs is dependent on your NetSuite configuration. Contact Estuary
support for assistance with your specific situation.

There are two things you can do to correct this issue:

1. Setup table associations in order to update line level records when a header record is updated.
2. Setup a scheduled full-table refresh on the line-level tables

Here's an example table association. This association updates the `transactionline` table whenever a `transaction` record is updated.

```yaml
parent_join_column_name: location
child_table_name: inventoryItemLocations
child_join_column_name: location
load_during_backfill: false
load_during_incremental: true
```

### Boolean Data Type

NetSuite represents booleans as a string. For example, `true` is represented as `"T"` and `false` is represented as `"F"`.

The connector does not cast these values to booleans. Instead, it will leave them as strings.

### Datetime Fields & Timezones

The connector returns all datetimes in UTC, regardless of the user, subsidiary, or NetSuite account timezone configuration.
