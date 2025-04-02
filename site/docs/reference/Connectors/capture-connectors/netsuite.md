# NetSuite

This connector captures data from Oracle NetSuite into Flow collections. It uses NetSuite's data warehouse functionality
to support high volume data capture (tens of millions of rows per table aren't an issue).

NetSuite provides two ways of access its data warehouse: SuiteAnalytics Connect and SuiteQL. This connector supports both
connection methods. SuiteAnalytics is preferred (but is also a premium NetSuite feature that comes with an additional cost).

## SuiteAnalytics vs SuiteQL

Here are the key differences between the SuiteAnalytics and SuiteQL connection methods:

| **Feature**            | **SuiteAnalytics Connect**                                                    | **SuiteQL**                                                          |
|---------------------------------|----------------------------------------------------------------------|----------------------------------------------------------------------|
| **Custom Records & Fields**     | Fully supports custom records and fields on standard records         | Supports custom fields on standard records (as strings); custom tables not supported |
| **DateTime Fields & Timezones** | All datetimes returned in UTC                                        | Datetimes returned in the timezone configured in the NetSuite account |
| **Row Limit**                   | No explicit row limit mentioned                                      | 1,000,000 rows per table limitation    |


The most important difference between the two connection methods is the SuiteQL row limit. SuiteQL imposes strict limits (1M,000 rows per table), while SuiteAnalytics has no limit. Unfortunately, the 1M limit is a hard limit imposed by NetSuite.

This comparison can help you choose the best method based on your NetSuite setup and data needs!

## Differences Compared to the REST or SOAP APIs

If you are used to the NetSuite REST or SOAP API, you'll notice some differences from the API objects available through these APIs.
This is because the underlying NetSuite data warehouse that we integrate with has a different schema than the NetSuite APIs. All of the data available via the REST or SOAP APIs is available through this connector, but you may need to look in a different location.

For instance, in the connector schema there's a `transaction` table with a `type` column instead of individual tables for sales orders, invoices, transfers, etc. Instead of pulling the `invoices` table you'll want to pull the `transaction` table and filter by `type = "invoice"`. This pattern applies to the `item` table.

## Supported data resources

Flow discovers all of the tables to which you grant access during [setup](#setup), including `Transactions`, `Reports`, `Lists`, and `Setup`. The tables displayed during setup are limited to the permissions associated with the access keys you provide.

## Prerequisites

- Oracle NetSuite [account](https://system.netsuite.com/pages/customerlogin.jsp)
- A new integration with token-based authentication
- A custom role (or the bundled "Data Warehouse Integrator" role) with access to objects you want to capture. See [setup](#setup).
- A new user assigned to the custom role
- Access token generated for the custom role

## Setup

### 1. Create a NetSuite account

Most likely, you already have a NetSuite account in place, but if you don't:

1. Create an account on the [Oracle NetSuite](https://www.netsuite.com/portal/home.shtml) portal.

2. Confirm your email address.

### 2. Enable the required features

Depending on which connector type you are using, you'll need to enable different features.

#### SuiteAnalytics Connect

1. Navigate to **Setup** > **Company** > **Enable Features**.

2. Click the **SuiteCloud** tab.

3. In the **Manage Authentication** section, check the checkbox labeled **TOKEN-BASED AUTHENTICATION**.

4. Save your changes.

5. Next, navigate to **Setup** > **Company** > **Analytics** > **Connectivity** and check the checkbox labeled **SuiteAnalytics Connect**.

6. Save your changes.

#### SuiteQL

1. Navigate to **Setup** > **Company** > **Enable Features**.

2. Click the **SuiteCloud** tab.

3. In the **Manage Authentication** section, check the checkbox labeled **TOKEN-BASED AUTHENTICATION**.

4. Save your changes.

5. Next, in the **SuiteTalk (Web Services)** section, check the checkbox labeled **REST WEB SERVICES**.

6. Save your changes.

### 3. Find Your Account ID

Find your _Realm_, or Account ID. You'll use this to connect with Flow.

1. In your NetSuite portal, go to **Setup** > **Company** > **Company Information**.

2. Copy your Account ID.

   If you have a production account, it will look like `2345678`. If you're using a sandbox, it'll look like `2345678_SB2`.

### 4. Generate Consumer Tokens

Create a NetSuite _integration_ to obtain a Consumer Key and Consumer Secret. You'll need these two tokens, combined with the user tokens we'll generate in the next step, to authenticate with NetSuite.

   1. Navigate to **Setup** > **Integration** > **Manage Integrations** > **New**.

   2. Give the integration a name, for example, `estuary-rest-integration`.

   3. Make sure the **State** option is enabled.

   4. In the **Authentication** section, check the **Token-Based Authentication** checkbox. You do not need to enable "TBA: authorization flow" or "authorization code grant".

   5. Save your changes.

   Your Consumer Key and Consumer Secret will be shown once. Copy them to a safe place.

### 5. Setup a Custom Role

Most of the time, you'll want to setup a custom role to use with Flow to limit the data available to Flow.

If you are using SuiteAnalytics, you can use the bundled "Data Warehouse Integrator" role, instead of creating a new role using the instructions below, if you don't want to manage custom permissions. If you aren't using this read-all role, determining which permissions are required can be very challenging. [Check out this repository](https://github.com/iloveitaly/netsuite-permissions) for help with determining exactly which permissions are required in your case.

1. Go to **Setup** > **Users/Roles** > **Manage Roles** > **New**.

2. Give the role a name, for example, `estuary-integration-role`.

3. Scroll to the **Permissions** section.

4. (IMPORTANT) Click **Transactions** and add all the dropdown entities with either **full** or **view** access level.

5. (IMPORTANT) Click **Reports** and add all the dropdown entities with either **full** or **view** access level.

6. (IMPORTANT) Click **Lists** and add all the dropdown entities with either **full** or **view** access level.

7. (IMPORTANT) Click **Setup** an add all the dropdown entities with either **full** or **view** access level.

To allow your custom role to reflect future changes, be sure to edit these parameters again when you rename or customize any NetSuite object.

### 6. Assign the role to a user

Now that we've chosen the role to use, we have to assign the role to a user.

1. Go to **Setup** > **Users/Roles** > **Manage Users**.

2. Find the user you want to give access to use with Flow. In the **Name** column, click the user's name. Then, click the **Edit** button.

3. Find the **Access** tab.

4. From the dropdown list, select role you created previously; for example, `estuary-integration-role`.

5. Save your changes.

### 7. Generate User Access Tokens

The final step! We are ready to generate the next two tokens that we'll combine with the consumer tokens in order to connect to NetSuite.

1. Go to **Setup** > **Users/Roles** > **Access Tokens** > **New**.

2. Select an **Application Name**.

3. Under **User**, select the user you assigned the role previously.

4. Under **Role**, select the role you assigned to the user previously.

5. Under **Token Name**, give a descriptive name to the token you are creating, for example `estuary-rest-integration-token`.

6. Save your changes.

Your Token ID and Token Secret will be shown once. Copy them to a safe place. You cannot access them again.

You now have a properly configured account with the correct permissions and all the information you need to connect with Flow:

- Realm (Account ID)
- Consumer Key
- Consumer Secret
- Token ID
- Token Secret

:::info
You can also authenticate with a username and password if you using the old `NetSuite.com` data warehouse with SuiteAnalytics, but a consumer/token is recommended for security.
:::

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the NetSuite source connector.

### Properties

#### Endpoint

| Property                      | Title                  | Description                                                                                      | Type   | Required/Default |
| ----------------------------- | ---------------------- | ------------------------------------------------------------------------------------------------ | ------ | ---------------- |
| `/account`                     | Netsuite Account ID    | Netsuite realm/Account ID e.g. 2344535, as for `production` or 2344535_SB1, as for `sandbox` | string | Required         |
| `/suiteanalytics_data_source` | Data Source            | Which NetSuite data source to use. Options are `NetSuite.com`, or `NetSuite2.com`. Use `NetSuite2.com` if you aren't sure.                | string | Required         |
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
| `/authentication/role_id` | Role ID | The internal ID of the role used to access NetSuite.      | int | Required         |

#### Advanced Config options

| Property                     | Title            | Description                                                                                                                                                                                                         | Type | Required/Default |
| ---------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---- | ---------------- |
| `/advanced/connection_limit` | Connection Limit | The maximum number of concurrent data streams to attempt at once.                                                                                                                                                   | int  | 10 Connections   |
| `/advanced/task_limit`       | Task Limit       | The maximum number of concurrent tasks to run at once. A task is either a backfill or incremental load. Backfills can load multiple chunks in parallel, so this must be strictly &lt;= `/advanced/connection_limit` | int  | 5 Tasks          |
| `/advanced/start_date`       | Start Date       | The date that we should attempt to start backfilling from. If not provided, backfill from the beginning. This also prevents data from being imported from before this date.                                                                                                            | date | Not Required     |
| `/advanced/cursor_fields`      | Cursor Fields     | Columns to use as cursor for incremental replication, in order of preference. Case insensitive.                                                                                               | array  | Not Required. `last_modified_date`, `lastmodifieddate`, and more are used as the default list.     |
| `/advanced/enable_auto_cursor` | Enable Auto Cursor | Enable automatic cursor field selection. If enabled, will walk through the list of candidate cursors and select the first one with no null values, otherwise select the cursor with the least nulls. Depending on your NetSuite account performance, you may not be able to enable this. | boolean | Not Required. Defaults to `false`.     |

#### Bindings

| Property                                    | Title                   | Description                                                                                                                           | Type                                                                                                                             | Required/Default                    |
| ------------------------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `/name`                                     | Name                    | The name of the table this binding refers to                                                                                          | string                                                                                                                           | Required                            |
| `/interval`                                 | Interval                | How frequently to check for incremental changes                                                                                       | [`ISO8601` Duration](https://www.digi.com/resources/documentation/digidocs/90001488-13/reference/r_iso_8601_duration_format.htm) | `PT1H` (1 Hour)                     |
| `/log_cursor`                               | Log Cursor              | A date-time column to use for incremental capture of modifications.                                                                   | String                                                                                                                           | Required (Automatically Discovered) |
| `/page_cursor`                              | Page Cursor             | An indexed, non-NULL integer column to use for ordered table backfills. Does not need to be unique, but should have high cardinality. | String                                                                                                                           | Required (Automatically Discovered) |
| `/concurrency`                              | Concurrency             | Maximum number of concurrent connections to use for backfilling.                                                                      | int                                                                                                                              | 1 Connection                        |
| `/query_limit`                              | Query Limit             | Maximum number of rows to fetch in a query. Will be divided between all connections if `/concurrency` > 1                             | int                                                                                                                              | 100,000 Rows                        |
| `/snapshot_backfill`                              | Snapshot Backfill             | Attempt to backfill using a single-shot query to load all rows. Useful when no good page cursor exists, and the table is of reasonable size. Incremental updates are still possible if a log cursor is defined. | bool                                                                                                                              | `false` Rows                        |
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
            log_cursor: lastmodifieddate
         target: ${PREFIX}/${CAPTURE_NAME}/transaction
    {...}
```

## Technical Notes

Here are some additional technical notes about the connector's functionality.

### Boolean Data Type

NetSuite represents booleans as a string. For example, `true` is represented as `"T"` and `false` is represented as `"F"`.

The connector does not cast these values to booleans. Instead, it will leave them as strings.

### Start Data & Table Filtering

The connector's start date is used to filter data across tables to ensure that the connector does not pull data
created before the connector's start date.

However, not all tables have a `createdDate` column. For these tables, this start date filter is not applied. Most tables
without a `createdDate` column are mapping tables (i.e. `AccountSubsidiaryMap`).

## Multiple Subsidiaries

Depending on how you configure your authentication credentials, you may accidentally limit the data available to the connector to a single subsidiary.

Here's how you can ensure the role you've selected has access to multiple subsidiaries:

1. Make the role explicitly available to all subsidiaries by selecting all subsidiaries on the header-level subsidiary field on the role.
2. Add the 'Lists > Subsidiaries: View' permissions to role (i.e. the role that we use to connect to your NetSuite account).

## TransactionLine Primary Key

The transaction line's primary keys are especially tricky. When a transaction's lines are updated, they are assigned a new `linesequencenumber` but are not given a new `id` or `uniquekey`.

The `uniqueid` field is a unique identifier across the entire `transactionLine` table, while the `id` field is only a unique identfier within the scope of a given `transaction`.

The `linesequencenumber` is unique with the scope of a `transaction` but is mutable on a given transaction line.

In Estuary, only records with the same primary key are replaced. If a record is discovered which contains a different set of primary keys, it will be appended to the data set. This means if we chose a primary key set that did *not* contain the `linesequencenumber`, each time the transaction is updated the previous record with that `{transaction, linesequencenumber}` would not be removed from the Estuary data set for `transationLine` causing query inaccuracies if you group data based on the transaction ID.

To avoid this case, we use `{transaction, linesequencenumber}` as the compound primary key

[This article](https://netsuite.smash-ict.com/understand-line-id-vs-line-sequence-number-in-netsuite-transactions/) has a great explanation of the different `transactionLine` keys.

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

## Notes on the SuiteAnalytics Connector

### Custom Records & Fields

All custom records and custom fields on standard records are fully supported in the NetSuite SuiteAnalytics connector. Custom
tables are *not* yet supported on the SuiteQL connector.

### Datetime Fields & Timezones

The connector returns all datetimes in UTC, regardless of the user, subsidiary, or NetSuite account timezone configuration.

## Notes on the SuiteQL Connector

### 1M Row Limit

By default, SuiteQL does not allow a single query to return more than 100,000 records. Additionally, SuiteQL will not
return a proper count of a given query if the result is above 1,000,000 records.

The connector works around the 100,000 query row limit and enables you to pull up to 1,000,000 records per table. However,
due to the count limit SuiteQL imposes, the connector will not be able to properly capture data if a table has more than
1,000,000 records.

### Custom Records and Fields

Custom tables are *not* yet supported on the SuiteQL connector. Contact Estuary support if you need this feature.

Custom fields on standard records are supported, but regardless of the type in NetSuite, they are represented as strings.

If you need the types of the fields to be correct, consider using the NetSuite SuiteAnalytics connector instead.

### DateTime Fields & Timezones

The connector returns all datetimes in the timezone that is configured in your NetSuite account.