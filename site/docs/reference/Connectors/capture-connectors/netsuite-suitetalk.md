# NetSuite SuiteTalk REST

This connector captures data from Oracle NetSuite into Flow collections. It connects to the NetSuite Analytics Data Warehouse using the SuiteQL REST endpoint and a custom role.

It is available for use in the Flow web application.

## SuiteAnalytics vs SuiteQL via REST API

These two different connection modes have some key differences:

### [SuiteAnalytics Connect](../netsuite-suiteanalytics)

- Requires the SuiteAnalytics Connect feature to be purchased on your NetSuite account
- Can inspect which tables (standard & custom) exist in your account
- Can inspect the exact data types specified on these table columns
- This means you can connect to any table in your account and all fields (booleans, date, and datetimes) are properly formatted in Estuary

### SuiteQL via REST API

- Custom columns are assumed to be strings. Use the SuiteAnalytics connector if you need correct data type casting.
- Custom tables will custom work by the Estuary team.
- You are responsible for determining the right set of permissions to grant the connector. The SuiteAnalytics connector has a purpose-built read-only role for the connector to use.

## Prerequisites

- Oracle NetSuite [account](https://system.netsuite.com/pages/customerlogin.jsp?country=US)
- Allowed access to all Account permissions options
- A new integration with token-based authentication
- A custom role with access to objects you want to capture _or_ a purchased SuiteAnalytics Module. See [setup](#general-setup).
- A new user assigned to the custom role
- Access token generated for the custom role

## General Setup

**Set up required features on your NetSuite account**

1. Find your Account ID (also know as the "Realm"). You'll use this to connect with Flow.

   1. In your NetSuite portal, go to **Setup** > **Company** > **Company Information**.

   2. Copy your Account ID.

      If you have a production account, it will look like `2345678`. If you're using a sandbox, it'll look like `2345678_SB2`.

2. Enable the required features.

   1. Navigate to **Setup** > **Company** > **Enable Features**.

   2. Click the **SuiteCloud** tab.

   3. In the **Manage Authentication** section, check the checkbox labeled **TOKEN-BASED AUTHENTICATION**.

   4. If you are using the SuiteQL connection, in the **SuiteTalk (Web Services)** section, check the checkbox labeled **REST WEB SERVICES**.

   5. Save your changes.

   6. If you are using SuiteAnalytics Connect, navigate to **Setup** > **Company** > **Analytics** > **Connectivity** and check the checkbox labeled **SuiteAnalytics Connect**.

   7. Save your changes.

3. Create a NetSuite _integration_ to obtain a Consumer Key and Consumer Secret.

   1. Navigate to **Setup** > **Integration** > **Manage Integrations** > **New**.

   2. Give the integration a name, for example, `estuary-netsuite-integration`.

   3. Make sure the **State** option is enabled.

   4. In the **Authentication** section, check the **Token-Based Authentication** checkbox.

   5. Save your changes.

   Your Consumer Key and Consumer Secret will be shown once. Copy them to a safe place. They will never show up again
   and will be key to the integration working properly.

4. If you are using the **SuiteQL** over REST API connection, Set up a role for use with Flow.

   1. Go to **Setup** > **Users/Roles** > **Manage Roles** > **New**.

   2. Give the role a name, for example, `estuary-integration-role`.

   3. The easiest thing to do here is to click "Core Administrative Permissions". If you want to scope down the permissions given to the connector (which you should) you'll have to determine which permissions are necessary. This is challenging because many different settings and configurations can expand the required permissions. [Check out this repository](https://github.com/iloveitaly/netsuite-permissions) for help with determining exactly which permissions are required in your case.

   4. Scroll to the **Permissions** section.

   5. (IMPORTANT) Click **Transactions** and add all the dropdown entities with either **full** or **view** access level.

   - Find Transaction

   6. (IMPORTANT) Click **Setup** an add the following entities with either **full** or **view** access level.

   - Log in using Access Tokens
   - REST Web Services
   - User Access Tokens

   To allow your custom role to reflect future changes, be sure to edit these parameters again when you rename or customize any NetSuite object.

5. If you are using **SuiteAnalytics Connect** you don't need a custom role. Instead, you can use the bundled "Data Warehouse Integrator"

6. Set up user for use with the connector.

   1. Go to **Setup** > **Users/Roles** > **Manage Users**.

   2. Find the user you want to give access to use with Flow. In the **Name** column, click the user's name. Then, click the **Edit** button.

   3. Find the **Access** tab.

   4. From the dropdown list, select either role you created previously (e.g. `estuary-integration-role`) or the **Data Warehouse Integrator** role if you are using SuiteAnalytics Connect.

   5. Save your changes.

7. Generate an access token.

   1. Go to **Setup** > **Users/Roles** > **Access Tokens** > **New**.

   2. Select the **Application Name** you created earlier.

   3. Under **User**, select the user you assigned the role previously.

   4. Under **Role**, select the role you assigned to the user previously.

   5. Under **Token Name**, give a descriptive name to the token you are creating, for example `estuary-rest-integration-token`.

   6. Save your changes.

   Your Token ID and Token Secret will be shown once. Copy them to a safe place.

You now have a properly configured account with the correct permissions and all the information you need to connect with Flow:

- Account ID (Realm)
- Consumer Key
- Consumer Secret
- Token ID
- Token Secret

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the NetSuite source connector.

### Properties

#### Endpoint

| Property           | Title           | Description                                                                           | Type   | Required/Default |
| ------------------ | --------------- | ------------------------------------------------------------------------------------- | ------ | ---------------- |
| `/account_id`      | Realm           | Netsuite realm e.g. 2344535, as for `production` or 2344535_SB1, as for the `sandbox` | string | Required         |
| `/start_date`      | Token Secret    | The date to start collecting data from                                                | date   | Required         |
| `/consumer_key`    | Consumer Key    | Consumer key associated with your integration.                                        | string | Required         |
| `/consumer_secret` | Consumer Secret | Consumer secret associated with your integration.                                     | string | Required         |
| `/token_key`       | Token Key       | Access token key                                                                      | string | Required         |
| `/token_secret`    | Token Secret    | Access token secret                                                                   | string | Required         |

#### Bindings

| Property        | Title     | Description                                                            | Type   | Required/Default |
| --------------- | --------- | ---------------------------------------------------------------------- | ------ | ---------------- |
| **`/stream`**   | Stream    | Resource of your NetSuite project from which collections are captured. | string | Required         |
| **`/syncMode`** | Sync Mode | Connection method.                                                     | string | Required         |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-netsuite:dev
        config:
          account_id: <your account id>
          consumer_key: <key>
          consumer_secret: <secret>
          token_key: <key>
          token_secret: <secret>
          start_date: "2023-11-01T00:00:00Z"
    bindings:
      - resource:
          stream: Transaction
        target: ${PREFIX}/Transaction
      {...}
```

## Connector Notes

### 1M Row Limit

By default, SuiteQL does not allow a single query to return more than 100,000 records. Additionally, SuiteQL will not
return a proper count of a given query if the result is above 1,000,000 records.

The connector works around the 100,000 query row limit and enables you to pull up to 1,000,000 records per table. However,
due to the count limit SuiteQL imposes, the connector will not be able to properly capture data if a table has more than
1,000,000 records.

### Boolean Data Type

NetSuite represents booleans as a string. For example, `true` is represented as `"T"` and `false` is represented as `"F"`.

The connector does not cast these values to booleans. Instead, it will leave them as strings.

### Custom Records & Fields

Custom tables are *not* yet supported on the SuiteQL connector. Contact Estuary support if you need this feature.

Custom fields on standard records are supported, but regardless of the type in NetSuite, they are represented as strings.

If you need the types of the fields to be correct, consider using the NetSuite SuiteAnalytics connector instead.

### Start Data & Table Filtering

The connector's start date is used to filter data across tables to ensure that the connector does not pull data
created before the connector's start date.

However, not all tables have a `createdDate` column. For these tables, this start date filter is not applied. Most tables
without a `createdDate` column are mapping tables (i.e. `AccountSubsidiaryMap`).

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

### DateTime Fields & Timezones

The connector returns all datetimes in the timezone that is configured in your NetSuite account.