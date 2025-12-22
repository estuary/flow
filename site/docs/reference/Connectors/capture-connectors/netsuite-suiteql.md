
# Netsuite SuiteQL (Beta)

This connector captures data from Oracle NetSuite into Estuary collections using SuiteQL.

To use SuiteAnalytics Connect to sync your data instead, see the [Netsuite SuiteAnalytics connector](./netsuite-suiteanalytics.md).

## Supported data resources

Currently, this connector supports a subset of NetSuite tables, such as:

* Customer
* Item
* Term
* Transaction
* TransactionHistory
* TransactionLine
* TransactionShippingAddress
* TransactionStatus

If you need to capture a table that is not yet supported, [contact support](mailto:support@estuary.dev) with the table name(s).
Estuary support will be able to confirm availability and, if needed, add the table(s) to the connector.

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

   7. (IMPORTANT) Click **Setup** an add all the dropdown entities with either **full** or **view** access level.

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
| `/interval` | Interval | How frequently to check for incremental changes | [`ISO8601` Duration](https://www.digi.com/resources/documentation/digidocs/90001488-13/reference/r_iso_8601_duration_format.htm) | `PT1H` (1 Hour) |
| `/schedule` | Schedule | Schedule to automatically rebackfill this binding. Accepts a cron expression. | string |  |
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
            interval: PT1H
            page_cursor: id
            columns: []
            query_limit: 10000
         target: ${PREFIX}/${CAPTURE_NAME}/transaction
    {...}
```
