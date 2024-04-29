
# NetSuite

This connector captures data from Oracle NetSuite into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-netsuite:dev`](https://ghcr.io/estuary/source-netsuite:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

You can find their documentation [here](https://docs.airbyte.com/integrations/sources/netsuite/),
but keep in mind that the two versions may be significantly different.

## Supported data resources

Flow captures collections from any NetSuite object to which you grant access during [setup](#setup), including `Transactions`, `Reports`, `Lists`, and `Setup`.

## Prerequisites

* Oracle NetSuite [account](https://system.netsuite.com/pages/customerlogin.jsp?country=US)
* Allowed access to all Account permissions options
* A new integration with token-based authentication
* A custom role with access to objects you want to capture. See [setup](#setup).
* A new user assigned to the custom role
* Access token generated for the custom role

## Setup

**Create a NetSuite account**

1. Create an account on the [Oracle NetSuite](https://www.netsuite.com/portal/home.shtml) portal.

2. Confirm your email address.

**Set up your NetSuite account**

1. Find your *Realm*, or Account ID. You'll use this to connect with Flow.

   1. In your NetSuite portal, go to **Setup** > **Company** > **Company Information**.

   2. Copy your Account ID.

      If you have a production account, it will look like `2345678`. If you're using a sandbox, it'll look like `2345678_SB2`.

2. Enable the required features.

   1. Navigate to **Setup** > **Company** > **Enable Features**.

   2. Click the **SuiteCloud** tab.

   3. In the **SuiteScript** section, check the checkboxes labeled **CLIENT SUITESCRIPT** and **SERVER SUITESCRIPT**.

   4. In the **Manage Authentication** section, check the checkbox labeled **TOKEN-BASED AUTHENTICATION**.

   5. In the **SuiteTalk (Web Services)** section, check the checkbox labeled **REST WEB SERVICES**.

   6. Save your changes.

3. Create a NetSuite *integration* to obtain a Consumer Key and Consumer Secret.

   1. Navigate to **Setup** > **Integration** > **Manage Integrations** > **New**.

   2. Give the integration a name, for example, `estuary-rest-integration`.

   3. Make sure the **State** option is enabled.

   4. In the **Authentication** section, check the **Token-Based Authentication** checkbox.

   5. Save your changes.

   Your Consumer Key and Consumer Secret will be shown once. Copy them to a safe place.

4. Set up a role for use with Flow.

   1. Go to **Setup** > **Users/Roles** > **Manage Roles** > **New**.

   2. Give the role a name, for example, `estuary-integration-role`.

   3. Scroll to the **Permissions** section.

   4. (IMPORTANT) Click **Transactions** and add all the dropdown entities with either **full** or **view** access level.

   5. (IMPORTANT) Click **Reports** and add all the dropdown entities with either **full** or **view** access level.

   6. (IMPORTANT) Click **Lists** and add all the dropdown entities with either **full** or **view** access level.

   7. (IMPORTANT) Click **Setup** an add all the dropdown entities with either **full** or **view** access level.

   To allow your custom role to reflect future changes, be sure to edit these parameters again when you rename or customize any NetSuite object.

5. Set up user for use with Flow.

   1. Go to **Setup** > **Users/Roles** > **Manage Users**.

   2. Find the user you want to give access to use with Flow. In the **Name** column, click the user's name. Then, click the **Edit** button.

   3. Find the **Access** tab.

   4. From the dropdown list, select role you created previously; for example, `estuary-integration-role`.

   5. Save your changes.

6. Generate an access token.

   1. Go to **Setup** > **Users/Roles** > **Access Tokens** > **New**.

   2. Select an **Application Name**.

   3. Under **User**, select the user you assigned the role previously.

   4. Under **Role**, select the role you assigned to the user previously.

   5. Under **Token Name**,  give a descriptive name to the token you are creating, for example `estuary-rest-integration-token`.

   6. Save your changes.

   Your Token ID and Token Secret will be shown once. Copy them to a safe place.

You now have a properly configured account with the correct permissions and all the information you need to connect with Flow:

* Realm (Account ID)
* Consumer Key
* Consumer Secret
* Token ID
* Token Secret

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the NetSuite source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/realm` | Realm | Netsuite realm e.g. 2344535, as for `production` or 2344535_SB1, as for the `sandbox` | string | Required |
| `/consumer_key` | Consumer Key | Consumer key associated with your integration. | string | Required |
| `/consumer_secret` | Consumer Secret | Consumer secret associated with your integration. | string | Required |
| `/token_key` | Token Key | Access token key | string | Required |
| `/token_secret` | Token Secret | Access token secret | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your NetSuite project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-netsuite:dev
        config:
          realm: <your account id>
          consumer_key: <key>
          consumer_secret: <secret>
          token_key: <key>
          token_secret: <secret>
    bindings:
      - resource:
          stream: items
          syncMode: full_refresh
        target: ${PREFIX}/items
      {...}
```
