---
description: Use Estuary's Salesforce connector to capture Salesforce standard and custom objects. Supports formula field handling, custom fields, and multiple authentication methods.
---

# Salesforce

This connector captures data from Salesforce objects into Estuary collections.
It uses Salesforce's [Bulk API 2.0](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_intro.htm) and [REST API](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_rest.htm).

This connector offers several unique advantages:

- **Efficient Backfills**: Uses Salesforce's Bulk API 2.0 for initial data loads and backfills, enabling significantly faster data transfer rates while preserving REST API call limits. Note: Bulk API 2.0 has its own usage limits, and bulk jobs submitted by the connector count against those.

- **Formula Field Handling**: The connector automatically refreshes formula fields on a configurable schedule (default: daily). This ensures your formula field data stays current without manual intervention, even though Salesforce doesn't track formula field changes in record modification timestamps.

- **Custom Field Support**: Enhanced handling of custom fields with better type detection and mapping, ensuring all your custom Salesforce objects and fields are captured accurately.

## Supported data resources

This connector captures Salesforce [standard objects](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_list.htm), [custom objects](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_custom_objects.htm), and field history for custom objects.

All available objects will appear after connecting to Salesforce.

:::info

To reduce how many API calls are needed to discover objects, the connector maintains an internal list of standard objects available for replication. If a standard object does not appear as an available binding after connecting to Salesforce, please contact us via [Slack](https://go.estuary.dev/slack) or [email](mailto://support@estuary.dev) to request the standard object's addition to the connector's internal list.

:::

Because most Salesforce accounts contain large volumes of data, you may only want to capture a subset of the available objects.
There are several ways to control this:

* Create a [dedicated Salesforce user](#create-a-read-only-salesforce-user) with access only to the objects you'd like to capture.

* During [capture creation in the web application](../../../../guides/create-dataflow.md#create-a-capture),
disable the bindings for objects you don't want to capture.

## Prerequisites

* A Salesforce organization on the Enterprise tier, or with an equivalent [API request allocation](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_api.htm).

* Salesforce user credentials. We recommend creating a dedicated read-only [Salesforce user](#create-a-read-only-salesforce-user).

:::tip
If your Salesforce organization's login policy prevents signing in through `login.salesforce.com`, set the [`my_domain`](#endpoint) field to your full My Domain login host - for example, `mycompany.my.salesforce.com` or `acme--uat.sandbox.my.salesforce.com`. This applies to all authentication methods and is required when authenticating with Client Credentials. Leave it blank to use the standard login endpoint.
:::

### Authentication

There are three ways to authenticate with Salesforce when capturing data into Estuary.

#### OAuth

Sign in to Salesforce from the Estuary web app and grant access. Estuary handles the token exchange and refresh for you. This is the recommended method in the web app.

#### Username, Password, & Security Token

Sign in using a Salesforce user's username, password, and [security token](https://help.salesforce.com/s/articleView?id=sf.user_security_token.htm&type=5). This is a good fit if you cannot use OAuth, but **will not work in organizations that enforce single sign-on (SSO)** for interactive logins.

#### Client Credentials

Authenticate via Salesforce's [OAuth 2.0 Client Credentials flow](https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_client_credentials_flow.htm&type=5) using a Salesforce [External Client App](https://help.salesforce.com/s/articleView?id=xcloud.external_client_apps_overview.htm&type=5) (or a legacy Connected App). The connector exchanges the app's Consumer Key and Consumer Secret for an access token minted as the app's configured "Run As" user.

Use this option when your organization enforces SSO or other login restrictions that prevent the other authentication methods from working. The [`my_domain`](#endpoint) field is required since the Client Credentials flow must hit your org's My Domain token endpoint.

See [Create an External Client App](#create-an-external-client-app) for setup steps.

### Setup

#### Create a read-only Salesforce user

Creating a dedicated read-only Salesforce user is a simple way to specify which objects Estuary will capture.
This is useful if you have a large amount of data in your Salesforce organization.

1. While signed in as an administrator, create a [new profile](https://help.salesforce.com/s/articleView?id=sf.users_profiles_cloning.htm&type=5) by cloning the standard [Minimum Access](https://help.salesforce.com/s/articleView?id=sf.standard_profiles.htm&type=5) profile.

2. [Edit the new profile's permissions](https://help.salesforce.com/s/articleView?id=sf.perm_sets_object_perms_edit.htm&type=5). Grant it read access to all the standard and custom objects you'd like to capture with Estuary.

3. [Create a new user](https://help.salesforce.com/s/articleView?id=sf.adding_new_users.htm&type=5), applying the profile you just created.
You'll use this user's email address and password to authenticate Salesforce in Estuary. If you're authenticating with username, password, and security token, you'll also need the user's security token. If you're authenticating with Client Credentials, you'll designate this user as the External Client App's "Run As" user.

#### Create an External Client App

Follow these steps to create an External Client App in Salesforce for use with Client Credentials authentication. You must be a Salesforce administrator to complete this setup.

##### Create the external client app

1. In Salesforce, go to **Setup**.

2. In the Quick Find box, enter `App Manager`, then click **App Manager**.

3. In App Manager, click **New External Client App**.

4. Enter the following details:
   * **Label**: a name for the app, for example, `Estuary Integration`.
   * **Contact Email**: your contact email address.

5. Find the **Distribution State** field and set **Distribution State** to `Local`. This makes the app a local External Client App only usable in this org and not packageable.

6. In the **API (Enable OAuth Settings)** section, select the **Enable OAuth** checkbox.

7. Expand the **OAuth Settings** section.

8. In the **Callback URL** field, enter `https://dashboard.estuary.dev/oauth`. The Client Credentials flow does not redirect to the specified callback URL, but Salesforce requires a value be provided.

9. In the **OAuth Scopes** menu, select the `Manage user data via APIs (api)` scope.

10. In the **Field Enablement** section, select the **Enable Client Credentials Flow** checkbox.

11. Click **Create**. Your local external client app now appears in the External Client App Manager.

##### Modify app policy

1. Open your app.

2. Go to **Policies** and click **Edit**.

3. In the **OAuth Policies** section, under **OAuth Flows and External Client App Enhancements**, make sure **Enable Client Credentials Flow** is checked.

4. In the **Security** section, deselect all the checkboxes.

5. Enter the integration user for the app. We recommend the dedicated Salesforce user you created in [Create a read-only Salesforce user](#create-a-read-only-salesforce-user). The connector's access token will inherit this user's permissions, so the user must have read access to every object you intend to capture.

6. In the **App Authorization** section, select `Relax IP restrictions` in the **IP Relaxation** dropdown menu.

7. Save your changes.

##### Find client credentials

1. Open your app.

2. Click the **Settings** tab.

3. Expand the **OAuth Settings** section.

4. In the **App Settings** field, click **Consumer Key and Secret**.

5. Remember the **Consumer Key** and **Consumer Secret**. You'll use these as the `client_id` and `client_secret` values when configuring the connector.

You must also provide your My Domain URL when configuring the connector with the Client Credentials flow - for example, `mycompany.my.salesforce.com`.

:::note
If your org does not yet expose the External Client App Manager, you can use a legacy [Connected App](https://help.salesforce.com/s/articleView?id=sf.connected_app_create_basics.htm&type=5) instead. The connector works the same way with either app type - it just needs the Consumer Key and Consumer Secret. The Client Credentials Flow and integration user configuration steps are equivalent in the Connected App UI.
:::

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the Data Flow specification file.
See [connectors](../../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Salesforce source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/start_date` | Start Date | Start date in the format YYYY-MM-DD. Data added on and after this date will be captured. If left blank, the start date will be set to Salesforce's founding date. | string | 1999-02-03T00:00:00Z |
| `/my_domain` | My Domain | Your Salesforce My Domain login host. Enter the full host ending in .my.salesforce.com to login with your My Domain host. e.g. mycompany.my.salesforce.com, acme--uat.sandbox.my.salesforce.com. Leave blank to log in via the standard login/test endpoint. Required when authenticating with Client Credentials. | string | `""` |
| `/is_sandbox` | Sandbox | Whether you&#x27;re using a [Salesforce Sandbox](https://help.salesforce.com/s/articleView?id=sf.deploy_sandboxes_parent.htm&type=5). | boolean | `false` |
| **`/credentials`** | Authentication | Credentials for the chosen [authentication method](#authentication). See the per-method credential properties below. | object | Required |
| `/advanced/window_size` | Window size | The date window size in days to use when querying the Salesforce APIs. | integer | 18250 |

##### Credentials

**OAuth:**

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/credentials_title`** | Authentication Method | Must be `OAuth Credentials`. | string | Required |
| **`/credentials/client_id`** | OAuth Client ID | The OAuth app's client ID. | string | Required |
| **`/credentials/client_secret`** | OAuth Client Secret | The OAuth app's client secret. | string | Required |
| **`/credentials/refresh_token`** | OAuth Refresh Token | The refresh token received from the OAuth app. | string | Required |
| **`/credentials/instance_url`** | Instance URL | The URL for the instance of your Salesforce organization. | string | Required |

**Username, Password, & Security Token:**

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/credentials_title`** | Authentication Method | Must be `Username, Password, & Security Token`. | string | Required |
| **`/credentials/username`** | Username | The user's username. | string | Required |
| **`/credentials/password`** | Password | The user's password. | string | Required |
| **`/credentials/security_token`** | Security Token | The user's security token. | string | Required |

**Client Credentials:**

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/credentials_title`** | Authentication Method | Must be `Client Credentials`. | string | Required |
| **`/credentials/client_id`** | Consumer Key | The Consumer Key of the External Client App or Connected App. | string | Required |
| **`/credentials/client_secret`** | Consumer Secret | The Consumer Secret of the External Client App or Connected App. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Name | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string | PT5M |
| `/schedule` | Formula Field Refresh Schedule | The schedule for refreshing this binding's [formula fields](#formula-fields). Accepts a cron expression. For example, a schedule of `55 23 * * *` means the binding will refresh formula fields at 23:55 UTC every day. If left empty, the binding will not refresh formula fields. | string | 55 23 * * * |

### Sample

This sample specification reflects the Client Credentials authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-salesforce-native:v1
        config:
          credentials:
            credentials_title: "Client Credentials"
            client_id: <secret>
            client_secret: <secret>
          my_domain: mycompany.my.salesforce.com
          is_sandbox: false
          start_date: "2025-03-19T12:00:00Z"
          advanced:
            window_size: 18250
    bindings:
      - resource:
          name: Account
          interval: PT5M
          schedule: "55 23 * * *"
        target: ${PREFIX}/Account
      {...}
```

## Formula Fields

Salesforce objects can contain [formula fields](https://help.salesforce.com/s/articleView?id=platform.customize_formulas.htm&type=5), fields whose values are calculated at query time. Since formula fields [do not maintain state](https://help.salesforce.com/s/articleView?id=000396215&type=1) in Salesforce, formula fields updates do not update the associated record's last modified timestamp. The Salesforce connector uses the last modified timestamp to incrementally detect changes, and since formula field updates don't update the last modified timestamp, formula fields updates are not incrementally captured by the connector.

To address this challenge, the Salesforce connector is able to refresh the values of formula fields on a schedule after the initial backfill completes. This is controlled at a binding level by the cron expression in the [`schedule` property](#bindings). When a scheduled formula field refresh occurs, the connector fetches every record's current formula field values and merges them into the associated collection with a top-level [`merge` reduction strategy](/reference/reduction-strategies/merge).

Note that formula field refreshes emit partial documents containing only the record's key and formula field values. By default, these are combined with previously captured complete documents, and this works well for [standard updates materializations](/concepts/materialization/#how-continuous-materialization-works). However, [delta updates materializations](/concepts/materialization/#delta-updates) do not fully reduce documents, so partial documents from formula field refreshes are materialized as-is with all non-formula fields as `null`.

## Troubleshooting

### Field Permissions

If a field is not present in documents captured by the connector but the field exists on the object in Salesforce, confirm that the field is visible for the configured user in Salesforce's [field permissions](https://help.salesforce.com/s/articleView?id=platform.users_profiles_field_perms.htm&type=5). If the Salesforce account used when authenticating the connector does not have permission to view a field, Salesforce prevents the connector from replicating that field.

To check field permissions in Salesforce:
1. Go to **Setup > Object Manager**
2. Click the object for the specific field (Account, Contact, Opportunity, etc.)
3. Click "Fields & Relationships" and select the field that is not being captured.
4. Click "Set Field-Level Security" and make sure the profile of the account used for authentication has visibility for the field.
5. If the associate profile does not have visibility, update it and click "Save".
