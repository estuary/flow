# Salesforce

This connector captures data from Salesforce objects into Flow collections.
It uses Salesforce's [Bulk API 2.0](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_intro.htm) and [REST API](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_rest.htm).

This connector offers several unique advantages:

- **Efficient Backfills**: Uses Salesforce's Bulk API 2.0 for initial data loads and backfills, which doesn't consume API credits and provides significantly faster data transfer rates.

- **Formula Field Handling**: The connector automatically refreshes formula fields on a configurable schedule (default: daily). This ensures your formula field data stays current without manual intervention, even though Salesforce doesn't track formula field changes in record modification timestamps.

- **Custom Field Support**: Enhanced handling of custom fields with better type detection and mapping, ensuring all your custom Salesforce objects and fields are captured accurately.

This connector is available for use in the Flow web application.
For local development or open-source workflows, [`ghcr.io/estuary/source-salesforce-native:dev`](https://ghcr.io/estuary/source-salesforce-native:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

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

### Authentication

Authentication to Salesforce is done via OAuth and requires the following:

* A Salesforce organization on the Enterprise tier, or with an equivalent [API request allocation](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_api.htm).

* Salesforce user credentials. We recommend creating a dedicated read-only [Salesforce user](#create-a-read-only-salesforce-user).

### Setup

#### Create a read-only Salesforce user

Creating a dedicated read-only Salesforce user is a simple way to specify which objects Flow will capture.
This is useful if you have a large amount of data in your Salesforce organization.

1. While signed in as an administrator, create a [new profile](https://help.salesforce.com/s/articleView?id=sf.users_profiles_cloning.htm&type=5) by cloning the standard [Minimum Access](https://help.salesforce.com/s/articleView?id=sf.standard_profiles.htm&type=5) profile.

2. [Edit the new profile's permissions](https://help.salesforce.com/s/articleView?id=sf.perm_sets_object_perms_edit.htm&type=5). Grant it read access to all the standard and custom objects you'd like to capture with Flow.

3. [Create a new user](https://help.salesforce.com/s/articleView?id=sf.adding_new_users.htm&type=5), applying the profile you just created.
You'll use this user's email address and password to authenticate Salesforce in Flow.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the Flow specification file.
See [connectors](../../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Salesforce source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/start_date` | Start Date | Start date in the format YYYY-MM-DD. Data added on and after this date will be captured. If left blank, the start date will be set to Salesforce's founding date. | string | 1999-02-03T00:00:00Z |
| `/is_sandbox` | Sandbox | Whether you&#x27;re using a [Salesforce Sandbox](https://help.salesforce.com/s/articleView?id=sf.deploy_sandboxes_parent.htm&type=5). | boolean | `false` |
| **`/credentials/credentials_title`** | Authentication Method | Set to `OAuth Credentials`. | string | Required |
| **`/credentials/client_id`** | OAuth Client ID | The OAuth app's client ID. | string | Required |
| **`/credentials/client_secret`** | OAuth Client Secret | The OAuth app's client secret. | string | Required |
| **`/credentials/refresh_token`** | Refresh Token | The refresh token received from the OAuth app. | string | Required |
| **`/credentials/instance_url`** | Instance URL | The URL for the instance of your Salesforce organization. | string | Required |
| `/advanced/window_size` | Window size | The date window size in days to use when querying the Salesforce APIs. | integer | 18250 |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Name | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string | PT5M |
| `/schedule` | Formula Field Refresh Schedule | The schedule for refreshing this binding's [formula fields](#formula-fields). Accepts a cron expression. For example, a schedule of `55 23 * * *` means the binding will refresh formula fields at 23:55 UTC every day. If left empty, the binding will not refresh formula fields. | string | 55 23 * * * |

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-salesforce-native:dev
        config:
          credentials:
            credentials_title: "OAuth Credentials"
            client_id: <secret>
            client_secret: <secret>
            refresh_token: <secret>
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

To address this challenge, the Salesforce connector is able to refresh the values of formula fields on a schedule after the initial backfill completes. This is controlled at a binding level by the cron expression in the [`schedule` property](#bindings). When a scheduled formula field refresh occurs, the connector fetches every record's current formula field values and merges them into the associated collection with a top-level [`merge` reduction strategy](../../../reduction-strategies/merge.md).

## Troubleshooting

### Field Permissions

If a field is not present in documents captured by the connector but the field exists on the object in Salesforce, confirm that the field is visible for the configured user in Salesforce's [field permissions](https://help.salesforce.com/s/articleView?id=platform.users_profiles_field_perms.htm&type=5). If the Salesforce account used when authenticating the connector does not have permission to view a field, Salesforce prevents the connector from replicating that field.

To check field permissions in Salesforce:
1. Go to **Setup > Object Manager**
2. Click the object for the specific field (Account, Contact, Opportunity, etc.)
3. Click "Fields & Relationships" and select the field that is not being captured.
4. Click "Set Field-Level Security" and make sure the profile of the account used for authentication has visibility for the field.
5. If the associate profile does not have visibility, update it and click "Save".
