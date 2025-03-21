# Salesforce — Historical data (deprecated)

This connector captures data from Salesforce objects into Flow collections.
It uses batch processing and is ideal for syncing your historical Salesforce data.

[A separate connector is available for real-time Salesforce data capture](./salesforce-real-time.md).
For help using both connectors in parallel, [contact your Estuary account manager](mailto:info@estuary.dev).

This connector is available for use in the Flow web application.
For local development or open-source workflows, [`ghcr.io/estuary/source-salesforce:dev`](https://ghcr.io/estuary/source-salesforce:dev) provides the latest connector image. You can also follow the link in your browser to see past image versions.

## Supported data resources

This connector can capture the following Salesforce [standard objects](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_list.htm), if present in your account:

* Account
* Contact
* User
* OpportunityFilledHistory
* LeadHistory
* Opportunity
* Campaign
* Case
* ContactLineItem
* Entitlement
* Lead
* LiveChatTranscript
* MessagingSession
* Quote
* QuoteLineItem
* ServiceAppointment
* ServiceContract
* Task
* UserServicePresence
* WorkOrder
* WorkOrderLineItem

The Salesforce connector has the ability to capture all standard Salesforce objects as well as custom objects. All available objects will appear after connecting to Salesforce.

Because most Salesforce accounts contain large volumes of data, you may only want to capture a subset of the available objects.
There are several ways to control this:

* Create a [dedicated Salesforce user](#create-a-read-only-salesforce-user) with access only to the objects you'd like to capture.

* Apply a filter when you [configure](#endpoint) the connector. If you don't apply a filter, the connector captures all objects available to the user.

* During [capture creation in the web application](/guides/create-dataflow.md#create-a-capture),
remove the bindings for objects you don't want to capture.

## Prerequisites

### Using OAuth2 to authenticate with Salesforce in the Flow web app

If you're using the Flow web app, you'll be prompted to authenticate with Salesforce using OAuth. You'll need the following:

* A Salesforce organization on the Enterprise tier, or with an equivalent [API request allocation](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_api.htm).

* Salesforce user credentials. We recommend creating a dedicated read-only [Salesforce user](#create-a-read-only-salesforce-user).

### Configuring the connector specification manually

If you're working with flowctl and writing specifications in a local development environment,
you'll need to manually supply OAuth credentials. You'll need:

* The items required to [set up with OAuth2](#using-oauth2-to-authenticate-with-salesforce-in-the-flow-web-app).

* A Salesforce developer application with a generated client ID, client secret, and refresh token. [See setup steps.](#create-a-developer-application-and-generate-authorization-tokens)

### Setup

#### Create a read-only Salesforce user

Creating a dedicated read-only Salesforce user is a simple way to specify which objects Flow will capture.
This is useful if you have a large amount of data in your Salesforce organization.

1. While signed in as an administrator, create a [new profile](https://help.salesforce.com/s/articleView?id=sf.users_profiles_cloning.htm&type=5) by cloning the standard [Minimum Access](https://help.salesforce.com/s/articleView?id=sf.standard_profiles.htm&type=5) profile.

2. [Edit the new profile's permissions](https://help.salesforce.com/s/articleView?id=sf.perm_sets_object_perms_edit.htm&type=5). Grant it read access to all the standard and custom objects you'd like to capture with Flow.

3. [Create a new user](https://help.salesforce.com/s/articleView?id=sf.adding_new_users.htm&type=5), applying the profile you just created.
You'll use this user's email address and password to authenticate Salesforce in Flow.

#### Create a developer application and generate authorization tokens

To manually write a capture specification for Salesforce, you need to create and configure a developer application.
Through this process, you'll obtain the client ID, client secret, and refresh token.

1. Create a [new developer application](https://help.salesforce.com/s/articleView?id=sf.connected_app_create_api_integration.htm&type=5).

   a. When selecting **Scopes** for your app, select **Manage user data via APIs `(api)`**, **Perform requests at any time `(refresh_token, offline_access)`**, and **Manage user data via Web browsers `(web)`**.

2. Edit the app to ensure that **Permitted users** is set to [All users may self-authorize](https://help.salesforce.com/s/articleView?id=sf.connected_app_manage_oauth.htm&type=5).

3. Locate the [Consumer Key and Consumer Secret](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_web_server_flow.htm&type=5). These are equivalent to the client id and client secret, respectively.

4. Follow the [Salesforce Web Server Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_web_server_flow.htm&type=5). The final POST response will include your refresh token.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the Flow specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the batch Salesforce source connector.

### Formula Fields

Estuary is able to capture Salesforce formula fields, however, regular full syncs must be configured in order to ensure up to date formula fields. Given the batch processing of this connector, if formula field values are updated in between syncs, Estuary will not be aware of any changes.

In order to ensure data freshness, it is recommended that you configure your capture to regularly initiate full refreshes of your source. Once a historical backfill is complete, updated formula field values will be reflected within Estuary.

### Slowly Changing Dimensions Type 2

Estuary is capable of capturing a stream of your Salesforce data as it changes through a feature called Delta Updates. To read more about how Delta Updates works visit our [docs](https://docs.estuary.dev/concepts/materialization/#delta-updates).

### Merging Real Time and Batch Data

Estuary offers connectors for both batch and real time capture. It is possible to create collections that reflect both batch and real time data capture using the following steps:

1. Create a new capture using the batch capture connector.

2. Create a new capture using the real time capture connector.

3. Both captures should share the same namespace in order to write to the same collection. For example, you would end up with a batch connector named `Tenant/Salesforce/source-salesforce` and a real time capture named `Tenant/Salesforce/source-salesforce-real-time`.

4. If configured correctly, both captures will read data into the same collections that are titled `Tenant/Salesforce/your-stream`.

Estuary should automatically merge your documents, ensuring that duplicates are not produced when pulling from two captures with the same source. Before continuing, it is important to make sure that a reduction strategy has been implemented for your collections.

This step requires using [flowctl](https://docs.estuary.dev/concepts/flowctl/), please visit our documentation for more information.

1. Pull down your active specifications into your local environment using the command `flowctl catalog pull-specs --prefix Tenant/Salesforce`

2. This command will generate a folder for each subtree of your tenant. Using the above example tenant, you would end up with a top level folder structure named Tenant and a sub folder named Salesforce.

3. Within the sub folder for your Salesforce capture you will find yaml specification files for your each of your collections that follow the naming convention `BindingName.schema.yaml`.

4. For each newly created collection, make sure that it contains the following reduction strategy:

```yaml
---
type: object
additionalProperties: true
properties:
  Id:
    type:
      - string
  ...
required:
  - Id
# Your collection must include this line. If missing, please add below
reduce:
  strategy: merge
```

5. If the above line was missing you must also run the command `flowctl catalog publish --source flow.yaml` at the root level of your local folder structure to publish the changes to Flow.

### Properties

#### Endpoint

The properties in the table below reflect the manual authentication method.
If you're working in the Flow web app, you'll use [OAuth2](#using-oauth2-to-authenticate-with-salesforce-in-the-flow-web-app),
so you won't need the `/credentials` values listed here.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** |  |  | object | Required |
| `/credentials/auth_type` | Authorization type | Set to `Client` | string |  |
| **`/credentials/client_id`** | Client ID | The Salesforce Client ID, also known as a Consumer Key, for your developer application. | string | Required |
| **`/credentials/client_secret`** | Client Secret | The Salesforce Client Secret, also known as a Consumer Secret, for your developer application. | string | Required |
| **`/credentials/refresh_token`** | Refresh Token | The refresh token generated by your developer application. | string | Required |
| `/is_sandbox` | Sandbox | Whether you&#x27;re using a [Salesforce Sandbox](https://help.salesforce.com/s/articleView?id=sf.deploy_sandboxes_parent.htm&type=5). | boolean | `false` |
| `/start_date` | Start Date | Start date in the format YYYY-MM-DD. Data added on and after this date will be captured. If this field is blank, all data will be captured. | string |  |
| `/streams_criteria` | Filter Salesforce Objects (Optional) | Filter Salesforce objects for capture. | array |  |
| _`/streams_criteria/-/criteria`_ | Search criteria | Possible criteria are `"starts with"`, `"ends with"`, `"contains"`, `"exacts"`, `"starts not with"`, `"ends not with"`, `"not contains"`, and `"not exacts"`. | string | `"contains"` |
| _`/streams_criteria/-/value`_ | Search value | Search term used with the selected criterion to filter objects. | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/cursorField` | Cursor field | Field used as a cursor to track data replication; typically a timestamp field. | array, null |  |
| **`/stream`** | Stream | Salesforce object from which a collection is captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-salesforce:dev
        config:
          credentials:
            auth_type: Client
            client_id: {your_client_id}
            client_secret: {secret}
            refresh_token: {XXXXXXXX}
          is_sandbox: false
          start_date: 2022-01-01
          streams_criteria:
            - criteria: "starts with"
              value: "Work"
    bindings:
      - resource:
          cursorField: [SystemModstamp]
          stream: WorkOrder
          syncMode: incremental
        target: ${PREFIX}/WorkOrder
      - resource:
          cursorField: [SystemModstamp]
          stream: WorkOrderLineItem
          syncMode: incremental
        target: ${PREFIX}/WorkOrderLineItem
```