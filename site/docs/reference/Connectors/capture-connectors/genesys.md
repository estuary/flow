# Genesys

This connector captures data from Genesys into Flow collections.

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-genesys:dev`](https://ghcr.io/estuary/source-genesys:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Genesys API:

* [Campaigns](https://developer.genesys.cloud/devapps/api-explorer#get-api-v2-outbound-campaigns-all)
* [Conversations](https://developer.genesys.cloud/routing/conversations/conversations-apis#post-api-v2-analytics-conversations-details-jobs)
* [Messaging Campaigns][https://developer.genesys.cloud/devapps/api-explorer#get-api-v2-outbound-messagingcampaigns]
* [Queues](https://developer.genesys.cloud/devapps/api-explorer#get-api-v2-routing-queues)
* [Queue Members](https://developer.genesys.cloud/devapps/api-explorer#get-api-v2-routing-queues--queueId--members)
* [Teams](https://developer.genesys.cloud/devapps/api-explorer#post-api-v2-teams-search)
* [Users](https://developer.genesys.cloud/useragentman/users/#get-api-v2-users)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

* A Genesys account with API access.
* An OAuth app created within your Genesys account with the Client Credentials grant type. See [Authentication](#authentication) for instructions on how to create this.

### Authentication

Genesys requires an OAuth client for authentication. To create an OAuth client in your Genesys account that will allow Flow to access data, follow the steps below or refer to [Genesys' documentation](https://help.mypurecloud.com/articles/create-an-oauth-client/).

1. Log into your [Genesys account](https://login.mypurecloud.com/#/authenticate).
2. Click **Admin**.
3. Under the **Integrations** section, click **OAuth**.
4. Click the **+ Add Client** button.
5. Enter an **App Name**, like "Estuary Flow OAuth Client".
6. Under **Grant Types**, select **Client Credentials**.
7. In the **Roles** tab, select the appropriate role for the OAuth app.
8. Click **Save**.
9. Note the **Client ID** and **Client Secret** for when you set up the connector.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Genesys source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/start_date` | Replication Start Date | UTC date and time in the format "YYYY-MM-DDTHH:MM:SSZ". Data prior to this date will not be replicated. | string | 30 days before the current date |
| **`/genesys_cloud_domain`** | Genesys Cloud Domain | The cloud region where the Genesys organization is deployed. Cloud regions and their domains can be found [here](https://help.mypurecloud.com/articles/aws-regions-for-genesys-cloud-deployment/). The `genesys_cloud_domain` is the part of the login URL after `https://login.`. For example, for the ap-south-1 region that has a login URL of `https://login.aps1.pure.cloud`, the `genesys_cloud_domain` is `aps1.pure.cloud` | string | Required |
| **`/credentials/client_id`** | OAuth Client ID | The client ID for your Genesys OAuth app. | string | Required |
| **`/credentials/client_secret`** | OAuth Client Secret | The client secret for your Genesys OAuth app. | string | Required |


#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Resource in Genesys from which collections are captured. | string | Required |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-genesys:dev
        config:
          start_date: "2024-11-11T00:00:00Z"
          genesys_cloud_domain: mypurecloud.com
          credentials:
            client_id: my_client_id
            client_secret: my_client_secret
    bindings:
      - resource:
          name: conversations
        target: ${PREFIX}/conversations
      - resource:
          name: users
        target: ${PREFIX}/users
```
