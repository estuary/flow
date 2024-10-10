
# Notion

This connector captures data from Notion into Flow collections via the [Notion API](https://developers.notion.com/reference/intro).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-notion:dev`](https://ghcr.io/estuary/source-notion:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

This connector is based on an open-source connector from a third party, with modifications for performance in the Flow system.

## Supported data resources

The following data resources are supported:

* [Blocks](https://developers.notion.com/reference/retrieve-a-block)
* [Comments](https://developers.notion.com/reference/retrieve-a-comment)
* [Databases](https://developers.notion.com/reference/retrieve-a-database)
* [Pages](https://developers.notion.com/reference/retrieve-a-page)
* [Users](https://developers.notion.com/reference/get-user)

By default, each resource is mapped to a Flow collection through a separate binding.

## Prerequisites

To use this connector, you'll need a Notion account with an [integration](https://developers.notion.com/docs/authorization) created to connect with Flow.

Before you create your integration, choose how you'll authenticate with Notion.
There are two ways: using OAuth to sign in directly in the web app,
or manually, using an access token.
OAuth is recommended in the web app; only manual configuration is supported when using the CLI.

### Setup for OAuth authentication

1. Go to [your integrations page](https://www.notion.so/my-integrations) and create a new integration.

2. On the new integration's **Secrets** page, change the integration type to **Public**. Fill in the required fields.

   * Redirect URIs: http://dashboard.estuary.dev
   * Website homepage: http://dashboard.estuary.dev
   * Privacy policy: https://www.estuary.dev/privacy-policy/
   * Terms of use: https://www.estuary.dev/terms/

### Setup for manual authentication

1. Go to [your integrations page](https://www.notion.so/my-integrations) and create a new [internal integration](https://developers.notion.com/docs/authorization#integration-types). Notion integrations are internal by default.

   1. During setup, [change **User Capabilities**](https://www.notion.so/help/create-integrations-with-the-notion-api#granular-integration-permissions)
   from **No user information** (the default) to **Read user information without email address**.

2. Copy the generated token for use in the connector configuration.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Notion source connector.

### Properties

#### Endpoint

The properties in the table below reflect the manual authentication method.
If you're working in the Flow web app, you'll use [OAuth2](#setup-for-oauth-authentication),
so many of these properties aren't required.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials`** | Authenticate using | Pick an authentication method. | object | Required |
| **`/credentials/auth_type`** | Authentication type | Set to `token` for manual authentication | string | Required |
| `/credentials/token` | Access Token | Notion API access token | string | |
| `/start_date` | Start Date | UTC date and time in the format YYYY-MM-DDTHH:MM:SS.000Z. Any data generated before this date will not be replicated. If left blank, the start date will be set to 2 years before the present date. | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Notion resource from which a collection is captured. | string | Required |
| **`/syncMode`** | Sync Mode | Sync this resource incrementally, or fully refresh it every run | string | Required |

### Sample

This sample specification reflects the manual authentication method.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-notion:dev
        config:
          credentials:
            auth_type: token
            token: {secret}
          start_date: 2021-01-25T00:00:00Z
    bindings:
      - resource:
          stream: blocks
          syncMode: incremental
        target: ${PREFIX}/blocks
      {...}
```
