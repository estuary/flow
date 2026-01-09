
# Airtable (deprecated)

This connector captures data from Airtable into Estuary collections.

It is available for use in the Estuary web application. For local development or open-source workflows, [`ghcr.io/estuary/source-airtable:dev`](https://ghcr.io/estuary/source-airtable:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

:::warning
This connector is deprecated. For the best experience, we recommend using our native [Airtable connector](./airtable-native.md) instead.
:::

## Prerequisites

* You must have an an active Airtable account.

### Setup

1. Log into your Airtable account.
2. In navigation bar, click on "Builder Hub".
3. In the "Developers" section, click on "Personal access tokens".
4. Click on "Create token".
5. Give your token a name and add the following scopes:
   - `data.records:read`
   - `data.recordComments:read`
   - `schema.bases:read`
6. Under the "Access" section, choose the bases you want to capture data from.
7. Click on "Create token".
8. Copy the token for use in the connector configuration.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Airtable source connector.

### Properties

#### Endpoint

The following properties reflect the API Key authentication method.

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/api_key` | API Key | API Key | string | Required |
| `/access_token` | Personal Access Token | The Personal Access Token for the Airtable account. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Resource of your Airtable project from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-airtable:dev
        config:
          access_token: <secret>
          api_key: <secret>
    bindings:
      - resource:
          stream: users
          syncMode: full_refresh
        target: ${PREFIX}/users
      {...}
```
