# Navan

This connector captures data from Navan into Estuary collections.

It is available for use in the Estuary web application. For local development or open-source workflows, [`ghcr.io/estuary/source-navan:dev`](https://ghcr.io/estuary/source-navan:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Navan API:

* [Bookings](https://app.navan.com/app/helpcenter/articles/travel/admin/other-integrations/booking-data-integration)

By default, each resource is mapped to an Estuary collection through a separate binding.

## Prerequisites

The Navan API requires that an OAuth application is set up in the Admin dashboard. The `client_id` and `client_secret` are then used to authenticate the connector to your OAuth app to request data. See https://app.navan.com/app/helpcenter/articles/travel/admin/other-integrations/booking-data-integration for more details.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Navan source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/credentials_title`** | Authentication Method | Set to `OAuth Credentials`. | string | Required |
| **`/credentials/client_id`** | Client ID | The client ID obtained from the OAuth app set up in Navan dashboard. | string | Required for OAuth2 authentication |
| **`/credentials/client_secret`** | Client Secret | The client secret obtained from the OAuth app set up in Navan dashboard. | string | Required for OAuth2 authentication |


#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string |          |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-navan:dev
        config:
            credentials:
              credentials_title: OAuth Credentials
              client_id: secret_client_id_value
              client_secret: secret_client_secret_value
    bindings:
      - resource:
          name: bookings
        target: ${PREFIX}/bookings
```
