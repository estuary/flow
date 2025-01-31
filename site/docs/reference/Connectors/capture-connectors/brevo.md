
# Brevo

This connector captures data from [Brevo's REST API](https://developers.brevo.com/reference).

It is available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-brevo:dev`](https://ghcr.io/estuary/source-brevo:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Supported data resources

The following data resources are supported through the Brevo APIs:

* [Contacts](https://developers.brevo.com/reference/getcontacts-1)
* [Contacts Attributes](https://developers.brevo.com/reference/getattributes-1)
* [Contacts Lists](https://developers.brevo.com/reference/getlists-1)

By default, each resource is mapped to a Flow collection through a separate binding.

If your use case requires additional Brevo APIs, such as Campaigns, Events, or Accounts, [contact us](mailto:info@estuary.dev) to discuss the possibility of expanding this connector.

## Prerequisites

You will need a Brevo API key. See [Brevo's documentation](https://developers.brevo.com/docs/getting-started#using-your-api-key-to-authenticate) for instructions on creating one.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Brevo source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/api-key` | API Key | The Brevo API key used for authentication. | string | Required |
| `/start_date` | Start Date | Earliest date to read data from. Uses date-time format, ex. `YYYY-MM-DDT00:00:00.000Z`. | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/stream`** | Stream | Brevo resource from which collections are captured. | string | Required |
| **`/syncMode`** | Sync Mode | Connection method. | string | Required |

### Sample

```yaml

captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-brevo:dev
        config:
          api-key: {secret}
          start_date: 2025-01-01T00:00:00.000Z
    bindings:
      - resource:
          stream: contacts
          syncMode: full_refresh
        target: ${PREFIX}/contacts
      {...}
```
